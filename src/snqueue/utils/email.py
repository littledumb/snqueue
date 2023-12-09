import json
import html
import os
import shutil
import tempfile

from base64 import b64decode
from collections.abc import Iterator
from contextlib import contextmanager
from Cryptodome.Cipher import AES
from email import policy, message_from_bytes
from email.header import decode_header, Header
from email.message import Message, EmailMessage
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pydantic import BaseModel, Field, NameEmail
from typing import IO, Optional

from snqueue.boto3_clients import SesClient, S3Client, KmsClient

class Email(BaseModel):
  """
  Email data encapsulation.

  :param From: string
  :param To: string or list of string
  :param Cc: string or list of string
  :param Date: string
  :param Subject: string
  :param Body: string
  :param Attachments: List of file paths
  """
  From: NameEmail
  To: NameEmail | list[NameEmail]
  Cc: Optional[NameEmail | list[NameEmail]] = Field(None)
  Date: Optional[str] = Field(None)
  Subject: str
  Body: str
  Attachments: Optional[list[str]] = Field([])

def encode_mailbox(mailbox: NameEmail) -> str:
  """
  Encode a mailbox which is a NameEmail object.

  :param mailbox: NameEmail in the format of `{'name': 'xxx', 'email': 'yyy'}`
  :return: encoded string of the mailbox, specified by RFC 5322
  """
  encoded_name = Header(mailbox.name, 'utf-8').encode()
  return f"{encoded_name} <{mailbox.email}>"

def send_ses_email(
      ses_client: SesClient,
      mail: Email
  ) -> dict:
  """
  Send email with ses client.

  :param ses_client: SES client
  :param mail: Encapsulated email data
  :return: Dictionary of the response
  """
  # Construct message body
  msg = MIMEMultipart()
  msg['Subject'] = mail.Subject
  msg['From'] = encode_mailbox(mail.From)
  addrs = [mail.To] if isinstance(mail.To, NameEmail) else mail.To
  msg['To'] = ', '.join([encode_mailbox(to) for to in addrs])
  if mail.Cc:
    addrs = [mail.Cc] if isinstance(mail.Cc, NameEmail) else mail.Cc
    msg['Cc'] = ', '.join([encode_mailbox(cc) for cc in addrs])
  body = MIMEText(mail.Body, 'plain')
  msg.attach(body)
  # Add attachments
  for filepath in mail.Attachments:
    with open(filepath, 'rb') as fp:
      data = fp.read()
    filename = os.path.basename(filepath)
    #maintype, subtype = guess_mimetype(filename)
    #part = MIMEApplication(data, _subtype=f'{maintype};{subtype}')
    part = MIMEApplication(data)
    part.add_header(
      'Content-Disposition',
      'attachment',
      filename=Header(filename, 'utf-8').encode()
    )
    msg.attach(part)

  return ses_client.send_raw_email(msg.as_string())

def decrypt_s3_email(
    kms_client: KmsClient,
    enc_file: IO,
    dec_file: IO,
    metadata: dict
) -> None:
  """
  Decrypt an email file.

  :param kms_client: KMS client for decrypting the encryption key
  :param enc_file: File object of the encrypted email
  :param dec_file: File object for storing decrypted content
  :param metadata: Dictionary of encryption metadata
  """
  # Decrypt email encryption key
  envelope_key = b64decode(metadata['x-amz-key-v2'])
  encrypt_ctx = json.loads(metadata['x-amz-matdesc'])  
  encrypt_key = kms_client.decrypt(
    envelope_key,
    EncryptionContext=encrypt_ctx
  )
  encrypt_key = encrypt_key['Plaintext']
  
  # Construct decryptor
  iv = b64decode(metadata['x-amz-iv'])
  chunk_size = 16*1024
  original_size = int(metadata['x-amz-unencrypted-content-length'])
  decryptor = AES.new(encrypt_key, AES.MODE_GCM, iv)

  # Decrypt email
  enc_file.seek(0)
  while True:
    chunk = enc_file.read(chunk_size)
    if len(chunk) == 0:
      break
    dec_file.write(decryptor.decrypt(chunk))

  # Finilize work
  dec_file.truncate(original_size)
  dec_file.flush()
  return

def decode_raw_email_text(text: str) -> str:
  """
  Decode raw email text.

  :param text: string
  :return: String of decoded text
  """
  return ''.join(map(
    lambda tpl: tpl[0].decode(tpl[1] or 'us-ascii') if isinstance(tpl[0], bytes) else tpl[0],
    decode_header(text)
  ))

def get_email_body(message: EmailMessage) -> str:
  """
  Extract `Body` from an `EmailMessage` object.

  :param message: `EmailMessage` object
  :return: String of the email `Body`
  """
  if message.is_multipart():
    for part in message.walk():
      cdispo = str(part.get('Content-Disposition'))
      if part.get_content_type() == 'text/plain' and 'attachment' not in cdispo:
        charset = part.get_content_charset()
        return part.get_payload(decode=True).decode(charset)
  else:
    charset = message.get_content_charset()
    return message.get_payload(decode=True).decode(charset)

def save_email_attachments(
    message: EmailMessage,
    dir: str
) -> list[str]:
  """
  Extract attachments from `EmailMessage` object and save them to a given directory.

  :param message: `EmailMessage` object
  :param dir: Path of the directory to save attachments
  :return: List of file paths of saved attachments
  """
  def _save_iter(iter: Iterator[Message]) -> str:
    """
    Save a single attachment.

    :param iter: Iterator of the attachment
    :return: File path of the saved attachment
    """
    filename = decode_raw_email_text(iter.get_filename())
    path = os.path.join(dir, filename)
    with open(path, 'wb') as file:
      file.write(iter.get_payload(decode=True))
    return path
  
  return map(_save_iter, message.iter_attachments())

def parse_email_message(
    message: EmailMessage,
    dir: str
) -> Email:
  raw_fields = list(map(
    lambda x: message.get(x, ''),
    ['From', 'To', 'Cc', 'Date', 'Subject']
  ))
  fields = list(map(decode_raw_email_text, raw_fields))
  # html.unescape converts `&nbsp`; to `\xa0`
  body = html.unescape(get_email_body(message)).replace('\xa0', ' ').strip()
  attachments = save_email_attachments(message, dir)

  return Email(
    From=fields[0],
    To=fields[1].split(','),
    Cc=fields[2].split(',') if fields[2] else None,
    Date=fields[3],
    Subject=fields[4],
    Body=body,
    Attachments=attachments
  )

@contextmanager
def get_s3_email(
    s3_client: S3Client,
    kms_client: KmsClient,
    bucket_name: str,
    object_key: str
) -> None:
  """
  Get email from an S3 object.

  :param s3_client: Client that can access the S3 object of email
  :param bucket_name: string
  :param object_key: string
  :return: Email object
  """
  # Make a temporary directory for storing possible attachments
  tmpdir = tempfile.mkdtemp()

  with tempfile.TemporaryFile('r+b') as encrypted_file:
    # Download S3 object and get its metadata
    s3_client.download(bucket_name, object_key, encrypted_file)
    metadata = s3_client.get_metadata(bucket_name, object_key)

    # Decrypt email
    with tempfile.TemporaryFile('r+b') as decrypted_file:
      decrypt_s3_email(kms_client, encrypted_file, decrypted_file, metadata)
      decrypted_file.seek(0)
      email_message: EmailMessage = message_from_bytes(
        decrypted_file.read(),
        _class=EmailMessage,
        policy=policy.default
      )

  # Parse email
  email = parse_email_message(email_message, tmpdir)

  yield email

  shutil.rmtree(tmpdir)
  
