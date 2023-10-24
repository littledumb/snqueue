import json
import html
import mimetypes
import os
import shutil
import tempfile

from base64 import b64decode
from Cryptodome.Cipher import AES
from email import policy, message_from_bytes
from email.header import decode_header, Header
from email.message import EmailMessage
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from snqueue.boto3_clients import Boto3Client, S3Client, KmsClient
from typing import IO, NamedTuple

class Email(NamedTuple):
  From: str = ''
  To: str = ''
  Cc: str = ''
  Date: str = ''
  Subject: str = ''
  Body: str = ''
  Attachments: list[str] = []

def decode_raw_email_text(text: str) -> str:
  decoded = ''

  for txt, encoding in decode_header(text):
    if isinstance(txt, bytes):
      txt = txt.decode(encoding or 'us-ascii')
    decoded += txt

  return decoded

def get_email_body(message: EmailMessage) -> str:
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
  attachments = []

  for iter in message.iter_attachments():
    filename = decode_raw_email_text(iter.get_filename())
    path = os.path.join(dir, filename)
    with open(path, 'wb') as file:
      file.write(iter.get_payload(decode=True))
    attachments.append(path)

  return attachments

def parse_email(
    message: EmailMessage,
    dir: str
) -> Email:
  raw_fields = map(
    lambda x: message.get(x, ''),
    ('From', 'To', 'Cc', 'Date', 'Subject')
  )
  fields = map(decode_raw_email_text, raw_fields)
  # html.unescape converts `&nbsp`; to `\xa0`
  body = html.unescape(get_email_body(message)).replace('\xa0', ' ').strip()
  attachments = save_email_attachments(message, dir)
  return Email(*fields, body, attachments)

class S3Email:
  def __init__(
      self,
      profile_name: str,
      bucket: str,
      key: str
  ) -> None:
    self.profile_name = profile_name
    self.bucket = bucket
    self.key = key
    return
  
  def _decrypt_email(
      self,
      in_file: IO,
      out_file: IO,
      metadata: dict
  ) -> None:
    # Decrypt email encryption key
    envelope_key = b64decode(metadata['x-amz-key-v2'])
    encrypt_ctx = json.loads(metadata['x-amz-matdesc'])
    with KmsClient(self.profile_name) as kms:
      encrypt_key = kms.decrypt(
        CiphertextBlob=envelope_key,
        EncryptionContext=encrypt_ctx
      )
      encrypt_key = encrypt_key['Plaintext']
    # Construct decryptor
    iv = b64decode(metadata['x-amz-iv'])
    chunk_size = 16*1024
    original_size = int(metadata['x-amz-unencrypted-content-length'])
    decryptor = AES.new(encrypt_key, AES.MODE_GCM, iv)
    # Decrypt email
    in_file.seek(0)
    while True:
      chunk = in_file.read(chunk_size)
      if len(chunk) == 0:
        break
      out_file.write(decryptor.decrypt(chunk))
    # Finilize the work
    out_file.truncate(original_size)
    out_file.flush()
    return
  
  def __enter__(self) -> 'S3Email':
    self.tmp_dir = tempfile.mkdtemp()
    
    with tempfile.TemporaryFile('r+b') as encrypted_file:
      # Download S3 object and get its metadata
      with S3Client(self.profile_name) as s3:
        s3.download(self.bucket, self.key, encrypted_file)
        metadata = s3.get_metadata(self.bucket, self.key)
      # Decrypt email
      with tempfile.TemporaryFile('r+b') as decrypted_file:
        self._decrypt_email(encrypted_file, decrypted_file, metadata)
        decrypted_file.seek(0)
        email_message: EmailMessage = message_from_bytes(
          decrypted_file.read(),
          _class=EmailMessage,
          policy=policy.default
        )

    # Parse email
    self.email = parse_email(email_message, self.tmp_dir)      

    return self
  
  def __exit__(self, exc_type, exc_value, traceback):
    shutil.rmtree(self.tmp_dir)
    return

def guess_mimetype(filename: str) -> list[str]:
  mime_type, _ = mimetypes.guess_type(filename)
  return mime_type.split('/', 1)

def encode_email_contact(contact: str) -> str:
  if not '<' in contact:
    return contact

  alias, addr = contact.split('<')
  alias = alias.strip()
  addr = addr.split('>')[0].strip()

  encoded_alias = Header(alias, 'utf-8').encode()
  return f"{encoded_alias} <{addr}>"

class SesClient(Boto3Client):

  def __init__(
      self,
      profile_name: str
  ) -> None:
    super().__init__('ses', profile_name)
    return
  
  def send_email(
      self,
      mail: Email
  ) -> dict:
    # basic construction
    msg = MIMEMultipart()
    msg['Subject'] = mail.Subject
    msg['From'] = encode_email_contact(mail.From)
    msg['To'] = ', '.join([encode_email_contact(to.strip()) for to in mail.To.split(',')])
    msg['Cc'] = ', '.join([encode_email_contact(cc.strip()) for cc in mail.Cc.split(',')])
    body = MIMEText(mail.Body, 'plain')
    msg.attach(body)
    # attachments
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

    return self.client.send_raw_email(
      RawMessage={ 'Data': msg.as_string() }
    )