import boto3

from typing import IO

class Boto3Client:
  '''
  A base class for AWS clients.

  :type client_name: string
  :param client_name: The name of the boto3 client, such as 'sqs', 'sns', 's3', etc.

  :type profile_name: string
  :param profile_name: The AWS profile name
  '''
  def __init__(
      self,
      client_name: str,
      profile_name: str
  ) -> None:
    self.client_name = client_name
    self.profile_name = profile_name
    return

  def __enter__(self):
    session = boto3.Session(profile_name=self.profile_name)
    self.client = session.client(self.client_name)
    return self
  
  def __exit__(self, exc_type, exc_value, traceback):
    self.client.close()
    return

class SqsClient(Boto3Client):
  '''
  A boto3 client receives messages from SQS.

  :type profile_name: string
  :param profile_name: The AWS profile name
  '''
  def __init__(
      self,
      profile_name: str
  ) -> None:
    super().__init__('sqs', profile_name)
    return
  
  def pull_messages(
      self,
      sqs_url: str,
      **kwargs
  ) -> list[dict]:
    '''
    Pull messages from SQS.
    
    :type sqs_url: string
    :param sqs_url: The URL of the SQS queue

    :type kwargs: dict
    :param kwargs: Additional arguments (e.g. {'MaxNumberOfMessages': 1})
    
    :rtype: list
    :return: The list of messages retrieved
    '''
    response = self.client.receive_message(
      QueueUrl = sqs_url,
      **kwargs
    )

    return response.get('Messages', [])
  
  def delete_messages(
      self,
      sqs_url: str,
      messages: list[dict]
  ) -> int:
    '''
    Delete messages from SQS.

    :type sqs_url: string
    :param sqs_url: The URL of the SQS queue

    :type messages: list
    :param messages: The list of messages to be deleted

    :rtype: int
    :return: The number of deleted messages
    '''
    for msg in messages:
      self.client.delete_message(
        QueueUrl=sqs_url,
        ReceiptHandle=msg['ReceiptHandle']
      )
    return len(messages)
  
class SnsClient(Boto3Client):
  '''
  A boto3 client sends notificaitons to SNS.

  :type profile_name: string
  :param profile_name: The name of AWS profile
  '''
  def __init__(
      self,
      profile_name: str
  ) -> None:
    super().__init__('sns', profile_name)
    return
  
  def publish(
      self,
      topic_arn: str,
      message: str,
      **kwargs
  ) -> dict:
    '''
    Publish message to SNS.
    
    :type topic_arn: string
    :param topic_arn: The ARN of the SNS topic

    :type message: string
    :param message: The message to be pulished

    :type kwargs: dict
    :param kwargs: Additional arguments (e.g. {'MessageDeduplicationId': 'x'})

    :rtype: dict
    :return: The SNS response of publishing the message
    '''
    return self.client.publish(
      TopicArn = topic_arn,
      Message = message,
      **kwargs
    )

class S3Client(Boto3Client):

  def __init__(
      self,
      profile_name: str
  ) -> None:
    super().__init__('s3', profile_name)
    return
  
  def download(
      self,
      bucket: str,
      key: str,
      fp: IO
  ) -> None:
    self.client.download_fileobj(bucket, key, fp)
    return
  
  def get_metadata(
      self,
      bucket: str,
      key: str
  ) -> dict:
    head = self.client.head_object(Bucket=bucket, Key=key)
    return head['Metadata']

class KmsClient(Boto3Client):

  def __init__(
      self,
      profile_name: str
  ) -> None:
    super().__init__('kms', profile_name)
    return
  
  def decrypt(
      self,
      **kwargs
  ) -> dict:
    return self.client.decrypt(**kwargs)