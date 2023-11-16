from pydantic import BaseModel, Field
from typing import Optional

from snqueue.boto3_clients import Boto3BaseClient

class SqsReceiveMessageArgs(BaseModel):
  MaxNumberOfMessages: Optional[int] = Field(1, gt=1, le=10)
  VisibilityTimeout: Optional[int]
  WaitTimeSeconds: Optional[int]

class SqsClient(Boto3BaseClient):
  """
  Boto3 SQS client.

  :param profile_name: Name of AWS profile
  """
  def __init__(
      self,
      profile_name: str
  ) -> None:
    super().__init__('sqs', profile_name)
    return
  
  def pull_messages(
      self,
      sqs_url: str,
      **kwargs: SqsReceiveMessageArgs
  ) -> list[dict]:
    """
    Pull messages from SQS.
    
    :param sqs_url: string
    :param kwargs: Dictionary of additional args, e.g. {'MaxNumberOfMessages': 1}
    :return: List of messages retrieved
    """
    response = self.client.receive_message(
      QueueUrl = sqs_url,
      **kwargs
    )

    return response.get('Messages', [])
  
  def delete_messages(
      self,
      sqs_url: str,
      messages: list[dict]
  ) -> dict:
    """
    Delete messages from SQS.

    :param sqs_url: string
    :param messages: List of message objects to be deleted
    :return: Dictionary of successful and failed results. See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/delete_message_batch.html
    """
    remained = [] + messages
    result = {
      'Successful': [],
      'Failed': []
    }

    while len(remained) > 0:
      batch = [{
        'Id': msg['MessageId'][:80],
        'ReceiptHandle': msg['ReceiptHandle']
      } for msg in remained[:10]]
      remained = remained[10:]
      res = self.client.delete_message_batch(
        QueueUrl=sqs_url,
        Entries=batch
      )
      result['Successful'] += res.get('Successful', [])
      result['Failed'] += res.get('Failed', [])
    
    return result

  def change_message_visibility_batch(
      self,
      sqs_url: str,
      messages: list[dict],
      timeout: int
  ) -> dict:
    entries = [{
      "Id": message["MessageId"],
      "ReceiptHandle": message["ReceiptHandle"],
      "VisibilityTimeout": timeout
    } for message in messages]

    return self.client.change_message_visibility_batch(
      QueueUrl=sqs_url,
      Entries=entries
    )
