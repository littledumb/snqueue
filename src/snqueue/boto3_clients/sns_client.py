from pydantic import BaseModel
from typing import Optional

from snqueue.boto3_clients import Boto3BaseClient

class SnsPublishArgs(BaseModel):
  Subject: Optional[str]
  MessageAttributes: Optional[dict[str, dict]]
  MessageDeduplicationId: Optional[str]
  MessageGroupId: Optional[str]

class SnsClient(Boto3BaseClient):
  '''
  Boto3 SNS client.

  :param profile_name: Name of AWS profile
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
      **kwargs: SnsPublishArgs
  ) -> dict:
    """
    Publish a message to SNS.
    
    :param topic_arn: string
    :param message: string
    :param kwargs: Dictionary of additional arguments, e.g. {'MessageDeduplicationId': 'x'})
    :return: Dictionary of SNS response
    """
    return self.client.publish(
      TopicArn = topic_arn,
      Message = message,
      **kwargs
    )
