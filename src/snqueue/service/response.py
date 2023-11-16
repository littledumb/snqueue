from dataclasses import dataclass
from typing import Any

from snqueue.boto3_clients import SnsClient
from snqueue.service.helper import to_str, convert_attributes
from snqueue.service.request import SnQueueRequest

@dataclass
class SnQueueResponse:
  request_message_id: str
  service_arn: str

  def __init__(
      self,
      aws_profile_name: str,
      req: SnQueueRequest
  ):
    self.request_message_id = req.request_message_id
    self.service_arn = req.topic_arn
    self._aws_profile_name = aws_profile_name

  def status(self, status: int) -> 'SnQueueResponse':
    self._status = status
    return self

  def send(
      self,
      arn: str,
      data: Any,
      attributes: dict={}
  ) -> dict:
    message = to_str(data)

    snqueue_response_metadata = {
      "SnQueueResponseMetadata": {
        "RequestId": self.request_message_id,
        "TopicArn": self.service_arn,
        "StatusCode": self._status or 200
      }
    }

    try:
      attributes.update(snqueue_response_metadata)
    except:
      attributes = snqueue_response_metadata
    
    msg_attr = convert_attributes(attributes)

    with SnsClient(self._aws_profile_name) as sns:
      return sns.publish(
        arn,
        message,
        MessageAttributes = msg_attr
      )
