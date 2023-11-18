import json

from dataclasses import dataclass
from typing import Any

from snqueue.service.helper import SqsMessage, SqsMessageBody, parse_message_attributes

@dataclass
class SnQueueRequest:
  message_id: str
  topic_arn: str
  request_message_id: str
  received_timestamp: str
  data: Any
  attributes: dict
  app = None # an `SnQueueServer` object

  @classmethod
  def parse(cls, raw_sqs_message: dict) -> 'SnQueueRequest':
    sqs_message = SqsMessage(**raw_sqs_message)
    message_id = sqs_message.MessageId
    body = SqsMessageBody(**json.loads(sqs_message.Body))
    request_message_id = body.MessageId
    topic_arn = body.TopicArn
    received_timestamp = body.Timestamp
    try:
      data = json.loads(body.Message)
    except:
      data = body.Message
    attributes = parse_message_attributes(body.MessageAttributes)

    return cls(
      message_id = message_id,
      topic_arn = topic_arn,
      request_message_id = request_message_id,
      received_timestamp = received_timestamp,
      data = data,
      attributes = attributes
    )
    