import json
import numbers

from pydantic import BaseModel, Field
from typing import Any, Literal

def convert_attributes(attributes: dict) -> dict:
  msg_attr = {}

  for key, value in attributes.items():
    if isinstance(value, str):
      msg_attr[key] = { "DataType": "String", "StringValue": value }
    elif isinstance(value, numbers.Number):
      msg_attr[key] = { "DataType": "Number", "StringValue": str(value) }
    elif value:
      msg_attr[key] = { "DataType": "String", "StringValue": to_str(value) }
  
  return msg_attr

def to_str(obj: Any) -> str:
  try:
    return json.dumps(obj, ensure_ascii=False).encode('utf8').decode()
  except:
    return str(obj)

class SqsConfig(BaseModel):
  MaxNumberOfMessages: int = Field(1, ge=1, le=10)
  VisibilityTimeout: int = Field(30, ge=0, le=60*60*12)
  WaitTimeSeconds: int = Field(20, ge=1, le=20) # enforce SQS long polling

class SqsMessageAttribute(BaseModel):
  Type: Literal["String", "Number", "Binary"]
  Value: str

class SqsMessageBody(BaseModel):
  MessageId: str
  TopicArn: str
  Message: str
  Timestamp: str
  MessageAttributes: dict[str, SqsMessageAttribute] = {}

class SqsMessage(BaseModel):
  MessageId: str
  ReceiptHandle: str
  Body: str

def parse_message_attributes(attributes: dict[str, SqsMessageAttribute]) -> dict:
  result = {}
  for key, value in attributes.items():
    try:
      parsed_value = json.loads(value.Value)
    except:
      parsed_value = value.Value
    result[key] = parsed_value
  return result

def parse_raw_sqs_message(raw_message: dict) -> dict:
  sqs_message = SqsMessage(**raw_message)
  sqs_message.Body = SqsMessageBody(**json.loads(sqs_message.Body))
  return sqs_message