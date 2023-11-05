import json
import logging
import os
import signal

from concurrent.futures import ThreadPoolExecutor
from pydantic import BaseModel, Field, validate_call
from snqueue.boto3_clients import SqsClient, SnsClient
from typing import Protocol, Any, TypeVar

class MessageDataModel(BaseModel):
  """
  Data model for processing SQS messages
  """
  @classmethod
  def parse_message(cls, message: dict) -> dict:
    """
    Parse message received from SQS, extracting message_id, data and attributes.

    :param message: Dictionary
    :param data_model: Used to validate data if provided
    :return: Dictionary with keys of 'id', 'data' and 'attributes'
    :raises:
      ValueError from pydantic
    """
    body = json.loads(message.get('Body'))
    id = body.get('MessageId')
    data = body.get('Message')
    data = cls.model_validate_json(data, strict=True)
    data = data.model_dump(exclude_none=True)

    attributes = body.get('MessageAttributes', {})
    for key, value in attributes.items():
      attributes[key] = value.get('Value')

    return {'id': id, 'data': data, 'attributes': attributes }

class ServiceConfig(BaseModel):
  MaxNumberOfMessages: int = Field(1, gt=1, le=10)
  VisibilityTimeout: int = Field(30, ge=0, le=60*60*12)
  WaitTimeSeconds: int = Field(20, ge=0, le=20)
  MaxWorkers: int = Field(None, ge=1, le=min(32, os.cpu_count()+4))

class ServiceFunc(Protocol):
  def __call__(
      self,
      message: dict,
      service: 'SnQueueService',
      **kwargs
  ) -> Any: ...

class SnQueueService:
  def __init__(
      self,
      name: str,
      aws_profile_name: str,
      service_func: ServiceFunc,
      **config
  ) -> None:
    self._name = name
    self._aws_profile_name = aws_profile_name
    self._service_func = service_func
    self._config = ServiceConfig(**config)

    self._logger = logging.getLogger(name)

    signal.signal(signal.SIGINT, self.shutdown)
    signal.signal(signal.SIGTERM, self.shutdown)

  @property
  def logger(self):
    return self._logger

  def shutdown(self, *args, **kwargs) -> None:
    self._running = False
    print("The service will be shutdown after all running tasks complete.")
  
  def listen(self, sqs_url: str, *args, **kwargs) -> None:
    print(f"The service is listening to {sqs_url}")
    self._running = True
    # https://www.digitalocean.com/community/tutorials/how-to-use-threadpoolexecutor-in-python-3
    with ThreadPoolExecutor(max_workers=self._config.MaxWorkers) as executor:
      while self._running:
        try:
          with SqsClient(self._aws_profile_name) as sqs:
            sqs_args = {key: dict(self._config).get(key) for key in [
              'MaxNumberOfMessages', 'VisibilityTimeout', 'WaitTimeSeconds']}
            messages = sqs.pull_messages(sqs_url, **sqs_args)
            executor.map(lambda message: self._service_func(message, self), messages)
            sqs.delete_messages(sqs_url, messages)
        except Exception as e:
          self._logger.exception(e)
    print("The service has been shut down.")

  @validate_call
  def notify(self, sns_topic_arn: str, message: dict, **kwargs) -> dict:
    """
    Send notification.

    :param sns_topic_arn: ARN of an SNS topic
    :param message: Dictionary
    :param kwargs: Dictionary of additional args passed to publish method of SnsClient
    :return: Dictionary of SNS response of publishing the message
    """
    message = json.dumps(message, ensure_ascii=False).encode('utf8').decode()
    with SnsClient(self._aws_profile_name) as sns:
      response = sns.publish(sns_topic_arn, message, **kwargs)
    return response

  def result_response(self, response_arn: str, request_message_id: str, result: dict) -> dict:
    """
    Send result response.

    :param response_arn: String.
    :param request_message_id: String.
    :param result: Dictionary.
    :return: Dictionary of response of sending response.
    """
    return self.notify(
      response_arn,
      result,
      MessageAttributes={
        'RequestMessageId': {
          'DataType': 'String',
          'StringValue': request_message_id
        },
        'Type': {
          'DataType': 'String',
          'StringValue': 'Response'
        }
      }
    )
  
  def error_response(self, response_arn: str, request_message_id: str, error_msg: str) -> dict:
    """
    Send result response.

    :param response_arn: String.
    :param request_message_id: String.
    :param result: Dictionary.
    :return: Dictionary of response of sending response.
    """
    return self.notify(
      response_arn,
      {},
      MessageAttributes={
        'RequestMessageId': {
          'DataType': 'String',
          'StringValue': request_message_id
        },
        'Type': {
          'DataType': 'String',
          'StringValue': 'ErrorResponse'
        },
        'ErrorMessage': {
          'DataType': 'String',
          'StringValue': error_msg
        }
      }
    )