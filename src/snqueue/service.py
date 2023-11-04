import json
import logging
import os
import signal

from concurrent.futures import ThreadPoolExecutor
from pydantic import BaseModel, Field, validate_call
from snqueue.boto3_clients import SqsClient, SnsClient
from typing import Protocol, Any

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

    self.logger = logging.getLogger("snqueue.service.%s" % name)

    signal.signal(signal.SIGINT, self.shutdown)
    signal.signal(signal.SIGTERM, self.shutdown)

  def shutdown(self, *args, **kwargs) -> None:
    self._running = False
    print("The service will be shutdown after all running tasks complete.")
  
  def listen(self, sqs_url: str, *args, **kwargs) -> None:
    print(f"The service is listening to {sqs_url}")
    self._running = True
    # https://www.digitalocean.com/community/tutorials/how-to-use-threadpoolexecutor-in-python-3
    with ThreadPoolExecutor(max_workers=self._config.MaxWorkers) as executor:
      while self._running:
        with SqsClient(self._aws_profile_name) as sqs:
          sqs_args = {key: dict(self._config).get(key) for key in [
            'MaxNumberOfMessages', 'VisibilityTimeout', 'WaitTimeSeconds']}
          messages = sqs.pull_messages(sqs_url, **sqs_args)
          executor.map(lambda message: self._service_func(message, self), messages)
          sqs.delete_messages(sqs_url, messages)
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
      return sns.publish(sns_topic_arn, message, **kwargs)