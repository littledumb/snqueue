import asyncio
import json
import logging
import signal

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Protocol

from snqueue.boto3_clients import SqsClient, SnsClient
from snqueue.service.helper import convert_attributes, parse_message_attributes, SqsConfig, SqsMessage, SqsMessageBody, to_str

# config default logging
logging.basicConfig(
  level=logging.INFO,
  format="[%(asctime)s - %(name)s - %(levelname)s] %(message)s"
)
default_logger = logging.getLogger("snqueue.service.server")
default_logger.setLevel(logging.INFO)
# hide botocore info level messages
logging.getLogger("botocore").setLevel(logging.WARN)

@dataclass
class SnQueueRequest:
  message_id: str
  topic_arn: str
  request_message_id: str
  received_timestamp: str
  data: Any
  attributes: dict
  app: 'SnQueueServer' = None

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

@dataclass
class SnQueueResponse:
  request_message_id: str
  service_arn: str
  app: 'SnQueueServer' = None

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

class SnQueueServiceFn(Protocol):
  def __call__(
      self,
      req: SnQueueRequest,
      res: SnQueueResponse
  ): ...

class SnQueueServer:

  def __init__(
      self,
      aws_profile_name: str,
      sqs_config: SqsConfig = SqsConfig(),
      logger: logging.Logger = default_logger
  ):
    self._aws_profile_name = aws_profile_name
    self._sqs_config = sqs_config
    self._logger = logger

    self._running = False
    self._services = {}

    signal.signal(signal.SIGINT, self.shutdown)
    signal.signal(signal.SIGTERM, self.shutdown)

  @property
  def aws_profile_name(self) -> str:
    return self._aws_profile_name
  
  @property
  def logger(self) -> logging.Logger:
    return self._logger

  def use(
      self,
      sqs_url: str,
      service_fn: SnQueueServiceFn
  ):
    self._services[sqs_url] = service_fn

  def _consume_message(
      self,
      message: dict,
      fn: SnQueueServiceFn
  ):
    req = SnQueueRequest.parse(message)
    res = SnQueueResponse(self.aws_profile_name, req)
    req.app = res.app = self
    fn(req, res)

  def _consume_messages(
      self,
      executor: ThreadPoolExecutor,
      messages: list[dict],
      fn: SnQueueServiceFn
  ):
    executor.map(
      lambda message: self._consume_message(message, fn),
      messages
    )

  def _serve(
      self,
      sqs_url: str,
      sqs_args: dict,
      executor: ThreadPoolExecutor
  ):
    service_fn = self._services.get(sqs_url)
    if not service_fn:
      return

    while self._running:
      try:
        with SqsClient(self.aws_profile_name) as sqs:
          messages = sqs.pull_messages(sqs_url, **sqs_args)
          self._consume_messages(executor, messages, service_fn)
          sqs.delete_messages(sqs_url, messages)
      except Exception as e:
        self.logger.exception(e)

  def start(self) -> None:
    """Start the server"""    
    sqs_args = dict(self._sqs_config)
    sqs_urls = list(self._services.keys())

    self._running = True
    self.logger.info("The server is up and running.")

    with ThreadPoolExecutor() as executor:
      def _start_service(sqs_url: str) -> None:
        self._serve(sqs_url, sqs_args, executor)

      executor.map(_start_service, sqs_urls)
      # keep the executor alive
      while self._running:
        asyncio.run(asyncio.sleep(5))

  def shutdown(self, *_):
    if self._running:
      self._running = False
      self.logger.info("The server is shutting down.")