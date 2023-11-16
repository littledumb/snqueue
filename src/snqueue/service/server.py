import logging
import signal

from concurrent.futures import ThreadPoolExecutor
from typing import Protocol

from snqueue.service.helper import SqsConfig
from snqueue.service import SnQueueRequest, SnQueueResponse

from snqueue.boto3_clients import SqsClient

# config default logging
logging.basicConfig(
  level=logging.INFO,
  format="[%(asctime)s - %(name)s - %(levelname)s] %(message)s"
)
default_logger = logging.getLogger("snqueue.service.server")
default_logger.setLevel(logging.INFO)
# hide botocore info level messages
logging.getLogger("botocore").setLevel(logging.WARN)

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
      for url in sqs_urls:
        self._serve(url, sqs_args, executor)

  def shutdown(self, *_):
    if self._running:
      self._running = False
      self.logger.info("The server is shutting down.")