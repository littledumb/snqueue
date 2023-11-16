import asyncio
import json

from typing import Any, Protocol, Hashable

from snqueue.boto3_clients import SqsClient, SnsClient
from snqueue.service.helper import to_str, SqsConfig

class MatchFn(Protocol):
  def __call__(
      self,
      message_id: str,
      raw_sqs_message: dict
  ) -> bool: ...

def default_match_fn(
    message_id: str,
    raw_sqs_message: dict
) -> bool:
  body = json.loads(raw_sqs_message.get('Body', {}))
  attributes = body.get('MessageAttributes', {})
  snqueue_response_metadata = json.loads(attributes.get('SnQueueResponseMetadata', {}).get('Value', ""))
  if not snqueue_response_metadata:
    return False
  
  return message_id == snqueue_response_metadata.get('RequestId')

class ResourceSingleton(type):
  _resource_instances = {}
  def __call__(cls, resource: Hashable, *args, **kwargs):
    if not isinstance(resource, Hashable):
      raise TypeError("Invalid arguments: `resource` must be hashable.")
    
    if resource not in cls._resource_instances:
      cls._resource_instances[resource] = super(ResourceSingleton, cls).__call__(resource, *args, **kwargs)

    return cls._resource_instances[resource]
  
class SqsVirtualQueueClient(metaclass=ResourceSingleton):
  
  def __init__(
      self,
      sqs_url: str,
      aws_profile_name: str,
      sqs_config: SqsConfig = SqsConfig()
  ):
    self._sqs_url = sqs_url
    self._aws_profile_name = aws_profile_name
    self._sqs_args = dict(sqs_config)

  async def __aenter__(self) -> 'SqsVirtualQueueClient':
    self._inqueue_messages: list[dict] = []
    self._processed_messages: list[dict] = []
    self._waiting_for_polling = set()

    return self

  async def __aexit__(self, *_) -> None:
    # clean up
    if len(self._processed_messages):
      with SqsClient(self.aws_profile_name) as sqs:
        sqs.delete_messages(self.sqs_url, self._processed_messages)
        self._processed_messages.clear()

  @property
  def sqs_url(self) -> str:
    return self._sqs_url

  @property
  def aws_profile_name(self) -> str:
    return self._aws_profile_name
  
  async def _poll_messages(self, match_fn: MatchFn) -> None:
    if len(self._inqueue_messages):
      # someone hasn't checked inqueue messages yet
      return
    
    with SqsClient(self.aws_profile_name) as sqs:
      if len(self._processed_messages):
        # delete processed messages first
        sqs.delete_messages(self.sqs_url, self._processed_messages)
        self._processed_messages.clear()

      messages = sqs.pull_messages(self.sqs_url, **self._sqs_args)
      unmatched = []
      # check with being waited
      for message in messages:
        matched = False
        for being_waited in self._waiting_for_polling:
          if match_fn(being_waited, message):
            matched = True
            self._inqueue_messages.append(message)
            break
        if not matched:
          unmatched.append(message)
      # change visibility for unmatched messages
      if len(unmatched):
        sqs.change_message_visibility_batch(self.sqs_url, unmatched, 0)

  async def _get_response(
      self,
      message_id: str,
      match_fn: MatchFn
  ) -> dict:
    self._waiting_for_polling.add(message_id) # mark waiting

    while True:
      # check inqueue messages
      queue = self._inqueue_messages
      for i in range(len(queue)):
        if match_fn(message_id, queue[i]):
          message = queue[i]
          self._inqueue_messages = queue[:i] + queue[i+1:]
          self._processed_messages.append(message) # mark processed
          self._waiting_for_polling.remove(message_id) # unmark waiting
          return message
      await asyncio.sleep(0) # allow switching to other tasks
      # call for polling
      await self._poll_messages(match_fn)
  
  async def request(
      self,
      topic_arn: str,
      data: Any,
      timeout: int=600,
      match_fn: MatchFn = default_match_fn,
      **kwargs
  ) -> dict:
    with SnsClient(self.aws_profile_name) as sns:
      res = sns.publish(
        topic_arn,
        to_str(data),
        **kwargs
      )
    message_id = res["MessageId"]
    return await asyncio.wait_for(self._get_response(message_id, match_fn), timeout)
    #async with asyncio.timeout(timeout):
    #  return await self._get_response(message_id, match_fn)
