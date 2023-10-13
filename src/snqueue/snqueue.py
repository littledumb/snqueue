import json
import logging

from pydantic import BaseModel
from snqueue.boto3_clients import SqsClient, SnsClient
from typing import Type, TypeVar, Protocol, Any

class SnQueueMessenger:
  '''
  An SNS/SQS event messenger.

  :type profile_name: string
  :param profile_name: The name of AWS profile
  '''
  def __init__(self,
               profile_name: str):
    self.profile_name = profile_name
    return
  
  def retrieve(self,
               sqs_url: str,
               delete: bool = True,
               **kwargs) -> list[dict]:
    '''
    Retrieve messages.

    :type sqs_url: string
    :param sqs_url: The URL of the SQS queue
    
    :type delete: bool
    :param delete: Whether to delete messages after receiving them. Default is True.

    :type kwargs: dict
    :param kwargs: Additional arguments (e.g. {'MaxNumberOfMessages': 1})

    :rtype: list
    :return: The list of messages retrieved
    '''
    with SqsClient(self.profile_name) as sqs:
      messages = sqs.pull_messages(sqs_url, **kwargs)

      if delete:
        sqs.delete_messages(sqs_url, messages)

      return messages
    
  def notify(self,
             sns_topic_arn: str,
             message: str | dict,
             **kwargs) -> dict:
    '''
    Send notifications.

    :type sns_topic_arn: string
    :param sns_topic_arn: The ARN of the SNS topic

    :type message: string | dict
    :param message: The notification message

    :type kwargs: dict
    :param kwargs: Additional arguments (e.g. {'MessageDeduplicationId': 'x'})

    :rtype: dict
    :return: The SNS response of publishing the message
    '''
    if isinstance(message, dict):
      message = json.dumps(message, ensure_ascii=False).encode('utf8').decode()
    with SnsClient(self.profile_name) as sns:
      return sns.publish(sns_topic_arn, message, **kwargs)

DataModel = TypeVar('DataModel', bound=BaseModel)

class ServiceFunc(Protocol):
  def __call__(self, data: str|dict, **kwargs) -> Any: ...

class SnQueueService:
  def __init__(
      self,
      name: str,
      aws_profile_name: str,
      service_func: ServiceFunc,
      silent: bool=False,
      require_notification_arn: bool=True,
      confirmation_only: bool=False,
      data_model_class: Type[DataModel]=None
  ):
    self.name = name
    self.messenger = SnQueueMessenger(aws_profile_name)
    self.service_func = service_func
    self.silent = silent
    self.require_notification_arn = require_notification_arn
    self.confirmation_only = confirmation_only
    self.data_model_class = data_model_class
    self.logger = logging.getLogger('snqueue.service.%s' % name)

  def run(self, sqs_url: str, sqs_args: dict = {}):
    try:
      messages = self.messenger.retrieve(sqs_url, **sqs_args)
    except Exception as e:
      self.logger.critical(' Service initialization error:\n  %s', e)
      return

    for message in messages:
      notif = {}
      try:
        if not self.silent:
          self.logger.info(' Received a message:\n  %s', message)
        body = json.loads(message.get('Body'))
        message_id = body.get('MessageId')

        # Extract notification arn
        notif_arn = body.get('MessageAttributes', {}).get('NotificationArn', {}).get('Value')
        if not notif_arn and self.require_notification_arn:
          raise '`NotificationArn` is required.'
        
        # Initiate notification
        notif['RequestMessageId'] = message_id
        
        # Extract and validate data
        data = body.get('Message')
        if self.data_model_class:
          data = self.data_model_class.model_validate_json(data, strict=True)
          data = data.model_dump(exclude_none=True)
        
        # Call the service function
        res = self.service_func(
          data,
          raw_message=message,
          messenger=self.messenger
        )
        if self.confirmation_only:
          notif['Confirmation'] = res or data
        else:
          notif['Result'] = res
      except Exception as e:
        notif['ErrorMessage'] = str(e)
        self.logger.exception(e)
      finally:
        if notif_arn:
          response = self.messenger.notify(notif_arn, notif)
          if not self.silent:
            self.logger.info(' Sent a notification:\n  %s', response)
