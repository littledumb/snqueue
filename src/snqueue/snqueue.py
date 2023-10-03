import boto3

class Boto3Client:
  """
  A base class for AWS clients.

  :type client_name: string
  :param client_name: The name of the boto3 client, such as "sqs", "sns", "s3", etc.

  :type profile_name: string
  :param profile_name: The AWS profile name
  """
  def __init__(self,
               client_name: str,
               profile_name: str):
    self.client_name = client_name
    self.profile_name = profile_name
    return

  def __enter__(self):
    session = boto3.Session(profile_name=self.profile_name)
    self.client = session.client(self.client_name)
    return self
  
  def __exit__(self, exc_type, exc_value, traceback):
    self.client.close()
    return

class SqsClient(Boto3Client):
  """
  A boto3 client receives messages from SQS.

  :type profile_name: string
  :param profile_name: The AWS profile name
  """
  def __init__(self,
               profile_name: str):
    super().__init__("sqs", profile_name)
    return
  
  def pull_messages(self,
                    sqs_url: str,
                    sqs_args: dict) -> list[dict]:
    """
    Pull messages from SQS.
    
    :type sqs_url: string
    :param sqs_url: The URL of the SQS queue

    :type sqs_args: dict
    :param sqs_args: The dict that provides additional arguments (e.g. {"MaxNumberOfMessages": 1})
    
    :rtype: list
    :return: The list of messages retrieved
    """
    sqs_args = sqs_args or {}

    response = self.client.receive_message(
      QueueUrl = sqs_url,
      **sqs_args
    )

    return response.get('Messages', [])
  
  def delete_messages(self,
                      sqs_url: str,
                      messages: list[dict]) -> int:
    """
    Delete messages from SQS.

    :type sqs_url: string
    :param sqs_url: The URL of the SQS queue

    :type messages: list
    :param messages: The list of messages to be deleted

    :rtype: int
    :return: The number of deleted messages
    """
    for msg in messages:
      self.client.delete_message(
        QueueUrl=sqs_url,
        ReceiptHandle=msg['ReceiptHandle']
      )
    return len(messages)
  
class SnsClient(Boto3Client):
  """
  A boto3 client sends notificaitons to SNS.

  :type profile_name: string
  :param profile_name: The name of AWS profile
  """
  def __init__(self,
               profile_name: str
               ):
    super().__init__("sns", profile_name)
    return
  
  def publish(self,
              topic_arn: str,
              message: str,
              sns_args: dict) -> dict:
    """
    Publish message to SNS.
    
    :type topic_arn: string
    :param topic_arn: The ARN of the SNS topic

    :type message: string
    :param message: The message to be pulished

    :type sns_args: dict
    :param sns_args: The dict that provides additional arguments (e.g. {"MessageDeduplicationId": "x"})

    :rtype: dict
    :return: The SNS response of publishing the message
    """
    sns_args = sns_args or {}

    return self.client.publish(
      TopicArn = topic_arn,
      Message = message,
      **sns_args
    )

class SnQueue:
  """
  An SNS/SQS event messenger.

  :type profile_name: string
  :param profile_name: The name of AWS profile
  """
  def __init__(self,
               profile_name: str):
    self.profile_name = profile_name
    return
  
  def retrieve(self,
               sqs_url: str,
               delete: bool = True,
               sqs_args: dict = None) -> list[dict]:
    """
    Retrieve messages.

    :type sqs_url: string
    :param sqs_url: The URL of the SQS queue
    
    :type delete: bool
    :param delete: Whether to delete messages after receiving them. Default is True.

    :type sqs_args: dict
    :param sqs_args: The dict that provides additional arguments (e.g. {"MaxNumberOfMessages": 1})

    :rtype: list
    :return: The list of messages retrieved
    """
    with SqsClient(self.profile_name) as sqs:
      messages = sqs.pull_messages(sqs_url, sqs_args)

      if delete:
        sqs.delete_messages(sqs_url, messages)

      return messages
    
  def notify(self,
             sns_topic_arn: str,
             message: str,
             sns_args: dict = None) -> dict:
    """
    Send notifications.

    :type sns_topic_arn: string
    :param sns_topic_arn: The ARN of the SNS topic

    :type message: string
    :param message: The notification message

    :type sns_args: dict
    :param sns_args: The dict that provides additional arguments (e.g. {"MessageDeduplicationId": "x"})

    :rtype: dict
    :return: The SNS response of publishing the message
    """
    with SnsClient(self.profile_name) as sns:
      return sns.publish(sns_topic_arn, message, sns_args)
