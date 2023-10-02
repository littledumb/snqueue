import boto3

from dataclasses import dataclass

class AwsAgent:
  """
  A base class for AWS agents
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

@dataclass
class SqsArgs:
  MaxNumberOfMessages: int = 1
  VisibilityTimeout: int = 30
  WaitTimeSeconds: int = 5

class SqsAgent(AwsAgent):
  """
  An agent For retrieving messages from SQS
  """
  def __init__(self,
               profile_name: str,
               sqs_url: str,
               sqs_args: SqsArgs = None):
    super().__init__("sqs", profile_name)
    self.sqs_url = sqs_url
    self.sqs_args = sqs_args or SqsArgs()
    return
  
  def pull_messages(self, delete: bool) -> list[dict]:
    response = self.client.receive_message(
      QueueUrl = self.sqs_url,
      *self.sqs_args
    )
    return response.get('Messages', [])
  
  def delete_messages(self, messages: list[dict]) -> int:
    for msg in messages:
      self.client.delete_message(
        QueueUrl=self.sqs_url,
        ReceiptHandle=msg['ReceiptHandle']
      )
    return len(messages)

class SnQueue:

  def __init__(self,
               profile_name: str):
    self.profile_name = profile_name
    return
  
  def retrieve(self,
               sqs_url: str,
               delete: bool = True,
               sqs_args: SqsArgs = None) -> list[dict]:
    with SqsAgent(self.profile_name,
                  sqs_url,
                  sqs_args) as sqs:
      messages = sqs.pull_messages(delete)

      if delete:
        sqs.delete_messages(messages)
        
      return messages
    
  def notify(self, sns_topic: str):
    pass