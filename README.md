# SnQueue - An SNS/SQS Microservice Mechanism

## Installation

```shell
pip install snqueue
```

## A Dumb Service Example

```py3
import json
import logging
import time

from pydantic import BaseModel
from snqueue.boto3_clients import SqsClient
from snqueue.service import SnQueueService
from threading import Thread

# A data model class for validation
class DataModel(BaseModel):
  a: int
  b: int

# Define the service function
def dumb_service_func(
    message: dict,
    service: SnQueueService
):
  body = json.loads(message.get('Body'))
  attributes = body.get('MessageAttributes', {})
  if attributes.get('Type', {}).get('Value') == 'Request':
    data = DataModel.model_validate_json(body.get('Message'), strict=True)
    notification_arn = attributes.get('NotificationArn', {}).get('Value')
    service.notify(
      notification_arn,
      {'sum': data.a + data.b},
      MessageAttributes={
        'Type': {'DataType': 'String', 'StringValue': 'Response'}}
    )

if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)

  # Some variables
  service_name = "A_SERVICE_NAME"
  aws_profile_name = "AN_AWS_PROFILE_NAME"
  service_sqs_url = "AN_SQS_URL"
  service_topic_arn = "AN_SNS_TOPIC_ARN"
  notification_arn = "ANOTHER_SNS_TOPIC_ARN"
  notification_sqs_url = "ANOTHER_SQS_URL"

  # Setup and start the service
  service = SnQueueService(
    service_name,
    aws_profile_name,
    dumb_service_func
  )

  thread = Thread(target=service.listen, args=(service_sqs_url,))
  thread.start()

  # Send request to the service
  service.notify(
    service_topic_arn,
    {'a': 1, 'b': 2},
    MessageAttributes={
      'Type': {
        'DataType': 'String',
        'StringValue': 'Request'
      },
      'NotificationArn': {
        'DataType': 'String',
        'StringValue': notification_arn
      }
    })
  logging.info("Request has been sent.")

  # Get result notification
  time.sleep(5)

  with SqsClient(aws_profile_name) as sqs:
    messages = sqs.pull_messages(notification_sqs_url)
    logging.info("Result notficiations:")
    logging.info(messages)
    sqs.delete_messages(notification_sqs_url, messages)

  # Shut down the service
  service.shutdown()
```