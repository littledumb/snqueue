# SnQueue - An SNS/SQS Event Messenger and Service Mechanism

## Installation

```shell
pip install snqueue
```

## Examples

### A Simple Messenger

```py3
from snqueue import SnQueueMessenger

profile_name = "MY_AWS_PROFILE_NAME"
sqs_url = "MY_SQS_URL"
sns_topic_arn = "MY_SNS_TOPIC_ARN"

try:
  messenger = SnQueueMessenger(profile_name)

  messages = messenger.retrieve(sqs_url)
  print(messages)

  response = messenger.notify(sns_topic_arn, "A dumb notification")
  print(response)
except Exception as e:
  print(e)
```

### A Dumb Service

```py3
import json
import time
from snqueue import SnQueueMessenger, start_service

# Define the service function
def dumb_service_func(data: str, **_) -> int:
  data = json.loads(data)
  return data['a'] + data['b']

if __name__ == '__main__':
  import logging

  # Configuration
  service_name = "MY_SERVICE_NAME"
  aws_profile_name = "MY_AWS_PROFILE_NAME"
  service_topic_arn = "MY_SERVICE_TOPIC_ARN"
  service_sqs_url = "MY_SERVICE_SQS_URL"
  notif_arn = "MY_RESULT_TOPIC_ARN"
  notif_sqs_url = "MY_RESULT_SQS_URL"

  # Start the service
  scheduler = start_service(
    service_name,
    aws_profile_name,
    service_sqs_url,
    dumb_service_func
  )

  # Send request to the service
  task_messenger = SnQueueMessenger(aws_profile_name)
  response = task_messenger.notify(
    service_topic_arn,
    {'a': 1, 'b': 2},
    MessageAttributes={
      'NotificationArn': {
        'DataType': 'String',
        'StringValue': notif_arn
      }
    })
  logging.info("Request has been sent:")
  logging.info(response)

  # Get result notification
  time.sleep(5)

  result_messenger = SnQueueMessenger(aws_profile_name)
  messages = result_messenger.retrieve(notif_sqs_url)
  logging.info("Result notficiations:")
  logging.info(messages)

  # Shut down the service
  scheduler.shutdown()
```