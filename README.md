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
from snqueue import SnQueueMessenger, SnQueueService
from apscheduler.schedulers.background import BackgroundScheduler

# Define the service function
def dumb_service_func(data: str, **_) -> int:
  data = json.loads(data)
  return data['a'] + data['b']

if __name__ == '__main__':
  # Configuration
  aws_profile_name = "MY_AWS_PROFILE_NAME"
  service_topic_arn = "MY_SERVICE_TOPIC_ARN"
  service_sqs_url = "MY_SERVICE_SQS_URL"
  notif_arn = "MY_RESULT_TOPIC_ARN"
  notif_sqs_url = "MY_RESULT_SQS_URL"

  # Setup and start the service
  service = SnQueueService(aws_profile_name,
                           dumb_service_func)

  scheduler = BackgroundScheduler()
  scheduler.add_job(service.run,
                    args=[service_sqs_url, {'MaxNumberOfMessages': 1}],
                    trigger='interval',
                    seconds=3)

  scheduler.start()
  print("Service is up and running.")

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
  print("Request has been sent:")
  print(response)

  # Get result notification
  time.sleep(5)

  result_messenger = SnQueueMessenger(aws_profile_name)
  messages = result_messenger.retrieve(notif_sqs_url)
  print("Result notficiations:")
  print(messages)

  # Shut down the service
  scheduler.shutdown()
```