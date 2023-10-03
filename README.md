# SnQueue - An SNS/SQS Event Messenger

### Installation

```shell
pip install snqueue
```

### Example

```py3
from snqueue import SnQueue

profile_name = "MY_AWS_PROFILE_NAME"
sqs_url = "MY_SQS_URL"
sns_topic_arn = "MY_SNS_TOPIC_ARN"

try:
  messenger = SnQueue(profile_name)

  messages = messenger.retrieve(sqs_url)
  print(messages)

  response = messenger.notify(sns_topic_arn, "A dumb notification")
  print(response)
except Exception as e:
  print(e)
```