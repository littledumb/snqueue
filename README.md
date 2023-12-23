# SnQueue - An SNS/SQS Microservice Mechanism

## Installation

```shell
pip install snqueue
```

## A Dumb Service Example

```py3
import asyncio
import json
import logging
import random
import time

from threading import Thread

from snqueue.service import SnQueueServer, SqsVirtualQueueClient, SnQueueRequest, SnQueueResponse
from snqueue.service.helper import parse_raw_sqs_message, SqsConfig

""" Service setup """
lower_bound_proc_time = 4
upper_bound_proc_time = 10

def proc_time(batch: int) -> int:
  return sum(random.sample(range(lower_bound_proc_time, upper_bound_proc_time), batch))

def bad_request(
    res: SnQueueResponse=None,
    response_topic_arn: str=None,
    message: str=None
) -> None:
  if not res or not response_topic_arn:
    return logging.error(message or "Bad request")
  
  res.status(400).send(response_topic_arn, message)

def service_fn(req: SnQueueRequest, res: SnQueueResponse):  
  request_metadata = req.attributes.get("SnQueueRequestMetadata")
  response_topic_arn = request_metadata.get('ResponseTopicArn')
  batch = req.data.get('batch')
  processing_time = proc_time(batch)
  logging.info(f"Requested batch: {batch}; processing time: {processing_time} seconds.")
  time.sleep(processing_time)
  logging.info(f"Processed batch of {batch} in {processing_time} seconds.")
  res.status(200).send(response_topic_arn, { "ProcessingTime": processing_time })

""" Server side setup """
aws_profile_name = "xxxxxx"
service_topic_arn = "arn:aws:sns:us-east-1:xxxxxx:xxxxxx"
service_sqs_url = "https://sqs.us-east-1.amazonaws.com/xxxxxx/xxxxxx"
response_topic_arn = "arn:aws:sns:us-east-1:xxxxxx:xxxxxx"
response_sqs_url = "https://sqs.us-east-1.amazonaws.com/xxxxxx/xxxxxx"

message_attributes = {
  "SnQueueRequestMetadata": {
    "DataType": "String",
    "StringValue": json.dumps({ "ResponseTopicArn": response_topic_arn })
  }
}

server = SnQueueServer(aws_profile_name, sqs_config=SqsConfig(WaitTimeSeconds=1))
server.use(service_sqs_url, service_fn)

""" Client side setup"""
async def send_request(batch: int, timeout: int):
  try:
    async with SqsVirtualQueueClient(
      response_sqs_url,
      aws_profile_name,
      sqs_config=SqsConfig(MaxNumberOfMessages=10, WaitTimeSeconds=1)
    ) as client:
      logging.info(f"Sending a request with batch {batch}")
      start = time.perf_counter()
      response = await client.request(
        service_topic_arn,
        { "batch": batch },
        timeout=timeout,
        MessageAttributes=message_attributes
      )
      stop = time.perf_counter()
      logging.info(f"Received a response for batch {batch} after {stop - start} seconds.")
      logging.info(parse_raw_sqs_message(response).Body.Message)
  except asyncio.TimeoutError as e:
    logging.error(f"Timeout for task with batch of {batch} after {timeout} seconds.")
  except Exception as e:
    logging.error(e)

# main thread
if __name__ == '__main__':
  # start server in another thread
  thread = Thread(target=server.start)
  thread.start()

  max_batch, num = 4, 20
  batches = [
    random.randrange(1, max_batch) for i in range(num)
  ]
  timeouts = [15] * num

  print(f"batches: {batches}")

  def sync_send_request(batch: int, timeout: int):
    asyncio.run(send_request(batch, timeout))

  with ThreadPoolExecutor() as executor:
    for i in range(num):
      executor.submit(sync_send_request, batches[i], timeouts[i])
      time.sleep(random.randrange(1, 3))

  # Keep the main thread alive
  while server.is_running:
    time.sleep(2)
```

The output would be like:

```console
[2023-12-23 16:22:26,849 - snqueue.service.server - INFO] The server is up and running.
batches: [1, 2, 3, 3, 2, 1, 3, 2, 1, 3, 1, 2, 2, 3, 1, 2, 1, 3, 2, 3]
[2023-12-23 16:22:26,850 - root - INFO] Sending a request with batch 1
[2023-12-23 16:22:27,979 - root - INFO] Requested batch: 1; processing time: 6 seconds.
[2023-12-23 16:22:28,850 - root - INFO] Sending a request with batch 2
[2023-12-23 16:22:29,858 - root - INFO] Sending a request with batch 3
[2023-12-23 16:22:29,864 - root - INFO] Requested batch: 2; processing time: 12 seconds.
[2023-12-23 16:22:30,859 - root - INFO] Sending a request with batch 3
[2023-12-23 16:22:31,118 - root - INFO] Requested batch: 3; processing time: 17 seconds.
[2023-12-23 16:22:32,422 - root - INFO] Requested batch: 3; processing time: 22 seconds.
[2023-12-23 16:22:32,866 - root - INFO] Sending a request with batch 2
[2023-12-23 16:22:33,866 - root - INFO] Sending a request with batch 1
[2023-12-23 16:22:33,988 - root - INFO] Processed batch of 1 in 6 seconds.
[2023-12-23 16:22:34,021 - root - INFO] Requested batch: 2; processing time: 17 seconds.
[2023-12-23 16:22:34,867 - root - INFO] Sending a request with batch 3
[2023-12-23 16:22:35,323 - root - INFO] Requested batch: 1; processing time: 7 seconds.
[2023-12-23 16:22:36,332 - root - INFO] Received a response for batch 1 after 9.481020738370717 seconds.
[2023-12-23 16:22:36,337 - root - INFO] {"ProcessingTime": 6}
[2023-12-23 16:22:36,558 - root - INFO] Requested batch: 3; processing time: 20 seconds.
[2023-12-23 16:22:36,867 - root - INFO] Sending a request with batch 2
[2023-12-23 16:22:37,868 - root - INFO] Sending a request with batch 1
[2023-12-23 16:22:38,042 - root - INFO] Requested batch: 2; processing time: 13 seconds.
[2023-12-23 16:22:39,277 - root - INFO] Requested batch: 1; processing time: 4 seconds.
[2023-12-23 16:22:39,899 - root - INFO] Sending a request with batch 3
[2023-12-23 16:22:40,905 - root - INFO] Sending a request with batch 1
[2023-12-23 16:22:41,475 - root - INFO] Requested batch: 3; processing time: 21 seconds.
[2023-12-23 16:22:41,864 - root - INFO] Processed batch of 2 in 12 seconds.
[2023-12-23 16:22:42,445 - root - INFO] Processed batch of 1 in 7 seconds.
[2023-12-23 16:22:42,725 - root - INFO] Requested batch: 1; processing time: 4 seconds.
[2023-12-23 16:22:42,906 - root - INFO] Sending a request with batch 2
[2023-12-23 16:22:43,277 - root - INFO] Processed batch of 1 in 4 seconds.
[2023-12-23 16:22:43,906 - root - INFO] Sending a request with batch 2
[2023-12-23 16:22:44,171 - root - INFO] Requested batch: 2; processing time: 15 seconds.
[2023-12-23 16:22:44,323 - root - INFO] Received a response for batch 1 after 6.455241528339684 seconds.
[2023-12-23 16:22:44,328 - root - INFO] {"ProcessingTime": 4}
[2023-12-23 16:22:44,907 - root - INFO] Sending a request with batch 3
[2023-12-23 16:22:44,944 - root - INFO] Received a response for batch 2 after 16.092936656437814 seconds.
[2023-12-23 16:22:44,944 - root - INFO] {"ProcessingTime": 12}
[2023-12-23 16:22:45,132 - root - INFO] Received a response for batch 1 after 11.256396994926035 seconds.
[2023-12-23 16:22:45,190 - root - INFO] {"ProcessingTime": 7}
[2023-12-23 16:22:45,471 - root - INFO] Requested batch: 2; processing time: 15 seconds.
[2023-12-23 16:22:45,936 - root - INFO] Sending a request with batch 1
[2023-12-23 16:22:46,728 - root - INFO] Processed batch of 1 in 4 seconds.
[2023-12-23 16:22:47,546 - root - INFO] Requested batch: 3; processing time: 21 seconds.
[2023-12-23 16:22:47,941 - root - INFO] Sending a request with batch 2
[2023-12-23 16:22:48,121 - root - INFO] Processed batch of 3 in 17 seconds.
[2023-12-23 16:22:48,964 - root - ERROR] Timeout for task with batch of 2 after 15 seconds.
[2023-12-23 16:22:49,684 - root - INFO] Requested batch: 1; processing time: 6 seconds.
[2023-12-23 16:22:50,138 - root - INFO] Sending a request with batch 1
[2023-12-23 16:22:50,139 - root - INFO] Received a response for batch 1 after 9.232792022638023 seconds.
[2023-12-23 16:22:50,145 - root - INFO] {"ProcessingTime": 4}
[2023-12-23 16:22:50,795 - root - INFO] Received a response for batch 3 after 20.895967309363186 seconds.
[2023-12-23 16:22:50,795 - root - INFO] {"ProcessingTime": 17}
[2023-12-23 16:22:51,096 - root - INFO] Processed batch of 2 in 17 seconds.
[2023-12-23 16:22:51,114 - root - INFO] Processed batch of 2 in 13 seconds.
[2023-12-23 16:22:51,159 - root - INFO] Sending a request with batch 3
[2023-12-23 16:22:51,396 - root - INFO] Requested batch: 2; processing time: 13 seconds.
[2023-12-23 16:22:52,143 - root - INFO] Sending a request with batch 2
[2023-12-23 16:22:53,232 - root - INFO] Requested batch: 1; processing time: 4 seconds.
[2023-12-23 16:22:53,480 - root - ERROR] Timeout for task with batch of 3 after 15 seconds.
[2023-12-23 16:22:54,265 - root - INFO] Sending a request with batch 3
[2023-12-23 16:22:54,423 - root - INFO] Processed batch of 3 in 22 seconds.
[2023-12-23 16:22:55,094 - root - INFO] Requested batch: 3; processing time: 18 seconds.
[2023-12-23 16:22:55,535 - root - INFO] Received a response for batch 2 after 18.651954454369843 seconds.
[2023-12-23 16:22:55,538 - root - INFO] {"ProcessingTime": 13}
[2023-12-23 16:22:55,767 - root - INFO] Processed batch of 1 in 6 seconds.
[2023-12-23 16:22:56,555 - root - INFO] Requested batch: 2; processing time: 14 seconds.
[2023-12-23 16:22:56,558 - root - INFO] Processed batch of 3 in 20 seconds.
[2023-12-23 16:22:57,232 - root - INFO] Processed batch of 1 in 4 seconds.
[2023-12-23 16:22:57,326 - root - INFO] Received a response for batch 1 after 11.390039392746985 seconds.
[2023-12-23 16:22:57,327 - root - INFO] {"ProcessingTime": 6}
[2023-12-23 16:22:57,510 - root - ERROR] Timeout for task with batch of 3 after 15 seconds.
[2023-12-23 16:22:58,052 - root - INFO] Requested batch: 3; processing time: 15 seconds.
[2023-12-23 16:22:59,204 - root - INFO] Processed batch of 2 in 15 seconds.
[2023-12-23 16:23:00,062 - root - INFO] Received a response for batch 1 after 9.915128106251359 seconds.
[2023-12-23 16:23:00,074 - root - INFO] {"ProcessingTime": 4}
[2023-12-23 16:23:00,471 - root - INFO] Processed batch of 2 in 15 seconds.
[2023-12-23 16:23:01,101 - root - ERROR] Timeout for task with batch of 2 after 15 seconds.
[2023-12-23 16:23:01,109 - root - ERROR] Timeout for task with batch of 3 after 15 seconds.
[2023-12-23 16:23:01,748 - root - ERROR] Timeout for task with batch of 2 after 15 seconds.
[2023-12-23 16:23:02,475 - root - INFO] Processed batch of 3 in 21 seconds.
[2023-12-23 16:23:04,396 - root - INFO] Processed batch of 2 in 13 seconds.
[2023-12-23 16:23:05,086 - root - ERROR] Timeout for task with batch of 3 after 15 seconds.
[2023-12-23 16:23:07,407 - root - INFO] Received a response for batch 2 after 19.46632814593613 seconds.
[2023-12-23 16:23:07,408 - root - INFO] {"ProcessingTime": 13}
[2023-12-23 16:23:08,546 - root - INFO] Processed batch of 3 in 21 seconds.
[2023-12-23 16:23:10,555 - root - INFO] Processed batch of 2 in 14 seconds.
[2023-12-23 16:23:10,796 - root - ERROR] Timeout for task with batch of 2 after 15 seconds.
[2023-12-23 16:23:10,803 - root - ERROR] Timeout for task with batch of 3 after 15 seconds.
[2023-12-23 16:23:10,815 - root - ERROR] Timeout for task with batch of 3 after 15 seconds.
[2023-12-23 16:23:13,053 - root - INFO] Processed batch of 3 in 15 seconds.
[2023-12-23 16:23:13,094 - root - INFO] Processed batch of 3 in 18 seconds.
```