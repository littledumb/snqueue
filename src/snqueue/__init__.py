import logging

from apscheduler.schedulers.background import BackgroundScheduler
from snqueue.snqueue import DataModel, ServiceFunc, SnQueueMessenger, SnQueueService

def start_service(
    service: SnQueueService,
    service_sqs_url: str,
    service_sqs_args: dict={'MaxNumberOfMessages': 1},
    interval: int=3,
    max_instances: int=2,
    **_
) -> BackgroundScheduler:
  # Set logging
  logging.basicConfig(level=logging.INFO)
  logging.getLogger('botocore').setLevel(logging.WARNING)
  logging.getLogger('apscheduler').setLevel(logging.WARNING)
  logging.getLogger('snqueue.service.%s' % service.name).setLevel(logging.INFO)

  # Schedule a background service
  scheduler = BackgroundScheduler()
  scheduler.add_job(
    service.run,
    args=[service_sqs_url, service_sqs_args],
    trigger='interval',
    seconds=interval,
    max_instances=max_instances
  )
  scheduler.start()
  service.logger.info('The service `%s` is up and running.' % service.name)

  return scheduler