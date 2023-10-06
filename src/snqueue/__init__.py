import logging

from apscheduler.schedulers.background import BackgroundScheduler
from snqueue.snqueue import DataModel, ServiceFunc, SnQueueMessenger, SnQueueService

def start_service(
    name: str,
    aws_profile_name: str,
    service_sqs_url: str,
    service_func: ServiceFunc,
    silent: bool=False,
    interval: int=3,
    max_instances: int=2,
    sqs_args: dict={'MaxNumberOfMessages': 1},
    data_model: DataModel=None
) -> BackgroundScheduler:
  # Set logging
  logging.basicConfig(level=logging.INFO)
  logging.getLogger('botocore').setLevel(logging.WARNING)
  logging.getLogger('apscheduler').setLevel(logging.WARNING)
  logging.getLogger('snqueue.service.%s' % name).setLevel(logging.INFO)

  # Setup and start the service
  service = SnQueueService(
    name,
    aws_profile_name,
    service_func,
    silent=silent,
    data_model_class=data_model
  )

  scheduler = BackgroundScheduler()
  scheduler.add_job(
    service.run,
    args=[service_sqs_url, sqs_args],
    trigger='interval',
    seconds=interval,
    max_instances=max_instances
  )
  scheduler.start()
  service.logger.info('The service `%s` is up and running.' % name)

  return scheduler