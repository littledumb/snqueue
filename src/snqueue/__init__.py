import logging
import os
import requests
import shutil

from apscheduler.schedulers.background import BackgroundScheduler
from snqueue.snqueue import DataModel, ServiceFunc, SnQueueMessenger, SnQueueService
from urllib.parse import unquote

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

def download_from_url(
    url: str,
    local_path: str=None,
    local_filename: str=None
) -> str | None:
  """
  Download a file from URL

  :param url: string
  :param local_filename: string
  :return: The name of local file downloaded
  :return: None if error
  """
  local_path = local_path or '.'
  local_filename = local_filename or unquote(url).split('/')[-1].split('?')[0]

  try:
    with requests.get(url, stream=True) as r:
      with open(os.path.join(local_path, local_filename), 'wb') as f:
        shutil.copyfileobj(r.raw, f)
  except Exception as e:
    logging.error(e)
    return None

  return local_filename