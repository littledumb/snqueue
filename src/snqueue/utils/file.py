import os
import requests
import shutil

#from typing import Iterable
#from parfive import Downloader, Results
from urllib.parse import unquote

def download(
    url: str,
    dest_dir: str
) -> str:
  filename = unquote(url).split('/')[-1].split('?')[0]
  filepath = os.path.join(dest_dir, filename)
  with requests.get(url, stream=True) as r:
    with open(filepath, 'wb') as f:
      shutil.copyfileobj(r.raw, f)
  return filepath

'''
def download(
    urls: Iterable[str],
    dest_dir: str
) -> Results:
  dl = Downloader()
  for url in urls:
    dl.enqueue_file(url, path=dest_dir)
  return dl.download()
'''