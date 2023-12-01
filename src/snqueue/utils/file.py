import aiofiles
import aiohttp
import asyncio
import os

from collections.abc import Iterable
from urllib.parse import unquote

async def _download_single(
    semaphore: asyncio.Semaphore,
    session: aiohttp.ClientSession,
    url: str,
    dest_dir: str
) -> str | None:
  async with semaphore:
    async with session.get(url) as r:
      if not r.status == 200:
        return None
    
      header = r.headers.get("content-disposition")
      if header:
        filename = header.split("filename=")[1]
      else:
        filename = unquote(url.split('?')[0].split('/')[-1])
      filepath = os.path.join(dest_dir, filename)

      async with aiofiles.open(filepath, mode="wb") as f:
          await f.write(await r.read())
          return filepath
    
async def download(
    urls: Iterable[str],
    dest_dir: str,
    max_workers: int = 5
):
  semaphore = asyncio.Semaphore(max_workers)
  async with aiohttp.ClientSession() as session:
    tasks = map(
      lambda url: _download_single(
        semaphore,
        session,
        url,
        dest_dir
      ),
      urls
    )
    return await asyncio.gather(*tasks)