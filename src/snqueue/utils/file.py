import aiofiles
import aiohttp
import asyncio
import os

from urllib.parse import unquote

MAX_TASKS = 5

async def aio_download_single(
    semaphore: asyncio.Semaphore,
    session: aiohttp.ClientSession,
    url: str,
    dest_dir: str,
    silent: bool=True,
    **kwargs
) -> str | None:
  filename = unquote(url).split('/')[-1].split('?')[0]
  dest = os.path.join(dest_dir, filename)

  async with semaphore:
    async with session.get(url, **kwargs) as r:
      if r.status == 200:
        async with aiofiles.open(dest, mode="wb") as f:
          if not silent:
            print(f"Start downloading {url} into {dest}...")
          await f.write(await r.read())
          if not silent:
            print(f"{url} is downloaded into {dest}.")
          return dest
      else:
        return None

async def aio_download(
    session: aiohttp.ClientSession,
    urls: list[str],
    dest_dir: str,
    **kwargs
) -> str:
  semaphore = asyncio.Semaphore(MAX_TASKS)
  tasks = map(
    lambda url: aio_download_single(semaphore, session, url, dest_dir, **kwargs),
    urls
  )
  return await asyncio.gather(*tasks)