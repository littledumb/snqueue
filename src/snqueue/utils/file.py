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
  async with semaphore:
    async with session.get(url, **kwargs) as r:
      if not r.status == 200:
        return None
      
      header = r.headers.get("content-disposition")
      if header:
        filename = header.split("filename=")[1]
      else:
        filename = unquote(url.split('?')[0].split('/')[-1])
      dest = os.path.join(dest_dir, filename)
      
      async with aiofiles.open(dest, mode="wb") as f:
        if not silent:
          print(f"Start downloading {url} into {dest}...")
        await f.write(await r.read())
        if not silent:
          print(f"{url} is downloaded into {dest}.")
        return dest

async def aio_download(
    session: aiohttp.ClientSession,
    urls: list[str],
    dest_dir: str,
    **kwargs
) -> list[str]:
  semaphore = asyncio.Semaphore(MAX_TASKS)
  tasks = map(
    lambda url: aio_download_single(semaphore, session, url, dest_dir, **kwargs),
    urls
  )
  return await asyncio.gather(*tasks)

def download(urls: list[str], dest_dir: str, **kwargs) -> list[str]:
  async def _async_download():
    async with aiohttp.ClientSession() as session:
      dest = await aio_download(session, urls, dest_dir, **kwargs)
      return dest
  return asyncio.run(_async_download())

