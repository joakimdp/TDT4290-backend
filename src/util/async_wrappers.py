import asyncio
from typing import Coroutine, Dict
import logging
import aiohttp


async def gather_with_semaphore(n: int, *tasks: Coroutine):
    semaphore = asyncio.Semaphore(n)

    async def sem_task(task):
        async with semaphore:
            return await task

    return await asyncio.gather(*(sem_task(task) for task in tasks))


async def get_with_retries(s: aiohttp.ClientSession, url: str, n: int) -> (
    aiohttp.ClientResponse
):
    for i in range(5, 0, -1):
        try:
            async with s.get(url) as response:
                return await response.json(content_type=None)
        except Exception as e:
            logging.warning(f'Fetching failed for url {url}, retrying..')
            if i == 1:
                logging.exception(f'Exception raised for url {url}')
                raise e
