import asyncio
import logging
import random
from functools import wraps
from telethon import errors

log = logging.getLogger("tgparser.retry")


class PermanentError(Exception):
    pass


def with_flood_wait(max_tries: int = 5, base: float = 1.0, cap: float = 60.0):
    def deco(fn):
        @wraps(fn)
        async def wrapper(*a, **kw):
            attempt = 0
            while True:
                attempt += 1
                try:
                    return await fn(*a, **kw)
                except errors.FloodWaitError as e:
                    wait = min(e.seconds + 2, 3600)
                    log.warning("FloodWait %ss on %s (attempt %s)", wait, fn.__name__, attempt)
                    await asyncio.sleep(wait)
                except (errors.ServerError, errors.TimedOutError, ConnectionError, OSError) as e:
                    if attempt >= max_tries:
                        raise
                    delay = min(cap, base * (2 ** (attempt - 1))) + random.uniform(0, 0.5)
                    log.warning("Transient %s on %s (attempt %s), retry in %.1fs",
                                type(e).__name__, fn.__name__, attempt, delay)
                    await asyncio.sleep(delay)
        return wrapper
    return deco


async def retry_async(fn, *args, max_tries: int = 5, base: float = 1.0, cap: float = 30.0,
                      retryable=(ConnectionError, OSError, asyncio.TimeoutError), **kwargs):
    attempt = 0
    while True:
        attempt += 1
        try:
            return await fn(*args, **kwargs)
        except errors.FloodWaitError as e:
            await asyncio.sleep(min(e.seconds + 2, 3600))
        except retryable as e:
            if attempt >= max_tries:
                raise
            await asyncio.sleep(min(cap, base * (2 ** (attempt - 1))) + random.uniform(0, 0.5))
