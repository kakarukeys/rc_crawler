import asyncio
import random
import time


# see https://stackoverflow.com/questions/45440900/throttling-async-functions-in-python-asyncio

class AsyncLeakyBucket(object):
    """A leaky bucket rate limiter.

    Allows up to max_rate / time_period acquisitions before blocking.

    time_period is measured in seconds; the default is 60.

    """
    def __init__(self, max_rate: float, time_period: float = 60) -> None:
        self._max_level = max_rate
        self._rate_per_sec = max_rate / time_period
        self._level = 0.0
        self._last_check = 0.0

    def _leak(self) -> None:
        """Drip out capacity from the bucket."""
        if self._level:
            # drip out enough level for the elapsed time since
            # we last checked
            elapsed = time.time() - self._last_check
            decrement = elapsed * self._rate_per_sec
            self._level = max(self._level - decrement, 0)
        self._last_check = time.time()

    def has_capacity(self, amount: float = 1) -> bool:
        """Check if there is enough space remaining in the bucket"""
        self._leak()
        return self._level + amount <= self._max_level

    async def acquire(self, amount: float = 1) -> None:
        """Acquire space in the bucket.

        If the bucket is full, block until there is space.

        """
        if amount > self._max_level:
            raise ValueError("Can't acquire more than the bucket capacity")

        while not self.has_capacity(amount):
            # wait for the next drip to have left the bucket
            await asyncio.sleep(1 / self._rate_per_sec)

        self._level += amount

    async def __aenter__(self) -> None:
        await self.acquire()
        return None

    async def __aexit__(self, exc_type, exc, tb) -> None:
        pass


def limit_actions(rate_limit_params):
    """ middleware to limit actions

        (platform-dependent arguments)
        rate_limit_params: [{max_rate: ..., time_period: ...}, ...]

        (decoratee)
        next_handler: coroutine that performs an action and returns a result

        returns a new coroutine that executes <next_handler> subject to <rate_limit_params>
    """
    leaky_buckets = [AsyncLeakyBucket(**kwargs) for kwargs in rate_limit_params]

    def middleware_factory(next_handler):
        async def middleware(*args, **kwargs):
            for bucket in leaky_buckets:
                await bucket.acquire(amount=random.random() + 1)  # rate limiting to avoid detection

            return await next_handler(*args, **kwargs)

        return middleware
    return middleware_factory
