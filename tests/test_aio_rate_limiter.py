"""Contains RateLimiter unit tests"""

import asyncio
from aio_rate_limiter.aio_rate_limiter import TooManyRequests
from uuid import uuid4

import aioredis
import pytest

from aio_rate_limiter import RateLimiter


@pytest.fixture
@pytest.mark.asyncio
async def redis_pool():
    """Provides a Redis connection pool"""
    pool = await aioredis.create_redis_pool("redis://localhost:6379")
    yield pool
    pool.close()
    await pool.wait_closed()


def test_initialize(redis_pool):
    """Should initialize"""
    limiter = RateLimiter(
        redis_pool, "resource", client="client", max_requests=10, time_window=3600
    )
    assert limiter._redis == redis_pool
    assert limiter._rate_limit_key == "rate_limit:resource:client"
    assert limiter._max_requests == 10
    assert limiter._time_window == 3600


@pytest.mark.asyncio
async def test_increment_usage__1(redis_pool):
    """Should fail on the 2nd hit in a second"""
    resource = str(uuid4())
    limiter = RateLimiter(redis_pool, resource, max_requests=1, time_window=1)
    async with limiter:
        pass
    with pytest.raises(TooManyRequests):
        async with limiter:
            pass


@pytest.mark.asyncio
async def test_increment_usage__10(redis_pool):
    """Should fail on the 11th hit in a second"""
    resource = str(uuid4())
    limiter = RateLimiter(redis_pool, resource, max_requests=10, time_window=1)
    for _ in range(10):
        async with limiter:
            pass
    with pytest.raises(TooManyRequests):
        async with limiter:
            pass


@pytest.mark.asyncio
async def test_increment_usage__wait(redis_pool):
    """Should succeed after waiting to the next window"""
    resource = str(uuid4())
    limiter = RateLimiter(redis_pool, resource, max_requests=1, time_window=1)
    async with limiter:
        pass
    await asyncio.sleep(1)
    async with limiter:
        pass
