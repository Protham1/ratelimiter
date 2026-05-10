import os
import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from ratelimiter.core import RateLimiter

# Locally, docker-compose maps Redis to 6380. In CI, it's 6379.
TEST_REDIS_URL = os.getenv("TEST_REDIS_URL", "redis://localhost:6380")

@pytest_asyncio.fixture(scope="session")
def event_loop():
    import asyncio
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest_asyncio.fixture(autouse=True)
async def clear_redis():
    """Fixture to completely wipe the Redis database before and after each test."""
    client = aioredis.from_url(TEST_REDIS_URL, decode_responses=True)
    await client.flushdb()
    yield
    await client.flushdb()
    await client.aclose()

@pytest_asyncio.fixture
async def rate_limiter():
    """Provide a connected RateLimiter instance for algorithm tests."""
    rl = RateLimiter(redis_url=TEST_REDIS_URL, capacity=5, refill_rate=2.0, window=10)
    await rl.connect()
    yield rl
    await rl.disconnect()
