import time
import redis.asyncio as aioredis

class ExponentialBackoff:
    def __init__(self, client: aioredis.Redis):
        self.client = client

    async def load(self):
        pass

    async def check(self, key: str, limit: int, window: int) -> dict:
        backoff_key = f"rl:bo:{key}:level"
        level = await self.client.get(backoff_key)
        level = int(level) if level else 0

        effective_limit = max(1, limit // (2 ** level))

        count_key = f"rl:bo:{key}:count"
        count = await self.client.incr(count_key)
        if count == 1:
            await self.client.expire(count_key, window)

        allowed = count <= effective_limit
        if not allowed and count % 10 == 0:
            new_level = min(level + 1, 5)
            await self.client.set(backoff_key, new_level, ex=300)

        return {
            "allowed": allowed,
            "remaining": max(0, effective_limit - count),
            "algorithm": "backoff",
            "backoff_level": level
        }