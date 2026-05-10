import time
import redis.asyncio as aioredis
from pathlib import Path

SCRIPT = (Path(__file__).parent.parent / "scripts" / "token_bucket.lua").read_text()

class TokenBucket:
    def __init__(self, client: aioredis.Redis):
        self.client = client
        self._sha = None

    async def load(self):
        self._sha = await self.client.script_load(SCRIPT)

    async def check(self, key: str, capacity: int, refill_rate: float) -> dict:
        now = time.time()
        allowed, remaining = await self.client.evalsha(
            self._sha, 1, f"rl:tb:{key}",
            capacity, refill_rate, now, 1
        )
        return {
            "allowed": bool(allowed),
            "remaining": int(remaining),
            "algorithm": "token_bucket"
        }