import time
import redis.asyncio as aioredis
from pathlib import Path

SCRIPT = (Path(__file__).parent.parent / "scripts" / "backoff.lua").read_text()

class ExponentialBackoff:
    def __init__(self, client: aioredis.Redis):
        self.client = client
        self._sha = None

    async def load(self):
        self._sha = await self.client.script_load(SCRIPT)

    async def check(self, key: str, limit: int, window: int) -> dict:
        level_key = f"rl:bo:{key}:level"
        count_key = f"rl:bo:{key}:count"
        
        allowed, remaining, level = await self.client.evalsha(
            self._sha, 2, level_key, count_key,
            limit, window
        )
        
        return {
            "allowed": bool(allowed),
            "remaining": int(remaining),
            "algorithm": "backoff",
            "backoff_level": int(level)
        }