import time
import redis.asyncio as aioredis
from pathlib import Path

SCRIPT = (Path(__file__).parent.parent / "scripts" / "backoff.lua").read_text()

class ExponentialBackoff:
    def __init__(self, client: aioredis.Redis, cooldown_time: int = 60):
        self.client = client
        self._sha = None
        self.cooldown_time = cooldown_time

    async def load(self):
        self._sha = await self.client.script_load(SCRIPT)

    async def check(self, key: str, limit: int, window: int) -> dict:
        level_key = f"rl:bo:{key}:level"
        count_key = f"rl:bo:{key}:count"
        deny_key = f"rl:bo:{key}:deny"
        cooldown_key = f"rl:bo:{key}:cooldown"
        
        allowed, remaining, level, retry_after = await self.client.evalsha(
            self._sha, 4, level_key, count_key, deny_key, cooldown_key,
            limit, window, self.cooldown_time
        )
        
        result = {
            "allowed": bool(allowed),
            "remaining": int(remaining),
            "algorithm": "backoff",
            "backoff_level": int(level)
        }
        
        if int(retry_after) > 0:
            result["retry_after"] = int(retry_after)
            
        return result