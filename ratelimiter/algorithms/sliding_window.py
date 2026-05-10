import time
import uuid
import redis.asyncio as aioredis
from pathlib import Path

SCRIPT = (Path(__file__).parent.parent / "scripts" / "sliding_window.lua").read_text()

class SlidingWindow:
    def __init__(self, client: aioredis.Redis):
        self.client = client
        self._sha = None

    async def load(self):
        self._sha = await self.client.script_load(SCRIPT)

    async def check(self, key: str, limit: int, window: int) -> dict:
        now = time.time()
        req_id = str(uuid.uuid4())
        allowed, remaining = await self.client.evalsha(
            self._sha, 1, f"rl:sw:{key}",
            limit, window, now, req_id
        )
        return {
            "allowed": bool(allowed),
            "remaining": int(remaining),
            "algorithm": "sliding_window"
        }