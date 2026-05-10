import redis.asyncio as aioredis
from contextlib import asynccontextmanager
from dataclasses import dataclass
from ratelimiter.algorithms.token_bucket import TokenBucket
from ratelimiter.algorithms.sliding_window import SlidingWindow
from ratelimiter.algorithms.backoff import ExponentialBackoff
from ratelimiter.monitor import TrafficMonitor

@dataclass
class RateLimitResult:
    allowed: bool
    remaining: int
    algorithm: str
    retry_after: float = 0.0
    backoff_level: int = 0

class RateLimiter:
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        capacity: int = 100,
        refill_rate: float = 10.0,
        window: int = 60,
        monitor_window: int = 10,
    ):
        self.redis_url = redis_url
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.window = window
        self._client: aioredis.Redis | None = None
        self._token_bucket: TokenBucket | None = None
        self._sliding_window: SlidingWindow | None = None
        self._backoff: ExponentialBackoff | None = None
        self.monitor = TrafficMonitor(window_seconds=monitor_window)

    async def connect(self):
        self._client = aioredis.from_url(
            self.redis_url,
            encoding="utf-8",
            decode_responses=True
        )
        await self._client.ping()

        self._token_bucket = TokenBucket(self._client)
        self._sliding_window = SlidingWindow(self._client)
        self._backoff = ExponentialBackoff(self._client)

        await self._token_bucket.load()
        await self._sliding_window.load()
        await self._backoff.load()

        self.monitor.client = self._client
        await self.monitor.start()

    async def disconnect(self):
        await self.monitor.stop()
        if self._client:
            await self._client.aclose()

    async def check(self, key: str, limit: int | None = None, window: int | None = None) -> RateLimitResult:
        if not self._client:
            raise RuntimeError("RateLimiter not connected. Call await rl.connect() first.")

        limit = limit or self.capacity
        window = window or self.window
        algo = self.monitor.get_stats().active_algorithm

        if algo == "token_bucket":
            result = await self._token_bucket.check(
                key, capacity=limit, refill_rate=self.refill_rate
            )
        elif algo == "backoff":
            result = await self._backoff.check(
                key, limit=limit, window=window
            )
        else:
            result = await self._sliding_window.check(
                key, limit=limit, window=window
            )

        await self.monitor.record(result["allowed"])

        retry_after = 0.0
        if not result["allowed"]:
            retry_after = 1.0 / self.refill_rate if algo == "token_bucket" else 1.0

        return RateLimitResult(
            allowed=result["allowed"],
            remaining=result["remaining"],
            algorithm=result["algorithm"],
            retry_after=retry_after,
            backoff_level=result.get("backoff_level", 0)
        )

    async def reset(self, key: str):
        if not self._client:
            raise RuntimeError("RateLimiter not connected.")
        await self._client.delete(
            f"rl:tb:{key}", f"rl:sw:{key}",
            f"rl:bo:{key}:count", f"rl:bo:{key}:level"
        )

@asynccontextmanager
async def create_rate_limiter(**kwargs):
    rl = RateLimiter(**kwargs)
    await rl.connect()
    try:
        yield rl
    finally:
        await rl.disconnect()