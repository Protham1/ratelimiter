import time
import asyncio
import redis.asyncio as aioredis
from dataclasses import dataclass
from typing import Literal, Optional

Algorithm = Literal["sliding_window", "token_bucket", "backoff"]

@dataclass
class TrafficStats:
    req_per_sec: float = 0.0
    burst_ratio: float = 1.0
    deny_rate: float = 0.0
    active_algorithm: Algorithm = "sliding_window"
    total_requests: int = 0
    total_denied: int = 0

class TrafficMonitor:
    def __init__(self, window_seconds: int = 10):
        self.window = window_seconds
        self.client: Optional[aioredis.Redis] = None
        self._stats = TrafficStats()
        self._lock = asyncio.Lock()
        self._running = False
        self._subscribers: list[asyncio.Queue] = []
        
        # Local counters for zero-latency recording
        self._local_reqs = 0
        self._local_denied = 0

    async def record(self, allowed: bool):
        async with self._lock:
            self._local_reqs += 1
            if not allowed:
                self._local_denied += 1

    async def start(self):
        self._running = True
        asyncio.create_task(self._loop())

    async def stop(self):
        self._running = False

    async def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=10)
        self._subscribers.append(q)
        return q

    async def unsubscribe(self, q: asyncio.Queue):
        if q in self._subscribers:
            self._subscribers.remove(q)

    def get_stats(self) -> TrafficStats:
        return self._stats

    async def _loop(self):
        while self._running:
            await asyncio.sleep(1)
            try:
                await self._compute()
                await self._broadcast()
            except Exception as e:
                print(f"Monitor loop error: {e}")

    async def _compute(self):
        if not self.client:
            return

        now = int(time.time())
        
        # Flush local counters to Redis buckets
        async with self._lock:
            reqs = self._local_reqs
            denied = self._local_denied
            self._local_reqs = 0
            self._local_denied = 0

        req_key = f"rl:metrics:reqs:{now}"
        deny_key = f"rl:metrics:denied:{now}"
        
        pipeline = self.client.pipeline()
        
        # 1. Update Current Second Bucket
        if reqs > 0:
            pipeline.incrby(req_key, reqs)
            pipeline.expire(req_key, 20)
        if denied > 0:
            pipeline.incrby(deny_key, denied)
            pipeline.expire(deny_key, 20)
            
        # 2. Update Global Totals
        pipeline.incrby("rl:metrics:total_reqs", reqs)
        pipeline.incrby("rl:metrics:total_denied", denied)
        
        # 3. Fetch past 10 seconds of buckets
        req_keys = [f"rl:metrics:reqs:{now - i}" for i in range(self.window)]
        deny_keys = [f"rl:metrics:denied:{now - i}" for i in range(self.window)]
        
        for k in req_keys: pipeline.get(k)
        for k in deny_keys: pipeline.get(k)
        
        # 4. Fetch global states
        pipeline.get("rl:metrics:total_reqs")
        pipeline.get("rl:metrics:total_denied")
        pipeline.get("rl:config:active_algorithm")
        
        results = await pipeline.execute()
        
        # Dynamically determine the offset in results because INCRBY/EXPIRE add responses
        fetch_start_idx = 0
        if reqs > 0: fetch_start_idx += 2
        if denied > 0: fetch_start_idx += 2
        fetch_start_idx += 2  # The two global total INCRBYs
        
        # Extract fetched data
        req_counts = results[fetch_start_idx : fetch_start_idx + self.window]
        deny_counts = results[fetch_start_idx + self.window : fetch_start_idx + 2 * self.window]
        
        total_reqs_global = results[fetch_start_idx + 2 * self.window]
        total_denied_global = results[fetch_start_idx + 2 * self.window + 1]
        active_algo_global = results[fetch_start_idx + 2 * self.window + 2]
        
        req_counts = [int(x) if x else 0 for x in req_counts]
        deny_counts = [int(x) if x else 0 for x in deny_counts]

        total_10s_reqs = sum(req_counts)
        total_10s_denied = sum(deny_counts)
        recent_reqs = sum(req_counts[:2])

        req_per_sec = total_10s_reqs / self.window
        recent_per_sec = recent_reqs / 2
        burst_ratio = (recent_per_sec / req_per_sec) if req_per_sec > 0 else 1.0
        deny_rate = (total_10s_denied / total_10s_reqs) if total_10s_reqs > 0 else 0.0
        
        current_algo = active_algo_global if active_algo_global else "sliding_window"
        
        new_algo = self._decide(req_per_sec, burst_ratio, deny_rate, current_algo)

        if new_algo != current_algo:
            await self.client.set("rl:config:active_algorithm", new_algo)
        
        self._stats.req_per_sec = round(req_per_sec, 2)
        self._stats.burst_ratio = round(burst_ratio, 2)
        self._stats.deny_rate = round(deny_rate, 2)
        self._stats.active_algorithm = new_algo
        self._stats.total_requests = int(total_reqs_global) if total_reqs_global else 0
        self._stats.total_denied = int(total_denied_global) if total_denied_global else 0

    def _decide(self, rps: float, burst_ratio: float, deny_rate: float, current: str) -> Algorithm:
        if deny_rate > 0.5 and rps > 20:
            return "backoff"

        if burst_ratio > 3.0:
            return "token_bucket"

        if burst_ratio < 1.5 and current == "token_bucket":
            return "sliding_window"

        if current == "backoff" and deny_rate < 0.1:
            return "sliding_window"

        return current

    async def _broadcast(self):
        if not self._subscribers:
            return
        data = {
            "req_per_sec": self._stats.req_per_sec,
            "burst_ratio": self._stats.burst_ratio,
            "deny_rate": self._stats.deny_rate,
            "algorithm": self._stats.active_algorithm,
            "total_requests": self._stats.total_requests,
            "total_denied": self._stats.total_denied,
            "timestamp": time.time()
        }
        dead = []
        for q in self._subscribers:
            try:
                q.put_nowait(data)
            except asyncio.QueueFull:
                dead.append(q)
        for q in dead:
            self._subscribers.remove(q)