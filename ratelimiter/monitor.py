import time
import asyncio
from collections import deque
from dataclasses import dataclass, field
from typing import Literal

Algorithm = Literal["sliding_window", "token_bucket", "backoff"]

@dataclass
class RequestEvent:
    timestamp: float
    allowed: bool

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
        self._events: deque[RequestEvent] = deque()
        self._stats = TrafficStats()
        self._lock = asyncio.Lock()
        self._running = False
        self._subscribers: list[asyncio.Queue] = []

    async def record(self, allowed: bool):
        async with self._lock:
            now = time.time()
            self._events.append(RequestEvent(timestamp=now, allowed=allowed))
            self._stats.total_requests += 1
            if not allowed:
                self._stats.total_denied += 1

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
        self._subscribers.remove(q)

    def get_stats(self) -> TrafficStats:
        return self._stats

    async def _loop(self):
        while self._running:
            await asyncio.sleep(1)
            await self._compute()
            await self._broadcast()

    async def _compute(self):
        async with self._lock:
            now = time.time()
            cutoff = now - self.window
            burst_cutoff = now - 2

            while self._events and self._events[0].timestamp < cutoff:
                self._events.popleft()

            total = len(self._events)
            denied = sum(1 for e in self._events if not e.allowed)

            req_per_sec = total / self.window if total > 0 else 0.0

            recent = sum(1 for e in self._events if e.timestamp >= burst_cutoff)
            recent_per_sec = recent / 2
            burst_ratio = (recent_per_sec / req_per_sec) if req_per_sec > 0 else 1.0
            deny_rate = (denied / total) if total > 0 else 0.0

            algo = self._decide(req_per_sec, burst_ratio, deny_rate)

            self._stats.req_per_sec = round(req_per_sec, 2)
            self._stats.burst_ratio = round(burst_ratio, 2)
            self._stats.deny_rate = round(deny_rate, 2)
            self._stats.active_algorithm = algo

    def _decide(self, rps: float, burst_ratio: float, deny_rate: float) -> Algorithm:
        current = self._stats.active_algorithm

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