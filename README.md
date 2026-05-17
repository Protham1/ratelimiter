<h1 align="center">Distributed RateLimiter</h1>

<p align="center">
  <strong>A high-performance, distributed rate-limiting service that dynamically switches algorithms based on real-time traffic patterns.</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/python-3.11+-blue?logo=python&logoColor=white" alt="Python"/>
  <img src="https://img.shields.io/badge/FastAPI-0.100+-00C7B7?logo=fastapi&logoColor=white" alt="FastAPI"/>
  <img src="https://img.shields.io/badge/Redis-7.0+-DC382D?logo=redis&logoColor=white" alt="Redis"/>
  <img src="https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker&logoColor=white" alt="Docker"/>
  <img src="https://github.com/Protham1/ratelimiter/actions/workflows/ci.yml/badge.svg" alt="CI"/>
</p>

---

## 🧠 What is This?

Most rate limiters use a single, static algorithm. This project is different — it implements **three** industry-standard rate-limiting algorithms and uses a real-time **Traffic Monitor** to intelligently switch between them based on live traffic patterns.

| Scenario | Algorithm Selected | Why |
|----------|-------------------|-----|
| Normal, steady traffic | **Sliding Window** | Precise, fair counting per time window |
| Sudden spike / burst | **Token Bucket** | Absorbs short bursts gracefully via token refill |
| Sustained abuse / DDoS | **Exponential Backoff** | Aggressively throttles repeat offenders with hard cooldown |

The system continuously analyzes **Requests Per Second**, **Burst Ratio**, and **Deny Rate** to make this decision — all computed from global Redis metrics, not local memory.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      Client / Browser                    │
│              (Dashboard + Traffic Simulator)              │
└──────────────────────┬──────────────────────────────────┘
                       │ HTTP / WebSocket
                       ▼
┌─────────────────────────────────────────────────────────┐
│                    FastAPI Server                         │
│                                                          │
│  ┌──────────┐  ┌──────────┐  ┌───────────┐              │
│  │ POST     │  │ GET      │  │ GET       │              │
│  │ /check   │  │ /health  │  │ /metrics  │              │
│  └────┬─────┘  └──────────┘  └───────────┘              │
│       │                                                  │
│       ▼                                                  │
│  ┌─────────────────────────────────────────┐            │
│  │           RateLimiter Core              │            │
│  │                                         │            │
│  │  ┌──────────┐ ┌────────┐ ┌──────────┐  │            │
│  │  │ Sliding  │ │ Token  │ │  Exp.    │  │            │
│  │  │ Window   │ │ Bucket │ │ Backoff  │  │            │
│  │  │ (Lua)    │ │ (Lua)  │ │ (Lua)   │  │            │
│  │  └──────────┘ └────────┘ └──────────┘  │            │
│  └─────────────────┬───────────────────────┘            │
│                    │                                     │
│       ┌────────────▼────────────┐                       │
│       │    Traffic Monitor      │                       │
│       │  (Hybrid Redis Sync)    │                       │
│       │                         │                       │
│       │  Local Counter → Redis  │                       │
│       │  Redis → Global Stats   │                       │
│       │  Global Stats → Decide  │                       │
│       └─────────────────────────┘                       │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
              ┌─────────────────┐
              │      Redis      │
              │                 │
              │  Sorted Sets    │  ← Sliding Window
              │  Hash Maps      │  ← Token Bucket
              │  Key/Value      │  ← Backoff Levels, Cooldown
              │  Metric Buckets │  ← Global Analytics
              │  Config Store   │  ← Algorithm Consensus
              └─────────────────┘
```

---

## ⚡ Rate-Limiting Algorithms

### 1. Sliding Window (Lua Script)

The default algorithm. Uses a **Redis Sorted Set** where each request is stored with its timestamp as the score. On every check, the Lua script atomically:

1. Removes all entries older than the window (`ZREMRANGEBYSCORE`)
2. Counts remaining entries (`ZCARD`)
3. Allows or denies based on the count vs. limit
4. Sets a TTL on the key for automatic cleanup

```lua
-- Atomic: no race conditions, no double counting
redis.call('ZREMRANGEBYSCORE', key, '-inf', cutoff)
local count = redis.call('ZCARD', key)
if count < limit then
    redis.call('ZADD', key, now, req_id)
end
```

**Why Lua?** Redis executes Lua scripts atomically — no other command can interleave. This eliminates race conditions that would occur with separate `GET` → check → `SET` calls in Python.

**Memory complexity:** O(R) per client where R is requests in the current window. At a limit of 100 req/min, worst case is ~13KB per client. For high-volume use cases, Token Bucket's O(1) footprint is preferable.

### 2. Token Bucket (Lua Script)

Activated when the **Burst Ratio exceeds 3.0x**. Uses a **Redis Hash** to store the current token count and last refill timestamp. On every check:

1. Calculates elapsed time since last refill
2. Adds `elapsed × refill_rate` tokens (capped at capacity)
3. If tokens ≥ 1, allows the request and decrements
4. Otherwise, denies with a `retry_after` hint

```lua
local elapsed = math.max(0, now - last_refill)
local refill = math.floor(elapsed * refill_rate)
tokens = math.min(capacity, tokens + refill)
```

**Why Token Bucket for bursts?** Unlike Sliding Window which has a hard cutoff, Token Bucket allows accumulated tokens to absorb short spikes gracefully. It's also O(1) memory per client regardless of request volume.

### 3. Exponential Backoff (Lua Script)

Activated when **Deny Rate > 50% AND RPS > 20** (sustained abuse). All state — level escalation, deny tracking, cooldown — runs inside a single atomic Lua script to prevent race conditions under concurrent load.

Effective limit is dynamically halved per level:

```
effective_limit = max(1, base_limit ÷ 2^level)
```

Every 10 consecutive denied requests escalates the backoff level. At level 5, a **hard cooldown** is triggered — all requests from that client are rejected until the TTL expires.

| Level | Effective Limit (base=100) | Trigger |
|-------|---------------------------|---------|
| 0 | 100 | Default |
| 1 | 50 | 10 denials |
| 2 | 25 | 20 denials |
| 3 | 12 | 30 denials |
| 4 | 6 | 40 denials |
| 5 | 3 → Hard Cooldown | 50 denials |

**De-escalation:** When a client's request counter expires naturally (window reset), they are de-escalated one level. This rewards sustained good behavior without adding overhead to the hot path.

**Responses include a `retry_after` hint** equal to `2^level` seconds, giving well-behaved clients a backoff signal.

---

## 🔄 Hybrid Redis Architecture (The Key Innovation)

The biggest challenge in distributed rate limiting is: **how do multiple server instances agree on traffic patterns without each request paying a round-trip to Redis?**

### The Problem
If you run 3 FastAPI servers behind a load balancer, each server only sees ~33% of the traffic. Server A might think traffic is calm while Server B is getting hammered.

### The Solution: Local Aggregation + Global Consensus

Every **1 second**, each server instance executes a Redis pipeline:

```
┌─ Step 1: FLUSH ──────────────────────────────────┐
│  Push local counters to Redis time-buckets        │
│  INCRBY rl:metrics:reqs:{timestamp} {local_count} │
│  EXPIRE rl:metrics:reqs:{timestamp} 20            │
└───────────────────────────────────────────────────┘
            │
┌─ Step 2: FETCH ──────────────────────────────────┐
│  MGET all 10 most recent 1-second buckets         │
│  (Combined data from ALL servers)                 │
└───────────────────────────────────────────────────┘
            │
┌─ Step 3: DECIDE ─────────────────────────────────┐
│  Calculate global RPS, Burst Ratio, Deny Rate     │
│  Run algorithm selection logic                    │
│  SET rl:config:active_algorithm {algo}            │
└───────────────────────────────────────────────────┘
```

### Why This is Fast
- **`record()` is zero-latency**: Just increments a Python integer (`self._local_reqs += 1`). No Redis call, no I/O, no blocking.
- **Sync is batched**: All Redis operations are packed into a single **pipeline** (1 round-trip, not 20+).
- **Buckets auto-expire**: Each metric bucket has a 20-second TTL, so Redis memory stays constant regardless of traffic volume.

---

## 🖥️ Real-Time Dashboard

The project includes a sleek, dark-themed monitoring dashboard served at the root URL (`/`). It features:

- **Live WebSocket streaming** of all traffic metrics (updates every 1 second)
- **Color-coded thresholds** — values turn yellow/red as they approach danger zones
- **Algorithm badges** with pulse animations when the active algorithm switches
- **Built-in Traffic Simulator** — visitors can generate real traffic directly from the browser:
  - 🟢 **Normal Traffic** — 5 req/s for 5 seconds (stays on Sliding Window)
  - 🟡 **Send Burst** — 100 concurrent requests (triggers Token Bucket)
  - 🔴 **Sustained Spam** — 30 req/s continuously (triggers Exponential Backoff)

---

## 📡 API Reference

### `POST /check`
Test the rate limiter against a key.

```json
// Request
{ "key": "user_123", "limit": 100, "window": 60 }

// Response (allowed)
{
  "key": "user_123",
  "allowed": true,
  "remaining": 94,
  "algorithm": "sliding_window",
  "retry_after": 0.0,
  "backoff_level": 0
}

// Response (denied)
{
  "key": "user_123",
  "allowed": false,
  "remaining": 0,
  "algorithm": "exponential_backoff",
  "retry_after": 4.0,
  "backoff_level": 2
}
```

### `GET /health`
Returns Redis connectivity status. Useful for load balancer health checks.
```json
{ "status": "ok", "redis": "connected" }
```

### `GET /metrics`
Returns real-time global traffic analytics as JSON.
```json
{
  "req_per_sec": 42.5,
  "burst_ratio": 1.8,
  "deny_rate": 0.12,
  "active_algorithm": "token_bucket",
  "total_requests": 15234,
  "total_denied": 1829
}
```

All responses include an `X-Process-Time` header (e.g., `0.002s`) for latency observability.

---

## 📊 Benchmarks (k6)

Benchmarked with [k6](https://k6.io/) using a 60-second test ramping from 0 → 50 → 300 virtual users against a **3-instance cluster load-balanced via Nginx**.

### 3-Instance Cluster (Current)

| Metric | Value |
|--------|-------|
| Total Requests | 120,038 |
| Throughput | ~1,000 req/s |
| Avg Latency | 24.66ms |
| Median Latency | 14.07ms |
| p90 Latency | 59.39ms |
| p95 Latency | 75.01ms |
| Max Latency | 253.44ms |
| Checks Passed | 100% |
| HTTP Failures | 0% |

### Single Instance (Baseline)

| Metric | Value |
|--------|-------|
| Throughput | ~561 req/s |
| Avg Latency | 122ms |
| p95 Latency | 314ms |

### What the Numbers Mean

The 3-instance cluster delivers ~349 req/s per instance vs the 561 req/s single-instance baseline — a ~38% per-instance reduction. This overhead is the **coordination cost**: cross-instance quota sync, shared Lua state in Redis, and atomic deny tracking. This is an intentional correctness-performance tradeoff — incorrect rate limiting decisions under concurrent load are worse than slightly higher throughput.

The median latency of 14ms vs avg of 24ms indicates a latency tail: ~10% of requests are slower due to Redis queue depth under peak concurrency (300 VUs). The hot path consistently delivers sub-15ms.

```bash
# Run the benchmark yourself
k6 run k6_benchmark.js

# Or via Docker
docker run --rm -v ${PWD}:/scripts -w /scripts grafana/k6 run k6_benchmark.js
```

---

## 🔧 Performance Optimizations

| Optimization | Detail |
|-------------|--------|
| **All Lua Scripts** | All three algorithms run as atomic Redis scripts — zero race conditions across sliding window, token bucket, and exponential backoff |
| **Redis Pipelines** | All monitoring I/O is batched into a single pipeline (1 round-trip per second) |
| **Zero-Latency Recording** | `record()` increments a local Python integer — no I/O on the hot path |
| **Auto-Expiring Buckets** | Metric keys have 20s TTL, keeping Redis memory constant |
| **Minimal Redis Calls** | Hot path averages 3–4 Redis ops per request; expensive ops (TTL refresh, level escalation) only fire on state transitions |
| **Multi-Stage Docker** | Builder pattern keeps the final image lean (~150MB vs ~800MB) |
| **Script Preloading** | Lua scripts are loaded once via `SCRIPT LOAD` and called by SHA — no re-parsing |
| **Async Everything** | Fully async FastAPI + `redis.asyncio` — no thread blocking |

---

## 🧩 Design Decisions & Tradeoffs

### Why Redis as the coordination layer?
Redis gives atomic Lua execution, sub-millisecond ops, and sidesteps distributed clock skew by making Redis the single time authority. The tradeoff is Redis becomes a single point of failure. In production this would be mitigated with Redis Sentinel (automatic failover) or a circuit breaker that falls back to per-instance in-memory limiting if Redis becomes unreachable.

### Fail open vs fail closed
If Redis goes down, the current system fails. The correct production behavior is **fail open with a circuit breaker** — detect Redis unavailability, switch to local in-memory limiting per instance, alert loudly, and resume coordinated limiting once Redis recovers. A rate limiter should never be more fragile than the service it protects.

### The 1-second sync window
Global metrics are up to 1 second stale. In the worst case, a client could get `N × limit` requests through across N instances within a single sync window. For stricter guarantees, a **distributed quota leasing** model (each instance pre-leases a token chunk from Redis, replenishes as needed) would reduce the stale window to near-zero while preserving the zero-latency local hot path.

### Algorithm switching hysteresis
Algorithm selection is currently evaluated every sync cycle. If traffic metrics oscillate around a threshold, the system could flap between algorithms. A production implementation would add a minimum dwell time (e.g., stay on the current algorithm for at least 5 seconds before switching) to prevent instability.

---

## ⚠️ Known Limitations

- **Single Redis instance**: No replication or failover. A Redis crash takes down coordinated limiting.
- **No multi-node correctness benchmark**: Distributed correctness (verifying no client exceeds global limits across instances) has not been formally benchmarked — only throughput and latency have been measured across instances.
- **Algorithm switching has no hysteresis**: Rapid threshold oscillation can cause frequent algorithm switches.
- **1-second coordination lag**: Global traffic view is eventually consistent within a 1-second window.

---

## 🚀 Getting Started

### Prerequisites
- Python 3.11+
- Redis (running locally or via Docker)
- Docker (optional, for containerized setup)

### Option 1: Local Development

```bash
# Clone the repo
git clone https://github.com/Protham1/ratelimiter.git
cd ratelimiter

# Create virtual environment
python -m venv venv
.\venv\Scripts\activate  # Windows
# source venv/bin/activate  # macOS/Linux

# Install dependencies
pip install -e ".[test]"

# Start the server (requires Redis on localhost:6379)
uvicorn api.main:app --reload
```

### Option 2: Docker Compose (Recommended)

```bash
# Spins up Redis + single API instance
docker-compose up --build

# Scale to 3 instances behind Nginx
docker-compose up --build --scale api=3

# Access the dashboard
open http://localhost:8000
```

### Running Tests

```bash
pytest
```

---

## 📁 Project Structure

```
ratelimiter/
├── api/
│   ├── main.py              # FastAPI app, endpoints, middleware
│   └── dashboard.html       # Real-time monitoring UI
├── ratelimiter/
│   ├── core.py              # RateLimiter orchestrator
│   ├── monitor.py           # Hybrid Redis traffic monitor
│   ├── algorithms/
│   │   ├── sliding_window.py
│   │   ├── token_bucket.py
│   │   └── backoff.py
│   └── scripts/
│       ├── sliding_window.lua
│       ├── token_bucket.lua
│       └── backoff.lua      # Full atomic backoff: escalation, cooldown, de-escalation
├── tests/
│   ├── conftest.py          # Pytest fixtures (Redis cleanup)
│   ├── test_algorithms.py   # Unit tests for all 3 algorithms
│   └── test_api.py          # Integration tests for API endpoints
├── .github/workflows/
│   └── ci.yml               # GitHub Actions CI pipeline
├── Dockerfile               # Multi-stage production build
├── docker-compose.yml       # Full stack orchestration
├── k6_benchmark.js          # Load testing script
└── pyproject.toml           # Dependencies & build config
```

---

## 🛠️ DevOps & CI/CD

- **GitHub Actions**: Every push to `main` triggers automated tests against a Redis service container
- **Docker Compose**: One-command local orchestration of API + Redis, with `--scale` support for multi-instance testing
- **Railway Ready**: Configured with dynamic `PORT` binding and `REDIS_URL` env var for instant cloud deployment

---

## 📜 License

This project is open source and available under the [MIT License](LICENSE).

---

<p align="center">
  Built with ❤️ using Python, FastAPI, Redis, and Lua
</p>