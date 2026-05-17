<p align="center">
  <img src="assets/banner.png" alt="RateLimiter Banner" width="100%"/>
</p>

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
| Sustained abuse / DDoS | **Exponential Backoff** | Aggressively throttles repeat offenders |

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
│  │  │ (Lua)    │ │ (Lua)  │ │ (Lua)    │  │            │
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
              │  Key/Value      │  ← Backoff Levels
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

**Why Token Bucket for bursts?** Unlike Sliding Window which has a hard cutoff, Token Bucket allows accumulated tokens to absorb short spikes gracefully.

### 3. Exponential Backoff (Lua Script)

Activated when **Deny Rate > 50% AND RPS > 20** (sustained abuse). Instead of a fixed limit, the effective limit is dynamically halved:

```
effective_limit = max(1, base_limit ÷ 2^level)
```

Every 10 denied requests, the `backoff_level` increases (up to level 5), making limits progressively stricter:

| Level | Effective Limit (base=100) |
|-------|---------------------------|
| 0 | 100 |
| 1 | 50 |
| 2 | 25 |
| 3 | 12 |
| 4 | 6 |
| 5 | 3 |

The backoff level is stored in Redis with a 5-minute TTL, so it auto-recovers.

---

## 🔄 Hybrid Redis Architecture (The Key Innovation)

The biggest challenge in distributed rate limiting is: **how do multiple server instances agree on traffic patterns?**

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
- **`record()` is zero-latency**: It just increments a Python integer (`self._local_reqs += 1`). No Redis call, no I/O, no blocking.
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
  "algorithm": "token_bucket",
  "retry_after": 0.1,
  "backoff_level": 0
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

Benchmarked with [k6](https://k6.io/) using a 60-second test ramping from 0 → 50 → 300 virtual users against a **3-instance cluster load-balanced via Nginx**:

| Metric | Value |
|--------|-------|
| Total Requests | 59,665 |
| Throughput | ~1,000 req/s |
| Avg Latency | 25.37ms |
| p95 Latency | 71.96ms |
| Checks Passed | 100% |
| HTTP Failures | 0% |

Run the benchmark yourself:
```bash
# Natively
k6 run k6_benchmark.js

# Or via Docker
docker run --rm -v ${PWD}:/scripts -w /scripts grafana/k6 run k6_benchmark.js
```

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
# Spins up both Redis and the API
docker-compose up --build

# Access the dashboard
open http://localhost:8000
```

### Running Tests

```bash
# Requires Redis running (Docker Compose handles this)
pytest
```

---

## 📁 Project Structure

```
ratelimiter/
├── api/
│   ├── main.py              # FastAPI app, endpoints, middleware
│   └── dashboard.html        # Real-time monitoring UI
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
│       └── backoff.lua
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

## 🔧 Performance Optimizations

| Optimization | Detail |
|-------------|--------|
| **Lua Scripts** | Sliding Window and Token Bucket run as atomic Redis scripts — zero race conditions |
| **Redis Pipelines** | All monitoring I/O is batched into a single pipeline (1 round-trip per second) |
| **Zero-Latency Recording** | `record()` increments a local Python integer — no I/O on the hot path |
| **Auto-Expiring Buckets** | Metric keys have 20s TTL, keeping Redis memory constant |
| **Multi-Stage Docker** | Builder pattern keeps the final image lean (~150MB vs ~800MB) |
| **Script Preloading** | Lua scripts are loaded once via `SCRIPT LOAD` and called by SHA — no re-parsing |
| **Async Everything** | Fully async FastAPI + `redis.asyncio` — no thread blocking |

---

## 🛠️ DevOps & CI/CD

- **GitHub Actions**: Every push to `main` triggers automated tests against a Redis service container
- **Docker Compose**: One-command local orchestration of API + Redis
- **Railway Ready**: Configured with dynamic `PORT` binding and `REDIS_URL` env var for instant cloud deployment

---

## 📜 License

This project is open source and available under the [MIT License](LICENSE).

---

<p align="center">
  Built with ❤️ using Python, FastAPI, Redis, and Lua
</p>
