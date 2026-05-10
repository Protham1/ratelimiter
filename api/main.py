import time
import asyncio
import json
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from ratelimiter.core import RateLimiter

# Initialize the rate limiter
rl = RateLimiter(redis_url="redis://localhost:6379")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Connect to Redis and start the background monitor when the app starts
    await rl.connect()
    yield
    # Graceful shutdown
    await rl.disconnect()

app = FastAPI(lifespan=lifespan, title="RateLimiter Dashboard")

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response

def get_dashboard_html():
    try:
        with open("api/dashboard.html", "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Dashboard HTML not found. Please create api/dashboard.html.</h1>"

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Serve the dashboard HTML page."""
    return get_dashboard_html()

@app.get("/health")
async def health_check():
    """Check system health and Redis connectivity."""
    try:
        await rl._client.ping()
        return {"status": "ok", "redis": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail="Redis connection failed")

@app.get("/metrics")
async def get_metrics():
    """Expose global traffic statistics."""
    stats = rl.monitor.get_stats()
    return {
        "req_per_sec": stats.req_per_sec,
        "burst_ratio": stats.burst_ratio,
        "deny_rate": stats.deny_rate,
        "active_algorithm": stats.active_algorithm,
        "total_requests": stats.total_requests,
        "total_denied": stats.total_denied
    }

class CheckRequest(BaseModel):
    key: str
    limit: Optional[int] = None
    window: Optional[int] = None

@app.post("/check")
async def check_rate_limit(request: CheckRequest):
    """
    Manually test the rate limiter via POST payload.
    """
    try:
        result = await rl.check(request.key, limit=request.limit, window=request.window)
        return {
            "key": request.key,
            "allowed": result.allowed,
            "remaining": result.remaining,
            "algorithm": result.algorithm,
            "retry_after": result.retry_after,
            "backoff_level": result.backoff_level
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.websocket("/ws/stats")
async def websocket_stats(websocket: WebSocket):
    """
    WebSocket endpoint for real-time stats from the TrafficMonitor.
    """
    await websocket.accept()
    queue = await rl.monitor.subscribe()
    try:
        while True:
            # Wait for stats data from the monitor
            data = await queue.get()
            await websocket.send_json(data)
    except WebSocketDisconnect:
        pass
    finally:
        await rl.monitor.unsubscribe(queue)
