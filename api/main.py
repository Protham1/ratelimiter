import asyncio
import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse

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

@app.get("/check/{key}")
async def check_rate_limit(key: str, limit: int = None, window: int = None):
    """
    Manually test the rate limiter.
    Example: /check/user_123
    """
    try:
        result = await rl.check(key, limit=limit, window=window)
        return {
            "key": key,
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
