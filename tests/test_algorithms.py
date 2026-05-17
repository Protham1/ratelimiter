import pytest
import asyncio
from ratelimiter.core import RateLimiter

@pytest.mark.asyncio
async def test_sliding_window_limits(rate_limiter: RateLimiter):
    # Force the active algorithm to sliding_window
    rate_limiter.monitor._stats.active_algorithm = "sliding_window"
    
    key = "sw_test_user"
    
    # Capacity is 5 in the fixture. 
    # The first 5 requests should be allowed.
    for _ in range(5):
        result = await rate_limiter.check(key)
        assert result.allowed is True
        
    # The 6th request should be denied
    result = await rate_limiter.check(key)
    assert result.allowed is False
    assert result.algorithm == "sliding_window"

@pytest.mark.asyncio
async def test_token_bucket_refills(rate_limiter: RateLimiter):
    # Force the active algorithm to token_bucket
    rate_limiter.monitor._stats.active_algorithm = "token_bucket"
    
    key = "tb_test_user"
    
    # Exhaust the bucket (capacity 5)
    for _ in range(5):
        result = await rate_limiter.check(key)
        assert result.allowed is True
        
    result = await rate_limiter.check(key)
    assert result.allowed is False
    assert result.retry_after > 0
    
    # Wait for exactly 1 token to refill (refill_rate is 2.0/sec -> 0.5 sec per token)
    # We wait 0.6 seconds to be safe
    await asyncio.sleep(0.6)
    
    # Should now be allowed
    result = await rate_limiter.check(key)
    assert result.allowed is True
    
    # Should be immediately exhausted again
    result = await rate_limiter.check(key)
    assert result.allowed is False

@pytest.mark.asyncio
async def test_exponential_backoff_triggers(rate_limiter: RateLimiter):
    rate_limiter.monitor._stats.active_algorithm = "backoff"
    key = "bo_test_user"
    
    # Flood the backoff algorithm to reach 10 consecutive denials
    # Capacity is 5, so 5 allowed + 10 denied = 15 requests
    for _ in range(15):
        await rate_limiter.check(key)
        
    # Now it should be denying AND have an increased backoff level
    result = await rate_limiter.check(key)
    assert result.allowed is False
    assert result.algorithm == "backoff"
    assert result.backoff_level > 0
