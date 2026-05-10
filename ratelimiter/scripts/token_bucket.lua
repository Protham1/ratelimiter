local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local data = redis.call('HMGET', key, 'tokens', 'last_refill')

local tokens = tonumber(data[1])
local last_refill = tonumber(data[2])

if tokens == nil then
    tokens = capacity
    last_refill = now
end

local elapsed = math.max(0, now - last_refill)
local refill = math.floor(elapsed * refill_rate)
tokens = math.min(capacity, tokens + refill)

local allowed = 0
local remaining = tokens

if tokens >= requested then
    allowed = 1
    remaining = tokens - requested
    redis.call('HMSET', key, 'tokens', remaining, 'last_refill', now)
else
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
end

redis.call('EXPIRE', key, 3600)

return {allowed, remaining}