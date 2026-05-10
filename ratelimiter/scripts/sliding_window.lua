local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local req_id = ARGV[4]

local cutoff = now - window

redis.call('ZREMRANGEBYSCORE', key, '-inf', cutoff)

local count = redis.call('ZCARD', key)

local allowed = 0
local remaining = 0

if count < limit then
    allowed = 1
    remaining = limit - count - 1
    redis.call('ZADD', key, now, req_id)
else
    remaining = 0
end

redis.call('EXPIRE', key, window + 10)

return {allowed, remaining}