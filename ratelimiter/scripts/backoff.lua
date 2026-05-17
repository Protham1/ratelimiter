local level_key = KEYS[1]
local count_key = KEYS[2]

local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

local level_str = redis.call("GET", level_key)
local level = 0
if level_str then
    level = tonumber(level_str)
end

local effective_limit = math.floor(limit / (2 ^ level))
if effective_limit < 1 then
    effective_limit = 1
end

local count = redis.call("INCR", count_key)
if count == 1 then
    redis.call("EXPIRE", count_key, window)
end

local allowed = 0
if count <= effective_limit then
    allowed = 1
end

if allowed == 0 and count % 10 == 0 then
    local new_level = level + 1
    if new_level > 5 then
        new_level = 5
    end
    redis.call("SET", level_key, new_level, "EX", 300)
end

local remaining = effective_limit - count
if remaining < 0 then
    remaining = 0
end

return {allowed, remaining, level}
