local level_key = KEYS[1]
local count_key = KEYS[2]
local deny_key = KEYS[3]
local cooldown_key = KEYS[4]

local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local cooldown_time = tonumber(ARGV[3])

-- 1. Check hard cooldown
local cooldown_ttl = redis.call("TTL", cooldown_key)
if cooldown_ttl > 0 then
    -- Already in cooldown
    return {0, 0, 5, cooldown_ttl}
end

-- 2. Fetch current level
local level_str = redis.call("GET", level_key)
local level = 0
if level_str then
    level = tonumber(level_str)
end

-- 3. Calculate effective limit
local effective_limit = math.floor(limit / (2 ^ level))
if effective_limit < 1 then
    effective_limit = 1
end

-- 4. Check limit
local count = redis.call("INCR", count_key)
if count == 1 then
    redis.call("EXPIRE", count_key, window)
end

local allowed = 0
if count <= effective_limit then
    allowed = 1
end

-- 5. Process logic
local retry_after = 0
if allowed == 1 then
    -- Reset deny count on success
    redis.call("DEL", deny_key)
else
    local deny_count = redis.call("INCR", deny_key)
    if deny_count == 1 then
        redis.call("EXPIRE", deny_key, 300)
    end
    
    -- Escalate level every 10 consecutive denials
    if deny_count % 10 == 0 then
        level = level + 1
        if level >= 5 then
            level = 5
            -- Trigger Hard Cooldown
            redis.call("SET", cooldown_key, 1, "EX", cooldown_time)
            retry_after = cooldown_time
        end
        redis.call("SET", level_key, level, "EX", 300)
    end
end

local remaining = effective_limit - count
if remaining < 0 then
    remaining = 0
end

return {allowed, remaining, level, retry_after}
