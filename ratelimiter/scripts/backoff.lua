local level_key = KEYS[1]
local count_key = KEYS[2]
local deny_key = KEYS[3]
local cooldown_key = KEYS[4]

local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local cooldown_time = tonumber(ARGV[3])

-- 1. Check hard cooldown first (fast exit)
local cooldown_ttl = redis.call("TTL", cooldown_key)
if cooldown_ttl > 0 then
    return {0, 0, 5, cooldown_ttl}
end

-- 2. Fetch current level
local level = tonumber(redis.call("GET", level_key)) or 0

-- 3. Calculate effective limit
local effective_limit = math.max(math.floor(limit / (2 ^ level)), 1)

-- 4. Increment and set TTL atomically on first request
local count = redis.call("INCR", count_key)
if count == 1 then
    redis.call("EXPIRE", count_key, window)
end

-- 5. Allow/deny decision
local allowed = count <= effective_limit and 1 or 0

-- 6. Process outcome
local retry_after = 0

if allowed == 1 then
    -- FIX 1: Don't DEL deny_key on every success — only reset after
    -- sustained good behavior, otherwise a single allowed request
    -- between denials resets the escalation progress unfairly
    local deny_count = tonumber(redis.call("GET", deny_key)) or 0
    if deny_count > 0 then
        local good_count = redis.call("INCR", "good:" .. count_key)
        if good_count >= 20 then
            -- Sustained good behavior: de-escalate one level
            redis.call("DEL", "good:" .. count_key)
            redis.call("DEL", deny_key)
            if level > 0 then
                level = level - 1
                redis.call("SET", level_key, level, "EX", 300)
            end
        end
    end
else
    -- FIX 2: deny_key TTL should refresh on each denial, not just first
    -- otherwise deny count expires mid-escalation and resets unfairly
    local deny_count = redis.call("INCR", deny_key)
    redis.call("EXPIRE", deny_key, 300)  -- refresh TTL every denial

    if deny_count % 10 == 0 then
        level = level + 1
        if level >= 5 then
            level = 5
            redis.call("SET", cooldown_key, 1, "EX", cooldown_time)
            retry_after = cooldown_time
        end
        -- FIX 3: level TTL should also refresh on escalation
        redis.call("SET", level_key, level, "EX", 300)
    end

    -- FIX 4: reset good count on denial so de-escalation
    -- requires sustained good behavior, not interleaved
    redis.call("DEL", "good:" .. count_key)

    retry_after = retry_after > 0 and retry_after or math.pow(2, level)
end

-- FIX 5: return current level, not stale pre-escalation level
local remaining = math.max(effective_limit - count, 0)
return {allowed, remaining, level, retry_after}