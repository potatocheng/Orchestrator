-- dequeue.lua
-- input:
-- KEYS[1]: queueKey
-- ARGV[1]: TaskPrefix
-- ARGV[2]: currentUnixTime

-- output:
-- nil: failed to execute
local queueKey = KEYS[1]
local TaskPrefix = ARGV[1]
local currentUnixTime = tonumber(ARGV[2])

local taskID = redis.call('ZRANGEBYSCORE', queueKey, 0, currentUnixTime, 'LIMIT', 0, 1)[1]
if not taskID then
    return nil
end
local taskKey = TaskPrefix .. taskID
local encoded = redis.call("HGET", taskKey, 'data')
redis.call('ZREM', queueKey, taskID)
if encoded then
    return encoded
else
    return nil
end