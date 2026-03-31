-- KEYS[1]: stockKey, KEYS[2]: orderKey
-- ARGV[1]: userId
local stockKey = KEYS[1]
local orderKey = KEYS[2]
local userId = ARGV[1]

if redis.call('sismember', orderKey, userId) == 1 then
    redis.call('srem', orderKey, userId)
    redis.call('incrby', stockKey, 1)
    return 1
end
return 0
