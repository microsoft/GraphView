local hashKey = KEYS[1]
local versionKey = ARGV[1]
local payload = ARGV[2]
local success = {""}
local fail = {}

if redis.call('HSETNX', hashKey, versionKey, payload) ~= 1 then
    return fail
end

-- field `__LATEST` store the lastest version number,
-- usage see `GET_VERSION_LIST.lua`
-- The correctness of this line relies on the lua-script caller
redis.call('HSET', hashKey, '__LATEST', versionKey)

return success