local hashKey = KEYS[1]
local versionKey = ARGV[1]
local payload = ARGV[2]
local success = {""}
local fail = {}

if redis.call('HSETNX', hashKey, versionKey, payload) ~= 1 then
    return fail
end

local function CheckNewVersion(hashKey, newVersion)
    local function BytesToInt(bytes)
        local result = 0
        for i = #bytes, 1, -1 do
            result = result * 256 + string.byte(bytes, i)
        end
        return result
    end
    local function Assert(b, message)
        if not b then
            error(message)
        end
    end
    local oldKey = redis.call('HGET', hashKey, 'LATEST_VERSION')
    local newVersionNum = BytesToInt(newVersion)
    if oldKey == false or oldKey == nil then
        Assert(newVersion == 0, "starting version num isn't 0 of key" .. hashKey)
        return
    end
    local oldKeyNum = BytesToInt(oldKey)
    Assert(
        oldKeyNum == newVersionNum - 1,
        "Old version: " .. tostring(oldKeyNum) ..
        ", new version: " .. tostring(newVersionNum) ..
        ", hashKey " .. hashKey)
end

-- CheckNewVersion(hashKey, versionKey)

-- field `LATEST_VERSION` store the lastest version number,
-- usage see `GET_VERSION_LIST.lua`
-- The correctness of this line relies on the lua-script caller
redis.call('HSET', hashKey, 'LATEST_VERSION', versionKey)

return success