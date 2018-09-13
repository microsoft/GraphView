local hashKey = KEYS[1]
local versionKey = ARGV[1]
local payload = ARGV[2]

local function BytesToInt(bytes)
    local result = 0
    for i = #bytes, 1, -1 do
        result = result * 256 + string.byte(bytes, i)
    end
    return result
end

local function IntToBytes(val)
    local result = {}
    for i = 1, 8 do
        result[i] = val % 256
        val = math.floor(val / 256)
    end
    return result
end

local function IntToByteString(val)
    return string.char(unpack(IntToBytes(val)))
end

local function CheckNewVersionIsValid(hashKey, newVersion)
    local function Assert(b, message)
        if not b then
            error(message)
        end
    end
    local oldKey = redis.call('HGET', hashKey, 'LATEST_VERSION')
    local newVersionNum = BytesToInt(newVersion)
    if oldKey == false or #oldKey == 0 then
        return
    end
    local oldKeyNum = BytesToInt(oldKey)
    Assert(
        oldKeyNum == newVersionNum - 1,
        "Old version: " .. tostring(oldKeyNum) ..
        ", new version: " .. tostring(newVersionNum) ..
        ", hashKey " .. hashKey)
end

local function DeleteOneOldVersion(maxVersionListLength)
    local oldVersion = BytesToInt(versionKey) - maxVersionListLength
    if oldVersion < 0 then return end
    redis.call('HDEL', hashKey, IntToByteString(oldVersion))
end

local function RecycleOldVersions()
    local MAX_VERSION_LIST_LENGTH = 32
    DeleteOneOldVersion(MAX_VERSION_LIST_LENGTH)
end

local SUCCESS = {""}
local FAIL = {}

if redis.call('HSETNX', hashKey, versionKey, payload) ~= 1 then
    return FAIL
end

-- CheckNewVersionIsValid(hashKey, versionKey)

-- field `LATEST_VERSION` store the lastest version number,
-- usage see `GET_VERSION_LIST.lua`
-- The correctness of this line relies on the lua-script caller
redis.call('HSET', hashKey, 'LATEST_VERSION', versionKey)
RecycleOldVersions()

return SUCCESS