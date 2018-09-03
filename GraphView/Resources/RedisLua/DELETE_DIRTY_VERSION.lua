local hashKey = KEYS[1]
local versionKey = ARGV[1]

-- lua doesn't support actual 'integer' before 5.3
-- So in theory, this code could blow up due to floating point precision issue.
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

local SUCCESS = {""}
local FAIL = {}

if redis.call('HDEL', hashKey, versionKey) ~= 1 then
    return FAIL
end

-- this relies on the caller is actually deleting the dirty version
local currentLatest = IntToByteString(BytesToInt(versionKey) - 1)
redis.call('HSET', hashKey, 'LATEST_VERSION', currentLatest)

return SUCCESS