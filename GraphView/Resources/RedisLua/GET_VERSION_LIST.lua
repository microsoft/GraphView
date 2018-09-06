local hashKey = KEYS[1]

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
    if val < 0 then return {} end
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

local function Assert(b, message)
    if not b then error(message) end
end

-- entry doesn't exist
local latestVersion = redis.call('HGET', hashKey, 'LATEST_VERSION')

Assert(
    latestVersion ~= false,
    "LATEST_VERSION field doesn't exist for hashKey " .. hashKey)

-- LATEST_VERSION exists, but the version is not yet uploaded
if #latestVersion ~= 8 then
    return {}
end

local secondLatestVersion = IntToByteString(BytesToInt(latestVersion) - 1)
local candidates = redis.call(
    'HMGET', hashKey, latestVersion, secondLatestVersion)

local result = nil

if candidates[1] == false then return {} end

if candidates[2] == false then
    result = {latestVersion, candidates[1]}
else
    result = {
        secondLatestVersion, candidates[2], latestVersion, candidates[1]}
end

local function CheckVersionListResultIsValid(hashKey, result)
    if #result == 2 then
        Assert(BytesToInt(result[1]) == 0)
        return
    end
    local ver2 = BytesToInt(result[1])
    local ver1 = BytesToInt(result[3])
    Assert(
        ver1 == ver2 + 1,
        "latest ver: " .. tostring(ver1) ..
        ", second latest ver: " .. tostring(ver2) ..
        ", hash key: " .. hashKey)
end

-- CheckVersionListResultIsValid(hashKey, result)

return result