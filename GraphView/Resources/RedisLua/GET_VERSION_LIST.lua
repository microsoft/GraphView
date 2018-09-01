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

-- entry doesn't exist
local latestVersion = redis.call('HGET', hashKey, '__LATEST')
if latestVersion == false then
    return {}
end

local latestPayload = redis.call('HGET', hashKey, latestVersion)
local latestVersionNum = BytesToInt(latestVersion)

local result = nil

if latestVersionNum == 0 then
    return {latestVersion, latestPayload}
else
    local secondLatestVersion = IntToByteString(latestVersionNum - 1)
    local secondLatestPayload = redis.call('HGET', hashKey, secondLatestVersion)
    result = {
        secondLatestVersion, secondLatestPayload, latestVersion, latestPayload}
end

local function CheckVersionListResult(hashKey, result)
    local function Assert(b, message)
        if not b then error(message) end
    end
    if #result == 2 then
        -- version num == 0
        return -- this case is passing for sure
    end
    local ver2 = BytesToInt(result[1])
    local ver1 = BytesToInt(result[3])
    Assert(
        ver1 == ver2 + 1,
        "latest ver: " .. tostring(ver1) ..
        ", second latest ver: " .. tostring(ver2) ..
        ", hash key: " .. hashKey)
end

-- CheckVersionListResult(hashKey, result)

return result