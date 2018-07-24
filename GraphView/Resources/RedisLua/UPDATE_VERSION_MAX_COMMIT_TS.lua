-- A CAS operation to update version entry' max_commit_ts if local commit_time
-- is smaller than max_commit_ts
-- usage: eval lua 1 record_key, version_key commit_time -1

local commit_time = ARGV[2]
local negative_one = ARGV[3]

local entry = redis.call('HGET', KEYS[1], ARGV[1])
if not entry then
    return negative_one
end

local max_commit_ts = string.sub(entry, 3*8+1, 4*8)

-- cann't compare strings directly, "2" < "15" will return false 
if string.byte(max_commit_ts) < string.byte(commit_time) then
    local new_version_entry = string.sub(entry, 1, 3*8) .. commit_time .. string.sub(entry, 4*8+1, string.len(entry))
    local ret = redis.call('HSET', KEYS[1], ARGV[1], new_version_entry);
    if ret == nil then
        return negative_one
    end
    return new_version_entry
else
    return entry
end