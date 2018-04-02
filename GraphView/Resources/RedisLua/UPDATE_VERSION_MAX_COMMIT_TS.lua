-- eval lua 1 record_key, version_key commit_time -1
local entry = redis.call('HGET', KEYS[1], ARGV[1])
if not entry then
    return ARGV[3]
end

local tx_id = string.sub(entry, 2*8+1, 3*8)
local max_commit_ts = string.sub(entry, 3*8+1, 4*8)

-- cann't compare strings directly, "2" < "15" will return false 
if tx_id == ARGV[3] and string.byte(max_commit_ts) < string.byte(ARGV[2]) then
    local new_version_entry = string.sub(entry, 1, 3*8) .. ARGV[2] .. string.sub(entry, 4*8+1, string.len(entry))
    local ret = redis.call('HSET', KEYS[1], ARGV[1], new_version_entry);
    if ret == nil then
        return ARGV[3]
    end
    return new_version_entry
else
    return entry
end