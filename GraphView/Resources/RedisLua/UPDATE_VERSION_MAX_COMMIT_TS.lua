-- eval lua 1 record_key, version_key commit_time read_tx_id -1
local commit_time = ARGV[2]
local read_tx_id = ARGV[3]
local negative_one = ARGV[4]

local entry = redis.call('HGET', KEYS[1], ARGV[1])
if not entry then
    return negative_one
end

local tx_id = string.sub(entry, 2*8+1, 3*8)
local max_commit_ts = string.sub(entry, 3*8+1, 4*8)

-- cann't compare strings directly, "2" < "15" will return false 
if (tx_id == read_tx_id or tx_id == negative_one) and string.byte(max_commit_ts) < string.byte(commit_time) then
    local new_version_entry = string.sub(entry, 1, 3*8) .. commit_time .. string.sub(entry, 4*8+1, string.len(entry))
    local ret = redis.call('HSET', KEYS[1], ARGV[1], new_version_entry);
    if ret == nil then
        return negative_one
    end
    return new_version_entry
else
    return entry
end