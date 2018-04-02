-- eval lua 1 record_key version_key begin_timestamp, end_timestamp, txId, read_txId -1
local entry = redis.call('HGET', KEYS[1], ARGV[1])
local begin_timestamp = ARGV[2]
local end_timestamp = ARGV[3]
local tx_id = ARGV[4]
local read_tx_id = ARGV[5]

if not entry then
    return ARGV[6]
end

local entry_tx_id = string.sub(entry, 2*8+1, 3*8)
local max_commit_ts = string.sub(entry, 3*8+1, 4*8)

if entry_tx_id == read_tx_id then
    local new_version_entry = begin_timestamp .. end_timestamp .. tx_id .. string.sub(entry, 3*8+1, string.len(entry))
    local ret = redis.call('HSET', KEYS[1], ARGV[1], new_version_entry);
    if ret == nil then
        return ARGV[6]
    end
    return max_commit_ts
else
    return ARGV[6]
end