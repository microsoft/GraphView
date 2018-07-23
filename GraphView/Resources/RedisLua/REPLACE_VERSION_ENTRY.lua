-- A CAS operations to update fields in a version entry
-- replace 3 fields: begin_timestamp, end_timestamp, txId in a version entry if
-- the end_timestamp and txId in the version entry are satisified with the given parameters
-- usage: eval lua 1 record_key version_key begin_timestamp, end_timestamp, txId, read_txId read_end_timestamp -1

local begin_timestamp = ARGV[2]
local end_timestamp = ARGV[3]
local tx_id = ARGV[4]
local read_tx_id = ARGV[5]
local read_end_timestamp = ARGV[6]
-- the uploaded negative_one bytes as the return value
local negative_one = ARGV[7]

local entry = redis.call('HGET', KEYS[1], ARGV[1])
if not entry then
    return nil
end

local entry_tx_id = string.sub(entry, 2*8+1, 3*8)
local entry_end_timestamp = string.sub(entry, 8+1, 2*8)
local max_commit_ts = string.sub(entry, 3*8+1, 4*8)

if entry_tx_id == read_tx_id and entry_end_timestamp == read_end_timestamp then
    local new_version_entry = begin_timestamp .. end_timestamp .. tx_id .. string.sub(entry, 3*8+1, string.len(entry))
    local ret = redis.call('HSET', KEYS[1], ARGV[1], new_version_entry);
    if ret == nil then
        return nil
    end
    return new_version_entry
else
    return entry
end