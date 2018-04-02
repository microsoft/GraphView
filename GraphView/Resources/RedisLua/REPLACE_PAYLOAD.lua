-- eval lua 1 record_key version_key beginTimestamp endTimestamp -1 0
local negative_one = ARGV[4]
local zero = ARGV[5]

local entry = redis.call('HGET', KEYS[1], ARGV[1])
if not entry then
    return negative_one
end

local new_entry = ARGV[2] .. ARGV[3] .. string.sub(entry, 2*8+1, string.len(entry))
local ret = redis.call('HSET', KEYS[1], ARGV[1], new_entry)
if ret == 0 then
    return zero
end
return negative_one