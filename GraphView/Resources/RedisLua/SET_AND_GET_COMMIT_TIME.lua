-- eval lua_script 1 txId try_commit_time -1
local try_commit_time = ARGV[1]
local negative_one = ARGV[2]

local tx_entry = redis.call('HMGET', KEYS[1], 'commit_time', 'commit_lower_bound')

if not tx_entry then
    return negative_one
end

local commit_time = tx_entry[1]
local commit_lower_bound = tx_entry[2]

if commit_time == negative_one and 
    string.byte(commit_lower_bound) <= string.byte(try_commit_time) then
    local ret = redis.call("HSET", KEYS[1], "commit_time", try_commit_time)
    if ret == 0 then
        return try_commit_time
    end
    return negative_one
end
return negative_one