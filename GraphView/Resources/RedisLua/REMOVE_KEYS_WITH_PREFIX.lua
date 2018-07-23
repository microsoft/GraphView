-- remove keys in redis with specify prefix, which is used to clear version database.
-- like removing all version entries with prefix "ver:" or all tx entries with prefix "tx:"
-- usage: eval lua 0 prefix

local prefix = ARGV[1]
local batch_size = 5000

local keys = redis.call('keys', prefix..'*')
for i=1, #keys, batch_size
do
	redis.call('del', unpack(keys, i, math.min(i+batch_size-1, #keys)))
end