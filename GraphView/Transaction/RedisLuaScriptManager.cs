namespace GraphView.Transaction
{
    using ServiceStack.Redis;
    using System;
    using System.Collections.Generic;
    using System.Text;

    /// <summary>
    /// A redis lua scripts manager to load sha1 from redis and get sha1 by script name
    /// 
    /// Here we assume that those redis lua scripts are registered in redis in advance.
    /// To get sha1 by name easily, we maintain a hashset in Redis meta database (with index 0)
    /// We store the map from script name to sha1 in a hashset in meta db with the name RedisVersionDb.META_SCRIPT_KEY 
    /// When the manager is inititlized, it will load the sha1 map from redis and has a interface to query sha1 by name
    /// 
    /// Those lua scripts are commented at the end of class
    /// </summary>
    internal class RedisLuaScriptManager
    {
        /// <summary>
        /// a init lock for singleton class
        /// </summary>
        private static readonly object initLock = new object();

        /// <summary>
        /// the singleton instance
        /// </summary>
        private static RedisLuaScriptManager instance;

        private IRedisClientsManager RedisManager
        {
            get
            {
                return RedisClientManager.Instance;
            }
        }

        /// <summary>
        /// A transient map from script name to sha1, it will be filled when the instance is created
        /// </summary>
        private readonly Dictionary<string, string> luaScriptSha1Map = new Dictionary<string, string>();

        internal static RedisLuaScriptManager Instance
        {
            get
            {
               if (RedisLuaScriptManager.instance == null)
                {
                    lock (RedisLuaScriptManager.initLock)
                    {
                        if (RedisLuaScriptManager.instance == null)
                        {
                            RedisLuaScriptManager.instance = new RedisLuaScriptManager();
                        }
                    }
                }
                return RedisLuaScriptManager.instance;
            }
        }

        public RedisLuaScriptManager()
        {
            this.LoadLuaScriptMapFromRedis();
        }

        /// <summary>
        /// Get a sha1 by the script name
        /// </summary>
        /// <param name="scriptName">The human readiable name for sha1, like INSERT_VERSION</param>
        /// <returns>script's sha1<returns>
        internal string GetLuaScriptSha1(string scriptName)
        {
            if (!this.luaScriptSha1Map.ContainsKey(scriptName))
            {
                throw new NotImplementedException($"{scriptName} has not been registered in redis");
            }
            return this.luaScriptSha1Map[scriptName];
        }

        /// <summary>
        /// load from the redis version db's meta data hashset
        /// </summary>
        private void LoadLuaScriptMapFromRedis()
        {
            using (RedisClient redisClient = (RedisClient) this.RedisManager.GetClient())
            {
                redisClient.ChangeDb(RedisVersionDb.META_DB_INDEX);

                string hashId = RedisVersionDb.META_SCRIPT_KEY;
                byte[][] valueBytes = redisClient.HGetAll(hashId);

                if (valueBytes == null)
                {
                    return;
                }

                for (int i = 0; i < valueBytes.Length; i += 2)
                {
                    string scriptName = Encoding.ASCII.GetString(valueBytes[i]);
                    string sha1 = Encoding.ASCII.GetString(valueBytes[i+1]);
                    this.luaScriptSha1Map[scriptName] = sha1;
                }
            }
        }


        /// <summary>
        /// This will be dropped in the future since we assume redis already has those scripts when it's connected.
        /// Client registration will be not supported
        /// </summary>
        /// <param name="scriptKey"></param>
        /// <param name="luaBody"></param>
        /// <returns></returns>
        private bool RegisterLuaScripts(string scriptKey, string luaBody)
        {
            using (RedisClient redisClient = (RedisClient) this.RedisManager.GetClient())
            {
                redisClient.ChangeDb(RedisVersionDb.META_DB_INDEX);

                // Extract the command sha1
                byte[] scriptKeyBytes = Encoding.UTF8.GetBytes(scriptKey);
                byte[] sha1Bytes = redisClient.HGet(RedisVersionDb.META_SCRIPT_KEY, scriptKeyBytes);

                // We will register the lua script only if the sha1 isn't in script table or sha1 is not in the redis cache 
                bool hasRegistered = false;
                if (sha1Bytes != null)
                {
                    byte[][] returnBytes = redisClient.ScriptExists(new byte[][] { sha1Bytes });
                    if (returnBytes != null)
                    {
                        // The return value == 1 means scripts have been registered
                        hasRegistered = BitConverter.ToInt64(returnBytes[0], 0) == 1;
                    }
                }

                // Register the lua scripts when it has not been registered
                if (!hasRegistered)
                {
                    // register and load the script
                    byte[] scriptSha1Bytes = redisClient.ScriptLoad(luaBody);
                    if (scriptSha1Bytes == null)
                    {
                        return false;
                    }
                    // insert into the script hashset
                    long result = redisClient.HSet(RedisVersionDb.META_SCRIPT_KEY, scriptKeyBytes, scriptSha1Bytes);
                    return result == 1;
                }
                return true;
            }
        }

        ////////////////////////////////////////////////////////////////
        /// GET_SET_COMMIT_TIME
        ////////////////////////////////////////////////////////////////
        /*
         *  -- eval lua_script 1 txId try_commit_time -1
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
         */

        ////////////////////////////////////////////////////////////////
        /// UPDATE_COMMIT_LOWER_BOUND
        ////////////////////////////////////////////////////////////////
        /*
         * -- eval lua_script 1 txId commit_time -1 -2
            local commit_time = ARGV[1]
            local negative_one = ARGV[2]
            local negative_two = ARGV[3]

            local data = redis.call('HMGET', KEYS[1], 'commit_time', 'commit_lower_bound')
            if not data then
                return negative_two
            end

            local tx_commit_time = data[1]
            local commit_lower_bound = data[2]

            if tx_commit_time == negative_one and 
                string.byte(commit_lower_bound) < string.byte(commit_time) then
                local ret = redis.call('HSET', KEYS[1], 'commit_lower_bound', commit_time)
                if ret ~= 0 then
                    return negative_two
                end
            end

            return tx_commit_time
        */

        ////////////////////////////////////////////////////////////////
        /// UPDATE_VERSION_MAX_COMMIT_TS
        ////////////////////////////////////////////////////////////////
        /*
         * -- eval lua 1 record_key, version_key commit_time -1
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
        */

        ////////////////////////////////////////////////////////////////
        /// REPLACE_VERSION_ENTRY_TXID
        ////////////////////////////////////////////////////////////////
        /*
         * -- eval lua 1 record_key version_key txId -1
            local entry = redis.call('HGET', KEYS[1], ARGV[1])
            if not entry then
                return ARGV[3]
            end

            local tx_id = string.sub(entry, 2*8+1, 3*8)
            local max_commit_ts = string.sub(entry, 3*8+1, 4*8)

            if tx_id == ARGV[3] then
                local new_version_entry = string.sub(entry, 1, 2*8) .. ARGV[2] .. string.sub(entry, 3*8+1, string.len(entry))
                local ret = redis.call('HSET', KEYS[1], ARGV[1], new_version_entry);
                if ret == nil then
                    return ARGV[3]
                end
                return max_commit_ts
            else
                return ARGV[3]
            end
         */

        ////////////////////////////////////////////////////////////////
        /// REPLACE_PAYLOAD
        ////////////////////////////////////////////////////////////////
        /*
         * -- eval lua 1 record_key version_key beginTimestamp endTimestamp -1 0
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
         */
    }
}
