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
         *  -- eval lua_script 1 txId try_commit_time
            local data = redis.call("HMGET", KEYS[1], "commit_time", "commit_lower_bound")
            if not data then
                return -2
            end

            local try_commit_time = tonumber(ARGV[1])
            local commit_time = data[1]
            local commit_lower_bound = data[2]

            if tonumber(commit_time) == -1 and 
                tonumber(commit_lower_bound) <= try_commit_time then
    
                local ret = redis.call("HSET", KEYS[1], "commit_time", try_commit_time)
                if ret == 0 then
                    return try_commit_time
                end
                return -2
            end
            return -1
         */

        ////////////////////////////////////////////////////////////////
        /// UPDATE_COMMIT_LOWER_BOUND
        ////////////////////////////////////////////////////////////////
        /*
         * -- eval lua_script 1 txId commit_time
            local data = redis.call('HMGET', KEYS[1], 'commit_time', 'commit_lower_bound')
            if not data then
                return -2
            end

            local commit_time = tonumber(ARGV[1])
            local tx_commit_time = data[1]
            local commit_lower_bound = data[2]

            if tonumber(tx_commit_time) == -1 and 
                tonumber(commit_lower_bound) < commit_time then
                local ret = redis.call('HSET', KEYS[1], 'commit_lower_bound', commit_time)
                if ret ~= 0 then
                    return -2
                end
            end

            return tx_commit_time
        */
    }
}
