namespace GraphView.Transaction
{
    using ServiceStack.Redis;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Reflection;
    using System.Text;

    /// <summary>
    /// A redis lua scripts manager to load sha1 from redis and get sha1 by script name
    /// 
    /// To get sha1 by name easily, we maintain a hashset in Redis meta database (with index 0)
    /// We store the map from script name to sha1 in a hashset in meta db with the name RedisVersionDb.META_SCRIPT_KEY 
    /// 
    /// When the manager is inititlized, it will check and register lua scripts, after that it loads
    /// the sha1 map from redis and has a interface to query sha1 by name
    /// </summary>
    public class RedisLuaScriptManager
    {
        /// <summary>
        /// A transient map from script name to sha1, it will be filled when the instance is created
        /// </summary>
        private readonly Dictionary<LuaScriptName, string> luaScriptSha1Map = new Dictionary<LuaScriptName, string>();

        private string[] readWriteHosts;

        private long metaDataDbIndex;

        internal RedisLuaScriptManager(string[] readWriteHosts, long metaDataDbIndex)
        {
            if (readWriteHosts == null || readWriteHosts.Length == 0)
            {
                Console.WriteLine("readWriteHosts mustn't be null");
            }

            this.readWriteHosts = readWriteHosts;
            this.metaDataDbIndex = metaDataDbIndex;

            // check and register 
            this.CheckAndLoadLuaScripts();
            this.LoadLuaScriptMapFromRedis();
        }

        /// <summary>
        /// Get a sha1 by the script name
        /// </summary>
        /// <param name="scriptName">The human readiable name for sha1, like INSERT_VERSION</param>
        /// <returns>script's sha1<returns>
        internal string GetLuaScriptSha1(LuaScriptName scriptName)
        {
            if (!this.luaScriptSha1Map.ContainsKey(scriptName))
            {
                throw new ArgumentException($"{LuaToString(scriptName)} has not been registered in redis");
            }
            return this.luaScriptSha1Map[scriptName];
        }

        /// <summary>
        /// load from the redis version db's meta data hashset
        /// </summary>
        private void LoadLuaScriptMapFromRedis()
        {
            using (RedisClient redisClient = CreateRedisClient(readWriteHosts[0], metaDataDbIndex))
            {
                string hashId = RedisVersionDb.META_SCRIPT_KEY;
                byte[][] valueBytes = redisClient.HGetAll(hashId);

                if (valueBytes == null || valueBytes.Length == 0)
                {
                    return;
                }

                for (int i = 0; i < valueBytes.Length; i += 2)
                {
                    string scriptName = Encoding.ASCII.GetString(valueBytes[i]);
                    string sha1 = Encoding.ASCII.GetString(valueBytes[i + 1]);
                    this.luaScriptSha1Map[LuaFromString(scriptName)] = sha1;
                }
            }
        }

        /// <summary>
        /// Check if every lua script havs been registered. If not, register them.
        /// </summary>
        private void CheckAndLoadLuaScripts()
        {
            var luaScriptNames =
                (LuaScriptName[])Enum.GetValues(typeof(LuaScriptName));

            string[] luaBodies = new string[luaScriptNames.Length];
            for (int i = 0; i < luaScriptNames.Length; i++)
            {
                string scriptName = LuaToString(luaScriptNames[i]);
                string resourceName = $"GraphView.Resources.RedisLua.{scriptName}.lua";
                Stream stream = Assembly.GetExecutingAssembly().GetManifestResourceStream(resourceName);

                using (StreamReader reader = new StreamReader(stream))
                {
                    string luaBody = reader.ReadToEnd();
                    luaBodies[i] = luaBody;
                }
            }

            foreach (string host in this.readWriteHosts)
            {
                for (int i = 0; i < luaScriptNames.Length; i++)
                {
                    this.RegisterLuaScripts(
                        host, LuaToString(luaScriptNames[i]), luaBodies[i]);
                }
            }
        }
        /// <summary>
        /// Regsiter the lua script, it includes two steps:
        /// 1. call "SCRIPT LOAD" command to load scripts to redis cache
        /// 2. store the map scriptName => sha1 in metaDb (redis index 0) 
        /// </summary>
        /// <param name="scriptKey">The human readable script name</param>
        /// <param name="luaBody">the lua script body</param>
        /// <returns></returns>
        private bool RegisterLuaScripts(string host, string scriptKey, string luaBody)
        {
            using (RedisClient redisClient = this.CreateRedisClient(host, this.metaDataDbIndex))
            {
                // Extract the command sha1
                byte[] scriptKeyBytes = Encoding.UTF8.GetBytes(scriptKey);
                byte[] sha1Bytes = redisClient.HGet(RedisVersionDb.META_SCRIPT_KEY, scriptKeyBytes);

                // We will register the lua script only if the sha1 isn't in script table or sha1 is not in the redis cache 
                bool hasRegistered = false;
                if (sha1Bytes != null)
                {
                    byte[][] returnBytes = redisClient.ScriptExists(new byte[][] { sha1Bytes });
                    if (returnBytes != null && returnBytes.Length != 0)
                    {
                        // The return value == "1" means scripts have been registered
                        // SCRIPT EXISTS will return an array of string
                        hasRegistered = Encoding.ASCII.GetString(returnBytes[0]) == "1";
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
                    return true;
                }
                return true;
            }
        }

        private RedisClient CreateRedisClient(string metaDataConnStr, long metaDataDbIndex)
        {
            string[] hostPort = metaDataConnStr.Split(':');
            string host = hostPort[0];
            int port = int.Parse(hostPort[1]);

            // create and change to the meta data db
            BasicRedisClientManager clientManager = new BasicRedisClientManager((int)metaDataDbIndex, new string[] { metaDataConnStr });
            RedisClient redisClient = (RedisClient)clientManager.GetClient();

            return redisClient;
        }
        static private LuaScriptName LuaFromString(String s)
        {
            return (LuaScriptName)Enum.Parse(typeof(LuaScriptName), s);
        }
        static private String LuaToString(LuaScriptName l)
        {
            return Enum.GetName(typeof(LuaScriptName), l);
        }
    }

    internal enum LuaScriptName
    {
        REMOVE_KEYS_WITH_PREFIX,
        SET_AND_GET_COMMIT_TIME,
        REPLACE_VERSION_ENTRY,
        UPDATE_COMMIT_LOWER_BOUND,
        UPDATE_VERSION_MAX_COMMIT_TS,
        DELETE_DIRTY_VERSION,
        GET_VERSION_LIST,
        UPLOAD_NEW_VERSION,
    }
}
