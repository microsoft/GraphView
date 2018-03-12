namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using ServiceStack.Redis;
    using RecordRuntime;
    using Newtonsoft.Json.Linq;
    using System.Text;

    internal partial class RedisVersionTable : VersionTable
    {
        /// <summary>
        /// The redis database index of the current table
        /// </summary>
        private readonly long redisDbIndex;

        /// <summary>
        /// Get redisClient from the client pool
        /// </summary>
        private IRedisClientsManager RedisManager
        {
            get
            {
                return RedisClientManager.Instance;
            }
        }

        public RedisVersionTable(string tableId, long redisDbIndex)
            : base(tableId)
        {
            this.redisDbIndex = redisDbIndex;
        }

        internal override IEnumerable<VersionEntry> GetVersionList(object recordKey)
        {
            using (RedisClient redisClient = (RedisClient) this.RedisManager.GetClient())
            {
                redisClient.ChangeDb(this.redisDbIndex);

                string hashKey = recordKey as string;
                byte[][] versionBytes = redisClient.HGetAll(hashKey);

                if (versionBytes == null)
                {
                    throw new ArgumentException("Invalid recordKey reference '{recordKey}'");
                }

                List<VersionEntry> versionList = new List<VersionEntry>();
                foreach (byte[] bytes in versionBytes)
                {
                    VersionEntry versionEntry = VersionEntrySerializer.DeserializeFromBytes(bytes);
                    versionList.Add(versionEntry);
                }
                return versionList;
            }
        }

        // This method should be overriden for Redis
        internal override VersionEntry GetVersionEntryByKey(object recordKey, long versionKey)
        {
            using (RedisClient redisClient = (RedisClient) this.RedisManager.GetClient())
            {
                redisClient.ChangeDb(this.redisDbIndex);

                string hashKey = recordKey as string;
                byte[] key = BitConverter.GetBytes(versionKey);

                byte[] valueBytes = redisClient.HGet(hashKey, key);
                if (valueBytes == null)
                {
                    return null;
                }

                return VersionEntrySerializer.DeserializeFromBytes(valueBytes);
            }
        }

        internal override bool InsertAndUploadVersion(object recordKey, VersionEntry version)
        {
            using (RedisClient redisClient = (RedisClient) this.RedisManager.GetClient())
            {
                redisClient.ChangeDb(this.redisDbIndex);

                string hashKey = recordKey as string;
                byte[] key = BitConverter.GetBytes(version.VersionKey);
                byte[] value = VersionEntrySerializer.SerializeToBytes(version);

                // result == 1, insert a new version key
                // result == 0, update an existed version key
                long result = redisClient.HSet(hashKey, key, value);
                return result == 1;
            }
        }

        internal override bool UpdateAndUploadVersion(object recordKey, long versionKey, VersionEntry oldVersion, VersionEntry newVersion)
        {
            using (RedisClient redisClient = (RedisClient) this.RedisManager.GetClient())
            {
                // change to the meta db
                redisClient.ChangeDb(RedisVersionDb.META_DB_INDEX);

                byte[] scriptKeyBytes = Encoding.ASCII.GetBytes("HSET_CAS");
                byte[] scriptSha1Bytes = redisClient.HGet(RedisVersionDb.META_SCRIPT_KEY, scriptKeyBytes);
                // Can not get the script sha from meta table
                if (scriptSha1Bytes == null)
                {
                    return false;
                }

                // change to the current db
                redisClient.ChangeDb(this.redisDbIndex);

                string hashKeyStr = recordKey as string;
                byte[] hashKey = Encoding.ASCII.GetBytes(hashKeyStr);
                byte[] field = BitConverter.GetBytes(versionKey);
                byte[] newValue = VersionEntrySerializer.SerializeToBytes(newVersion);
                byte[] oldValue = VersionEntrySerializer.SerializeToBytes(oldVersion);
                
                string scriptSha1 = Encoding.ASCII.GetString(scriptSha1Bytes);
                byte[][] keysAndArgs = new byte[][] { hashKey, field, oldValue, newValue };

                long result = redisClient.EvalShaInt(scriptSha1, 1, keysAndArgs);
                // CAS succeeds with return as 0
                return result == 0;
            } 
        }

        internal override bool DeleteVersionEntry(object recordKey, long versionKey)
        {
            using (RedisClient redisClient = (RedisClient) this.RedisManager.GetClient())
            {
                redisClient.ChangeDb(this.redisDbIndex);

                string hashKey = recordKey as string;
                byte[] key = BitConverter.GetBytes(versionKey);

                long result = redisClient.HDel(hashKey, key);
                return result == 1;
            }
        }

        /// <summary>
        /// Implement a HGET cas operation by EVAL commmand in redis
        /// The redis will compare its own value with oldValue, if they are same, it will set to newValue.
        /// Otherwise, it will return false
        /// </summary>
        /// <param name="hashKey">The hashset key in redis</param>
        /// <param name="field">The field name in redis</param>
        /// <param name="oldValue">The value will be checked </param>
        /// <param name="newValue">The value will be updated </param>
        /// <returns>
        /// 0: comparsion succeeds and updated an existed version
        /// 1: comparsion succeeds and inserted a new version
        /// 2: comparsion fails
        /// 3: other errors, which means the command runs error
        /// </returns>
        private long RedisHSetCAS(byte[] hashKey, byte[] field, byte[] oldValue, byte[] newValue)
        {
            using (RedisClient redisClient = (RedisClient) this.RedisManager.GetClient())
            {
                redisClient.ChangeDb(this.redisDbIndex);
                /*
                 -- eval 'lua_code' 1 hashkey field oldValue newValue
                 local ver = redis.call('HGET', KEYS[1], ARGV[1]);
                 if not ver or ver == ARGV[2] then
                     return redis.call('HSET', KEYS[1], ARGV[1], ARGV[3]);
                 end
                 return 2
                */
                string luaBody = @"local ver = redis.call('HGET', KEYS[1], ARGV[1]); if not ver or ver == ARGV[2]
                then return redis.call('HSET', KEYS[1], ARGV[1], ARGV[3]); end return 2";
                byte[][] keysAndArgs = new byte[][] { hashKey, field, oldValue, newValue };

                byte[][] resultBytes = redisClient.Eval(luaBody, 1, keysAndArgs);
                if (resultBytes == null)
                {
                    return 3;
                }
                return BitConverter.ToInt64(resultBytes[0], 0);
            }
        }
    }

    /// <summary>
    /// The implemetation of IVersionTableStore part
    /// </summary>
    internal partial class RedisVersionTable
    { 
        public override bool DeleteJson(object key, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public override JObject GetJson(object key, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public override IList<JObject> GetRangeJsons(object lowerKey, object upperKey, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public override IList<object> GetRangeRecordKeyList(object lowerValue, object upperValue, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public override IList<object> GetRecordKeyList(object value, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public override bool InsertJson(object key, JObject record, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public override bool UpdateJson(object key, JObject record, Transaction tx)
        {
            throw new NotImplementedException();
        }
    }
}
