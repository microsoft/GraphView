namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using ServiceStack.Redis;
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
        private RedisClientManager RedisManager
        {
            get
            {
                return RedisClientManager.Instance;
            }
        }

        /// <summary>
        /// The manager for lua scripts
        /// </summary>
        private RedisLuaScriptManager LuaManager
        {
            get
            {
                return RedisLuaScriptManager.Instance;
            }
        }

        public RedisVersionTable(string tableId, long redisDbIndex)
            : base(tableId)
        {
            this.redisDbIndex = redisDbIndex;
        }
    }

    internal partial class RedisVersionTable
    {
        /// <summary>
        /// Get all version entries by the command HGETALL
        /// MIND: HGETALL in ServiceStack.Redis only supports a string type as the hashId 
        /// If we want to take other types as the hashId, must override the HGETALL with lua
        /// </summary>
        /// <returns>A list of version entries, maybe an empty list</returns>
        internal override IEnumerable<VersionEntry> GetVersionList(object recordKey)
        {
            List<VersionEntry> entries = new List<VersionEntry>();
            string hashId = recordKey as string;
            byte[][] returnBytes = null;

            if (RedisVersionDb.Instance.PipelineMode)
            {
                RedisRequest request = new RedisRequest(hashId, RedisRequestType.HGetAll);
                RedisConnectionPool clientPool = this.RedisManager.GetClientPool(this.redisDbIndex, hashId);
                returnBytes = clientPool.ProcessValuesRequest(request);
            }
            else
            {
                using (RedisClient client = this.RedisManager.GetClient(this.redisDbIndex, hashId))
                {
                    returnBytes = client.HGetAll(hashId);
                }
            }
            
            if (returnBytes == null || returnBytes.Length == 0)
            {
                return entries;
            }
            // return format is [key1bytes, value1bytes, ....]
            for (int i = 0; i < returnBytes.Length; i += 2)
            {
                long versionKey = BitConverter.ToInt64(returnBytes[i], 0);
                VersionEntry entry = VersionEntry.Deserialize(recordKey, versionKey, returnBytes[i + 1]);
                entries.Add(entry);
            }

            return entries;
        }

        /// <summary>
        /// Read the the most recent version entry, if the list of recordKey is empty,
        /// initialize the list with an emtpy version entry
        /// 
        /// Initialization is implemented by HSETNX command
        /// </summary>
        /// <param name="recordKey"></param>
        /// <returns>The most recent commited version entry</returns>
        internal override IEnumerable<VersionEntry> InitializeAndGetVersionList(object recordKey)
        {
            string hashId = recordKey as string;
            long versionKey = VersionEntry.VERSION_KEY_STRAT_INDEX;
            VersionEntry emptyEntry = new VersionEntry(recordKey, versionKey,
                VersionEntry.EMPTY_RECORD, VersionEntry.EMPTY_TXID);

            byte[] keyBytes = BitConverter.GetBytes(versionKey);
            byte[] valueBytes = VersionEntry.Serialize(emptyEntry.BeginTimestamp, emptyEntry.EndTimestamp,
                emptyEntry.TxId, emptyEntry.MaxCommitTs, emptyEntry.Record);
            long ret = 0;

            if (RedisVersionDb.Instance.PipelineMode)
            {
                RedisRequest request = new RedisRequest(hashId, keyBytes, valueBytes, RedisRequestType.HSetNX);
                RedisConnectionPool clientPool = this.RedisManager.GetClientPool(this.redisDbIndex, hashId);
                ret = clientPool.ProcessLongRequest(request);
            }
            else
            {
                using (RedisClient client = this.RedisManager.GetClient(this.redisDbIndex, hashId))
                {
                    ret = client.HSetNX(hashId, keyBytes, valueBytes);
                }
            }

            if (ret == 1)
            {
                return new List<VersionEntry>(new VersionEntry[] { emptyEntry });
            }
            return this.GetVersionList(recordKey);
        }

        /// <summary>
        /// Replace the txId in version entry by a lua CAS script REPLACE_VERSION_ENTRY
        /// </summary>
        /// <param name="recordKey"></param>
        /// <param name="versionKey"></param>
        /// <param name="txId"></param>
        /// <returns>Version's maxCommitTs if success, -1 otherwise</returns>
        internal override VersionEntry ReplaceVersionEntry(object recordKey, long versionKey, 
            long beginTimestamp, long endTimestamp, long txId, long readTxId, long expectedEndTimestamp)
        {
            string sha1 = this.LuaManager.GetLuaScriptSha1("REPLACE_VERSION_ENTRY");
            string hashId = recordKey as string;

            byte[][] keysAndArgs =
            {
                Encoding.ASCII.GetBytes(hashId),
                BitConverter.GetBytes(versionKey),
                BitConverter.GetBytes(beginTimestamp),
                BitConverter.GetBytes(endTimestamp),
                BitConverter.GetBytes(txId),
                BitConverter.GetBytes(readTxId),
                BitConverter.GetBytes(expectedEndTimestamp),
                RedisVersionDb.NEGATIVE_ONE_BYTES,
            };

            byte[][] returnBytes = null;
            if (RedisVersionDb.Instance.PipelineMode)
            {
                RedisRequest request = new RedisRequest(keysAndArgs, sha1, 1, RedisRequestType.EvalSha);
                RedisConnectionPool clientPool = this.RedisManager.GetClientPool(this.redisDbIndex, hashId);
                returnBytes = clientPool.ProcessValuesRequest(request);
            }
            else
            {
                using (RedisClient client = this.RedisManager.GetClient(this.redisDbIndex, hashId))
                {
                    returnBytes = client.EvalSha(sha1, 1, keysAndArgs);
                }
            }

            // Maybe return [null, null]
            if (returnBytes == null || returnBytes.Length == 0 || returnBytes[1] == null)
            {
                return null;
            }
            return VersionEntry.Deserialize(recordKey, versionKey, returnBytes[1]);
        }

        /// <summary>
        /// Upload a whole version entry to the redis by HSETNX command
        /// MIND: HSETNX in ServiceStack.Redis only supports a string type as the hashId 
        /// If we want to take other types as the hashId, must override the HSETNX with lua
        ///
        /// </summary>
        /// <param name="recordKey"></param>
        /// <param name="versionKey"></param>
        /// <param name="versionEntry"></param>
        /// <returns>True if the uploading is successful, False otherwise</returns>
        internal override bool UploadNewVersionEntry(object recordKey, long versionKey, VersionEntry versionEntry)
        {
            List<VersionEntry> entries = new List<VersionEntry>();

            string hashId = recordKey as string;
            byte[] keyBytes = BitConverter.GetBytes(versionKey);
            byte[] valueBytes = VersionEntry.Serialize(versionEntry.BeginTimestamp, versionEntry.EndTimestamp,
                versionEntry.TxId, versionEntry.MaxCommitTs, versionEntry.Record);
            long ret = 0;

            if (RedisVersionDb.Instance.PipelineMode)
            {
                RedisRequest request = new RedisRequest(hashId, keyBytes, valueBytes, RedisRequestType.HSetNX);
                RedisConnectionPool clientPool = this.RedisManager.GetClientPool(this.redisDbIndex, hashId);
                ret = clientPool.ProcessLongRequest(request);
            }
            else
            {
                using (RedisClient client = this.RedisManager.GetClient(this.redisDbIndex, hashId))
                {
                    ret = client.HSetNX(hashId, keyBytes, valueBytes);
                }
            }
            return ret == 1;
        }


        /// <summary>
        /// Update a version's maxCommitTs if the current version is not updating by others
        /// By a lua CAS script UPDATE_VERSION_MAX_COMMIT_TS
        /// </summary>
        /// TODO: if it has the same txId
        /// 
        /// <param name="recordKey"></param>
        /// <param name="versionKey"></param>
        /// <param name="commitTime">The current transaction's commit time</param>
        /// <returns>If the update is successful, return the updated version entry.
        ///          Return the current version entry if it failed to update
        /// </returns>
        internal override VersionEntry UpdateVersionMaxCommitTs(object recordKey, long versionKey, long commitTime)
        {
            string sha1 = this.LuaManager.GetLuaScriptSha1("UPDATE_VERSION_MAX_COMMIT_TS");
            string hashId = recordKey as string;

            byte[][] keysAndArgs =
            {
                Encoding.ASCII.GetBytes(hashId),
                BitConverter.GetBytes(versionKey),
                BitConverter.GetBytes(commitTime),
                RedisVersionDb.NEGATIVE_ONE_BYTES,
            };
            byte[][] returnBytes = null;

            if (RedisVersionDb.Instance.PipelineMode)
            {
                RedisRequest request = new RedisRequest(keysAndArgs, sha1, 1, RedisRequestType.EvalSha);
                RedisConnectionPool clientPool = this.RedisManager.GetClientPool(this.redisDbIndex, hashId);
                returnBytes = clientPool.ProcessValuesRequest(request);
            }
            else
            {
                using (RedisClient client = this.RedisManager.GetClient(this.redisDbIndex, hashId))
                {
                    returnBytes = client.EvalSha(sha1, 1, keysAndArgs);
                }
            }

            if (returnBytes == null || returnBytes.Length < 2)
            {
                return null;
            }
            // return format is [keybytes, valuebytes]
            return VersionEntry.Deserialize(recordKey, versionKey, returnBytes[1]);
        }

        /// <summary>
        /// Delete a version entry by record key and version key by HDEL command
        /// </summary>
        /// <param name="recordKey"></param>
        /// <param name="versionKey"></param>
        /// <returns>True if it's successful, false otherwise</returns>
        internal override bool DeleteVersionEntry(object recordKey, long versionKey)
        {
            string hashId = recordKey as string;
            byte[] keyBytes = BitConverter.GetBytes(versionKey);
            long ret = 0;

            if (RedisVersionDb.Instance.PipelineMode)
            {
                RedisRequest request = new RedisRequest(hashId, keyBytes, RedisRequestType.HDel);
                RedisConnectionPool clientPool = this.RedisManager.GetClientPool(this.redisDbIndex, hashId);
                ret = clientPool.ProcessLongRequest(request);
            }
            else
            {
                using (RedisClient client = this.RedisManager.GetClient(this.redisDbIndex, hashId))
                {
                    ret = client.HDel(hashId, keyBytes);
                }
            }
            return ret == 1;
        }

        /// <summary>
        /// Get a version entry by key with the HGET command
        /// </summary>
        /// <returns>A version entry or null</returns>
        internal override VersionEntry GetVersionEntryByKey(object recordKey, long versionKey)
        {
            string hashId = recordKey as string;
            byte[] keyBytes = BitConverter.GetBytes(versionKey);
            byte[] valueBytes = null;

            if (RedisVersionDb.Instance.PipelineMode)
            {
                RedisRequest request = new RedisRequest(hashId, keyBytes, RedisRequestType.HGet);
                RedisConnectionPool clientPool = this.RedisManager.GetClientPool(this.redisDbIndex, hashId);
                valueBytes = clientPool.ProcessValueRequest(request);
            }
            else
            {
                using (RedisClient client = this.RedisManager.GetClient(this.redisDbIndex, hashId))
                {
                    valueBytes = client.HGet(hashId, keyBytes);
                }
            }

            if (valueBytes == null)
            {
                return null;
            }
            return VersionEntry.Deserialize(recordKey, versionKey, valueBytes);
        }

        //internal IDictionary<object, VersionEntry> GetVersionEntryByKey(IEnumerable<Tuple<object, long>> batch)
        //{
        //    Dictionary<object, VersionEntry> versionDict = new Dictionary<object, VersionEntry>();
        //    Dictionary<Tuple<string, int, long>, Tuple<Tuple<RedisClient, Tuple<object, long>>> 
        //    using (RedisClient redisClient = this.RedisManager.GetClient(0, 1))
        //    {
        //        redisClient.Db;
        //    }
        //}
    }
}
