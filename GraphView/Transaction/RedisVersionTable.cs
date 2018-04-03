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
        /// <param name="recordKey"></param>
        /// <param name="versionKey"></param>
        /// <param name="versionEntry"></param>
        /// <returns>A list of version entries, maybe an empty list</returns>
        internal override IEnumerable<VersionEntry> GetVersionList(object recordKey)
        {
            using (RedisClient redisClient = (RedisClient)this.RedisManager.GetClient())
            {
                List<VersionEntry> entries = new List<VersionEntry>();

                redisClient.ChangeDb(this.redisDbIndex);
                string hashId = recordKey as string;

                // return format is [key1bytes, value1bytes, ....]
                byte[][] returnBytes = redisClient.HGetAll(hashId);
                if (returnBytes == null || returnBytes.Length == 0)
                {
                    return entries;
                }

                for (int i = 0; i < returnBytes.Length; i += 2)
                {
                    long versionKey = BitConverter.ToInt64(returnBytes[i], 0);
                    VersionEntry entry = VersionEntry.Deserialize(recordKey, versionKey, returnBytes[i + 1]);
                    entries.Add(entry);
                }

                return entries;
            }
        }

        /// <summary>
        /// Get a version entry from redis with record key and version key by HGET command
        /// </summary>
        /// <param name="recordKey"></param>
        /// <param name="versionKey"></param>
        /// <returns>A version entry or null if the specified version entry doesn't exist</returns>
        internal override VersionEntry GetVersionEntryByKey(object recordKey, long versionKey)
        {
            using (RedisClient redisClient = (RedisClient)this.RedisManager.GetClient())
            {
                List<VersionEntry> entries = new List<VersionEntry>();

                redisClient.ChangeDb(this.redisDbIndex);
                string hashId = recordKey as string;
                byte[] fieldBytes = BitConverter.GetBytes(versionKey);

                byte[] returnBytes = redisClient.HGet(hashId, fieldBytes);
                if (returnBytes == null || returnBytes.Length == 0)
                {
                    return null;
                }

                return VersionEntry.Deserialize(recordKey, versionKey, returnBytes);
            }
        }

        /// <summary>
        /// Read the the most recent version entry, if the list of recordKey is empty,
        /// initialize the list with an emtpy version entry
        /// 
        /// Initialization is implemented by HSETNX command
        /// </summary>
        /// <param name="recordKey"></param>
        /// <param name="largestVersionKey"></param>
        /// <returns>The most recent commited version entry</returns>
        internal override IEnumerable<VersionEntry> InitializeAndGetVersionList(object recordKey)
        {
            using (RedisClient redisClient = (RedisClient) this.RedisManager.GetClient())
            {
                redisClient.ChangeDb(this.redisDbIndex);

                string hashId = recordKey as string;
                long versionKey = 0L;
                VersionEntry emptyEntry = new VersionEntry(recordKey, versionKey, "empty", -1);

                byte[] keyBytes = BitConverter.GetBytes(versionKey);
                byte[] valueBytes = VersionEntry.Serialize(emptyEntry.BeginTimestamp, emptyEntry.EndTimestamp,
                    emptyEntry.TxId, emptyEntry.MaxCommitTs, emptyEntry.Record);

                // Initialize the list with HSETNX command
                long ret = redisClient.HSetNX(hashId, keyBytes, valueBytes);

                if (ret == 1)
                {
                    return new List<VersionEntry>(new VersionEntry[] {emptyEntry});
                }
                return this.GetVersionList(recordKey);
            }
        }

        /// <summary>
        /// Replace the txId in version entry by a lua CAS script REPLACE_VERSION_ENTRY
        /// </summary>
        /// <param name="recordKey"></param>
        /// <param name="versionKey"></param>
        /// <param name="txId"></param>
        /// <returns>Version's maxCommitTs if success, -1 otherwise</returns>
        internal override long ReplaceVersionEntry(object recordKey, long versionKey, long beginTimestamp, long endTimestamp, long txId, long readTxId)
        {
            using (RedisClient redisClient = (RedisClient)this.RedisManager.GetClient())
            {
                redisClient.ChangeDb(this.redisDbIndex);

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
                    RedisVersionDb.NEGATIVE_ONE_BYTES,
                };

                try
                {
                    byte[][] returnBytes = redisClient.EvalSha(sha1, 1, keysAndArgs);
                    if (returnBytes == null || returnBytes.Length == 0)
                    {
                        return -1;
                    }

                    // The first byte array in return bytes is always null if no errors
                    return BitConverter.ToInt64(returnBytes[1], 0);
                }
                catch (RedisResponseException e)
                {
                    return -1;
                }
            }
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
            using (RedisClient redisClient = (RedisClient)this.RedisManager.GetClient())
            {
                List<VersionEntry> entries = new List<VersionEntry>();

                redisClient.ChangeDb(this.redisDbIndex);
                string hashId = recordKey as string;

                byte[] hashIdBytes = Encoding.ASCII.GetBytes(hashId);
                byte[] fieldBytes = BitConverter.GetBytes(versionKey);
                byte[] valueBytes = VersionEntry.Serialize(versionEntry.BeginTimestamp, versionEntry.EndTimestamp,
                    versionEntry.TxId, versionEntry.MaxCommitTs, versionEntry.Record);

                long ret = redisClient.HSetNX(hashId, fieldBytes, valueBytes);
                return ret == 1;
            }
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
            using (RedisClient redisClient = (RedisClient)this.RedisManager.GetClient())
            {
                redisClient.ChangeDb(this.redisDbIndex);

                string sha1 = this.LuaManager.GetLuaScriptSha1("UPDATE_VERSION_MAX_COMMIT_TS");
                string hashId = recordKey as string;

                byte[][] keysAndArgs =
                {
                    Encoding.ASCII.GetBytes(hashId),
                    BitConverter.GetBytes(versionKey),
                    BitConverter.GetBytes(commitTime),
                    RedisVersionDb.NEGATIVE_ONE_BYTES,
                };

                try
                {
                    byte[][] returnBytes = redisClient.EvalSha(sha1, 1, keysAndArgs);
                    if (returnBytes == null || returnBytes.Length < 2)
                    {
                        return null;
                    }

                    // return format is [keybytes, valuebytes]
                    return VersionEntry.Deserialize(recordKey, versionKey, returnBytes[1]);
                }
                catch (RedisResponseException e)
                {
                    return null;
                }
            }
        }

        /// <summary>
        /// Delete a version entry by record key and version key by HDEL command
        /// </summary>
        /// <param name="recordKey"></param>
        /// <param name="versionKey"></param>
        /// <returns>True if it's successful, false otherwise</returns>
        internal override bool DeleteVersionEntry(object recordKey, long versionKey)
        {
            using (RedisClient redisClient = (RedisClient)this.RedisManager.GetClient())
            {
                List<VersionEntry> entries = new List<VersionEntry>();

                redisClient.ChangeDb(this.redisDbIndex);
                string hashId = recordKey as string;

                byte[] fieldBytes = BitConverter.GetBytes(versionKey);
                long ret = redisClient.HDel(hashId, fieldBytes);
                return ret == 1;
            }
        }
    }
}
