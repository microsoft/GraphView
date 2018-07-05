namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using ServiceStack.Redis;
    using System.Text;
    using ServiceStack.Redis.Pipeline;

    internal partial class RedisVersionTable : VersionTable
    {
        /// <summary>
        /// The redis database index of the current table
        /// </summary>
        private readonly long redisDbIndex;

        /// <summary>
        /// The response visitor to handle response
        /// </summary>
        private readonly RedisResponseVisitor responseVisitor;

        /// <summary>
        /// Get redisClient from the client pool
        /// </summary>
        private RedisClientManager RedisManager
        {
            get
            {
                return ((RedisVersionDb)this.VersionDb).RedisManager;
            }
        }

        /// <summary>
        /// The manager for lua scripts
        /// </summary>
        private RedisLuaScriptManager LuaManager
        {
            get
            {
                return ((RedisVersionDb)this.VersionDb).RedisLuaManager;
            }
        }

        public RedisVersionTable(VersionDb versionDb, string tableId, long redisDbIndex)
            : base(versionDb, tableId, versionDb.PartitionCount)
        {
            this.redisDbIndex = redisDbIndex;
            this.responseVisitor = new RedisResponseVisitor();

            for (int pid = 0; pid < this.PartitionCount; pid++)
            {
                RedisConnectionPool clientPool = this.RedisManager.GetClientPool(
                    this.redisDbIndex, pid);
                this.tableVisitors[pid] = new RedisVersionTableVisitor(clientPool);
            }
        }

        internal override void EnqueueVersionEntryRequest(VersionEntryRequest req, int execPartition = 0)
        {
            base.EnqueueVersionEntryRequest(req, execPartition);
        }

        internal override void Clear()
        {
            for (int pid = 0; pid < this.PartitionCount; pid++)
            {
                using (RedisClient redisClient = this.RedisManager.GetClient(this.redisDbIndex, pid))
                {
                    redisClient.FlushDb();
                }
            }
        }

        internal override void MockLoadData(int recordCount)
        {
            int pk = 0;
            while (pk < this.PartitionCount)
            {
                Console.WriteLine("Loading Partition {0}", pk);
                using (RedisClient redisClient = this.RedisManager.GetClient(this.redisDbIndex, pk))
                {
                    int partitions = this.PartitionCount;
                    for (int i = pk; i < recordCount; i += partitions)
                    {
                        object recordKey = i;
                        string hashId = recordKey.ToString();

                        VersionEntry emptyEntry = new VersionEntry();
                        VersionEntry.InitEmptyVersionEntry(i, emptyEntry);
                        byte[] key = BitConverter.GetBytes(-1L);
                        byte[] value = VersionEntry.Serialize(emptyEntry);

                        redisClient.HSet(hashId, key, value);

                        VersionEntry versionEntry = new VersionEntry();
                        VersionEntry.InitFirstVersionEntry(i, versionEntry.Record == null ? new String('a', 100) : versionEntry.Record, versionEntry);

                        key = BitConverter.GetBytes(0L);
                        value = VersionEntry.Serialize(emptyEntry);
                        redisClient.HSet(hashId, key, value);
                    }
                }
                pk++;
            }
        }

        internal override void AddPartition(int partitionCount)
        {
            int prePartitionCount = this.PartitionCount;
            base.AddPartition(partitionCount);
            this.PartitionCount = partitionCount;

            Array.Resize(ref this.tableVisitors, partitionCount);
            for (int pk = 0; pk < this.PartitionCount; pk++)
            {
                RedisConnectionPool clientPool = this.RedisManager.GetClientPool(
                    this.redisDbIndex, pk);
                this.tableVisitors[pk] = new RedisVersionTableVisitor(clientPool);
            }

            // Reshuffle Data
            List<Tuple<byte[], byte[][]>>[] reshuffledRecords = new List<Tuple<byte[], byte[][]>>[partitionCount];
            for (int pk = 0; pk < prePartitionCount; pk++)
            {
                using (RedisClient redisClient = this.RedisManager.GetClient(this.redisDbIndex, pk))
                {
                    byte[][] keys = redisClient.Keys("*");
                    foreach (byte[] key in keys)
                    {
                        byte[][] values = redisClient.HGetAll(Encoding.ASCII.GetString(key));
                        object recordKey = BytesSerializer.Deserialize(key);
                        int npk = this.VersionDb.PhysicalPartitionByKey(recordKey);

                        reshuffledRecords[npk].Add(Tuple.Create(key, values));
                    }

                    redisClient.FlushDb();
                }
            }

            for (int pk = 0; pk < partitionCount; pk++)
            {
                List<Tuple<byte[], byte[][]>> records = reshuffledRecords[pk];
                using (RedisClient redisClient = this.RedisManager.GetClient(this.redisDbIndex, pk))
                {
                    foreach (Tuple<byte[], byte[][]> versions in records)
                    {
                        string hashId = BytesSerializer.Deserialize(versions.Item1).ToString();
                        byte[][] keys = new byte[versions.Item2.Length/2][];
                        byte[][] values = new byte[versions.Item2.Length / 2][];

                        for (int i = 0; 2 * i < versions.Item2.Length; i++)
                        {
                            keys[i/2] = versions.Item2[i];
                            values[i/2] = versions.Item2[i+1];
                        }
                        
                        redisClient.HMSet(hashId, keys, values);
                    }
                }
            }
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

            int partition = this.VersionDb.PhysicalPartitionByKey(hashId);
            if (((RedisVersionDb)this.VersionDb).PipelineMode)
            {
                RedisRequest request = new RedisRequest(hashId, RedisRequestType.HGetAll);
                RedisConnectionPool clientPool = this.RedisManager.GetClientPool(this.redisDbIndex, partition);
                returnBytes = clientPool.ProcessValuesRequest(request);
            }
            else
            {
                using (RedisClient client = this.RedisManager.GetClient(this.redisDbIndex, partition))
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
            VersionEntry emptyEntry = VersionEntry.InitEmptyVersionEntry(recordKey);

            byte[] keyBytes = BitConverter.GetBytes(versionKey);
            byte[] valueBytes = VersionEntry.Serialize(emptyEntry);
            long ret = 0;

            int partition = this.VersionDb.PhysicalPartitionByKey(hashId);
            if (((RedisVersionDb)this.VersionDb).PipelineMode)
            {
                RedisRequest request = new RedisRequest(hashId, keyBytes, valueBytes, RedisRequestType.HSetNX);
                RedisConnectionPool clientPool = this.RedisManager.GetClientPool(this.redisDbIndex, partition);
                ret = clientPool.ProcessLongRequest(request);
            }
            else
            {
                using (RedisClient client = this.RedisManager.GetClient(this.redisDbIndex, partition))
                {
                    ret = client.HSetNX(hashId, keyBytes, valueBytes);
                }
            }

            if (ret == 1)
            {
                return null;
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
            int partition = this.VersionDb.PhysicalPartitionByKey(hashId);
            if (((RedisVersionDb)this.VersionDb).PipelineMode)
            {
                RedisRequest request = new RedisRequest(keysAndArgs, sha1, 1, RedisRequestType.EvalSha);
                RedisConnectionPool clientPool = this.RedisManager.GetClientPool(this.redisDbIndex, partition);
                returnBytes = clientPool.ProcessValuesRequest(request);
            }
            else
            {
                using (RedisClient client = this.RedisManager.GetClient(this.redisDbIndex, partition))
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
        /// Replace the whole version entry by the HSET command
        /// </summary>
        /// <param name="recordKey"></param>
        /// <param name="versionKey"></param>
        /// <param name="versionEntry"></param>
        /// <returns>True or False</returns>
        internal override bool ReplaceWholeVersionEntry(object recordKey, long versionKey, VersionEntry versionEntry)
        {
            string hashId = recordKey as string;
            byte[] keyBytes = BitConverter.GetBytes(versionKey);
            byte[] valueBytes = VersionEntry.Serialize(versionEntry);

            long ret = 0;
            int partition = this.VersionDb.PhysicalPartitionByKey(recordKey);
            if (((RedisVersionDb)this.VersionDb).PipelineMode)
            {
                RedisRequest request = new RedisRequest(hashId, keyBytes, valueBytes, RedisRequestType.HSet);
                RedisConnectionPool clientPool = this.RedisManager.GetClientPool(this.redisDbIndex, partition);
                ret = clientPool.ProcessLongRequest(request);
            }
            else
            {
                using (RedisClient client = this.RedisManager.GetClient(this.redisDbIndex, partition))
                {
                    ret = client.HSet(hashId, keyBytes, valueBytes);
                }
            }
            return ret == 1;
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
            byte[] valueBytes = VersionEntry.Serialize(versionEntry);
            long ret = 0;

            int partition = this.VersionDb.PhysicalPartitionByKey(hashId);
            if (((RedisVersionDb)this.VersionDb).PipelineMode)
            {
                RedisRequest request = new RedisRequest(hashId, keyBytes, valueBytes, RedisRequestType.HSetNX);
                RedisConnectionPool clientPool = this.RedisManager.GetClientPool(this.redisDbIndex, partition);
                ret = clientPool.ProcessLongRequest(request);
            }
            else
            {
                using (RedisClient client = this.RedisManager.GetClient(this.redisDbIndex, partition))
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

            int partition = this.VersionDb.PhysicalPartitionByKey(hashId);
            if (((RedisVersionDb)this.VersionDb).PipelineMode)
            {
                RedisRequest request = new RedisRequest(keysAndArgs, sha1, 1, RedisRequestType.EvalSha);
                RedisConnectionPool clientPool = this.RedisManager.GetClientPool(this.redisDbIndex, partition);
                returnBytes = clientPool.ProcessValuesRequest(request);
            }
            else
            {
                using (RedisClient client = this.RedisManager.GetClient(this.redisDbIndex, partition))
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

            int partition = this.VersionDb.PhysicalPartitionByKey(hashId);
            if (((RedisVersionDb)this.VersionDb).PipelineMode)
            {
                RedisRequest request = new RedisRequest(hashId, keyBytes, RedisRequestType.HDel);
                RedisConnectionPool clientPool = this.RedisManager.GetClientPool(this.redisDbIndex, partition);
                ret = clientPool.ProcessLongRequest(request);
            }
            else
            {
                using (RedisClient client = this.RedisManager.GetClient(this.redisDbIndex, partition))
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

            int partition = this.VersionDb.PhysicalPartitionByKey(hashId);
            if (((RedisVersionDb)this.VersionDb).PipelineMode)
            {
                RedisRequest request = new RedisRequest(hashId, keyBytes, RedisRequestType.HGet);
                RedisConnectionPool clientPool = this.RedisManager.GetClientPool(this.redisDbIndex, partition);
                valueBytes = clientPool.ProcessValueRequest(request);
            }
            else
            {
                using (RedisClient client = this.RedisManager.GetClient(this.redisDbIndex, partition))
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

        /// <summary>
        /// Get entries by a batch of key with PIPELINE command in redis
        /// </summary>
        /// <param name="batch"></param>
        /// <returns></returns>
        internal override IDictionary<VersionPrimaryKey, VersionEntry> GetVersionEntryByKey(IEnumerable<VersionPrimaryKey> batch)
        {
            Dictionary<int, List<VersionPrimaryKey>> partitionBatch = new Dictionary<int, List<VersionPrimaryKey>>();
            foreach (VersionPrimaryKey key in batch)
            {
                int partition = this.VersionDb.PhysicalPartitionByKey(key);
                if (!partitionBatch.ContainsKey(partition))
                {
                    partitionBatch[partition] = new List<VersionPrimaryKey>();
                }
                partitionBatch[partition].Add(key);
            }

            Dictionary<VersionPrimaryKey, VersionEntry> versionEntries = new Dictionary<VersionPrimaryKey, VersionEntry>();
            foreach (int partition in partitionBatch.Keys)
            {
                IDictionary<VersionPrimaryKey, VersionEntry> partitionEntries = 
                    this.GetVersionEntryInPartition(partitionBatch[partition]);
                
                foreach (KeyValuePair<VersionPrimaryKey, VersionEntry> kv in partitionEntries)
                {
                    versionEntries.Add(kv.Key, kv.Value);
                }
            }
            return versionEntries;
        }

        /// <summary>
        /// All the record key and version keys in the batch belong to the same partition
        /// </summary>
        /// <param name="batch"></param>
        /// <returns></returns>
        private IDictionary<VersionPrimaryKey, VersionEntry> GetVersionEntryInPartition(List<VersionPrimaryKey> batch)
        {
            Dictionary<VersionPrimaryKey, VersionEntry> versionEntries = new Dictionary<VersionPrimaryKey, VersionEntry>();
            if (batch == null || batch.Count == 0)
            {
                return versionEntries;
            }

            // Append those requests into the list
            List<RedisRequest> reqList = new List<RedisRequest>();
            foreach (VersionPrimaryKey key in batch)
            {
                string hashId = key.RecordKey as string;
                byte[] keyBytes = BitConverter.GetBytes(key.VersionKey);
                RedisRequest req = new RedisRequest(hashId, keyBytes, RedisRequestType.HGet);
                reqList.Add(req);
            }

            int partition = this.VersionDb.PhysicalPartitionByKey(batch[0]);
            // In the pipeline mode, push all requests to the global queue
            byte[][] valuesBytes = null;
            if (((RedisVersionDb)this.VersionDb).PipelineMode)
            {
                RedisConnectionPool clientPool = this.RedisManager.GetClientPool(this.redisDbIndex, partition);
                valuesBytes = clientPool.ProcessValueRequestInBatch(reqList);
            }
            // In the non-pipeline mode, push all requests to the local queue
            else
            {
                using (RedisClient redisClient = this.RedisManager.GetClient(this.redisDbIndex, partition))
                {
                    using (IRedisPipeline pipe = redisClient.CreatePipeline())
                    {
                        foreach (RedisRequest req in reqList)
                        {
                            pipe.QueueCommand(
                             r => ((RedisNativeClient)r).HGet(req.HashId, req.Key),
                             req.SetValue, req.SetError);
                        }
                        pipe.Flush();
                    }
                }
                
                // extract the result bytes
                valuesBytes = new byte[reqList.Count][];
                int index = 0;
                foreach (RedisRequest req in reqList)
                {
                    valuesBytes[index++] = (byte[])req.Result;
                }
            }

            for (int i = 0; i < reqList.Count; i++)
            {
                object recordKey = reqList[i].HashId;
                long versionKey = BitConverter.ToInt64(reqList[i].Key, 0);
                VersionEntry entry = VersionEntry.Deserialize(recordKey, versionKey, valuesBytes[i]);
                versionEntries.Add(new VersionPrimaryKey(recordKey, versionKey), entry);
            }
            return versionEntries;
        }
    }
}
