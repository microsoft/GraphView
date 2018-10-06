namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using ServiceStack.Redis;
    using System.Text;
    using ServiceStack.Redis.Pipeline;
    using System.Threading;

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
        private RedisClientManager RedisManager { get; set; }

        /// <summary>
        /// The manager for lua scripts
        /// </summary>
        private RedisLuaScriptManager LuaManager { get; set; }

        /// <summary>
        /// The singleton connection pool from redisVerionDb.
        /// </summary>
        private RedisConnectionPool singletonConnPool;

        /// <summary>
        /// The redisVersionDb Instance
        /// </summary>
        private RedisVersionDb redisVersionDb;

        /// <summary>
        /// The mode of redisVersionDb
        /// </summary>
        private RedisVersionDbMode redisVersionDbMode;

        /// <summary>
        /// Request queues for logical partitions of a version table
        /// </summary>
        protected Queue<VersionEntryRequest>[] requestQueues;

        /// <summary>
        /// A queue of version entry requests for each partition to be flushed to the k-v store
        /// </summary>
        protected Queue<VersionEntryRequest>[] flushQueues;

        /// <summary>
        /// The latches to sync flush queues and request Queues
        /// </summary>
        protected int[] queueLatches;

        public RedisVersionTable(VersionDb versionDb, string tableId, long redisDbIndex)
            : base(versionDb, tableId, versionDb.PartitionCount)
        {
            this.redisDbIndex = redisDbIndex;
            this.responseVisitor = new RedisResponseVisitor();
            this.redisVersionDb = ((RedisVersionDb)this.VersionDb);
            this.RedisManager = redisVersionDb.RedisManager;
            this.LuaManager = redisVersionDb.RedisLuaManager;
            this.singletonConnPool = redisVersionDb.SingletonConnPool;
            this.redisVersionDbMode = redisVersionDb.Mode;

            this.requestQueues = new Queue<VersionEntryRequest>[this.PartitionCount];
            this.flushQueues = new Queue<VersionEntryRequest>[this.PartitionCount];
            this.queueLatches = new int[this.PartitionCount];

            RedisConnectionPool clientPool = null;
            for (int pid = 0; pid < this.PartitionCount; pid++)
            {
                if (this.redisVersionDbMode == RedisVersionDbMode.Cluster)
                {
                    clientPool = this.singletonConnPool;
                }
                else
                {
                    clientPool = this.RedisManager.GetClientPool(
                        this.redisDbIndex, RedisVersionDb.GetRedisInstanceIndex(pid));
                }
                this.tableVisitors[pid] = new RedisVersionTableVisitor(
                    clientPool, this.LuaManager, this.responseVisitor, this.redisVersionDbMode);

                this.requestQueues[pid] = new Queue<VersionEntryRequest>(VersionDb.REQUEST_QUEUE_CAPACITY);
                this.flushQueues[pid] = new Queue<VersionEntryRequest>(VersionDb.REQUEST_QUEUE_CAPACITY);
                this.queueLatches[pid] = 0;
            }
        }

        internal override void EnqueueVersionEntryRequest(VersionEntryRequest req, int srcPartition = 0)
        {
            if (this.redisVersionDbMode == RedisVersionDbMode.Cluster)
            {
                this.requestQueues[srcPartition].Enqueue(req);
            }
            else
            {
                int pk = this.VersionDb.PhysicalPartitionByKey(req.RecordKey);

                while (Interlocked.CompareExchange(ref queueLatches[pk], 1, 0) != 0) ;
                Queue<VersionEntryRequest> reqQueue = Volatile.Read(ref this.requestQueues[pk]);
                reqQueue.Enqueue(req);
                Interlocked.Exchange(ref queueLatches[pk], 0);
            }
        }

        /// <summary>
        /// Move pending requests of a version table partition to the partition's flush queue. 
        /// </summary>
        /// <param name="pk">The key of the version table partition to flush</param>
        private void DequeueVersionEntryRequests(int pk)
        {
            if (this.redisVersionDbMode == RedisVersionDbMode.Cluster)
            {
                Queue<VersionEntryRequest> freeQueue = this.flushQueues[pk];
                this.flushQueues[pk] = this.requestQueues[pk];
                this.requestQueues[pk] = freeQueue;
            }
            else
            {
                // Check whether the queue is empty at first
                if (this.requestQueues[pk].Count > 0)
                {
                    while (Interlocked.CompareExchange(ref queueLatches[pk], 1, 0) != 0) ;

                    Queue<VersionEntryRequest> freeQueue = Volatile.Read(ref this.flushQueues[pk]);
                    Volatile.Write(ref this.flushQueues[pk], Volatile.Read(ref this.requestQueues[pk]));
                    Volatile.Write(ref this.requestQueues[pk], freeQueue);

                    Interlocked.Exchange(ref queueLatches[pk], 0);
                }
            }
        }

        internal override void Visit(int partitionKey)
        {
            this.DequeueVersionEntryRequests(partitionKey);
            Queue<VersionEntryRequest> flushQueue = this.flushQueues[partitionKey];

            if (flushQueue.Count == 0)
            {
                return;
            }

            VersionTableVisitor visitor = this.tableVisitors[partitionKey];
            visitor.Invoke(flushQueue);
            flushQueue.Clear();
        }

        internal override void Clear()
        {
            if (this.redisVersionDbMode == RedisVersionDbMode.Cluster)
            {
                // IMPORTMENT: Since the Redis Cluster doesn't allow multi-key commands across multiple hash slots
                // So we couldn't clear keys in batch
                //using (RedisClient redisClient = this.singletonConnPool.GetRedisClient())
                //{
                //    byte[][] keysAndArgs =
                //    {
                //        Encoding.ASCII.GetBytes(RedisVersionDb.VER_KEY_PREFIX),
                //    };
                //    string sha1 = this.LuaManager.GetLuaScriptSha1(LuaScriptName.REMOVE_KEYS_WITH_PREFIX);
                //    redisClient.EvalSha(sha1, 0, keysAndArgs);
                //}

                int batchSize = 100;
                using (RedisClient redisClient = this.singletonConnPool.GetRedisClient())
                {
                    byte[][] keys = redisClient.Keys(RedisVersionDb.VER_KEY_PREFIX+"*");
                    if (keys != null)
                    {
                        for (int i = 0; i < keys.Length; i += batchSize)
                        {
                            int upperBound = Math.Min(keys.Length, i + batchSize);
                            using (IRedisPipeline pipe = redisClient.CreatePipeline())
                            {
                                for (int j = i; j < upperBound; j++)
                                {
                                    string keyStr = Encoding.ASCII.GetString(keys[j]);
                                    pipe.QueueCommand(r => ((RedisNativeClient)r).Del(keyStr));
                                }
                                pipe.Flush();
                            }    
                        }
                    }
                }
            }
            else
            {
                for (int pid = 0; pid < this.PartitionCount; pid++)
                {
                    using (RedisClient redisClient = this.RedisManager.GetClient(
                        this.redisDbIndex, RedisVersionDb.GetRedisInstanceIndex(pid)))
                    {
                        redisClient.FlushDb();
                    }
                }
            }
        }

        internal override void MockLoadData(int recordCount)
        {
            int pk = 0;
            RedisConnectionPool connPool = null;
            if (this.redisVersionDbMode == RedisVersionDbMode.Cluster)
            {
                connPool = this.singletonConnPool;
            }

            int loaded = 0;
            while (pk < this.PartitionCount)
            {
                Console.WriteLine("Loading Partition {0}", pk);
                if (connPool == null)
                {
                    connPool = this.RedisManager.GetClientPool(this.redisDbIndex, RedisVersionDb.GetRedisInstanceIndex(pk));
                }

                using (RedisClient redisClient = connPool.GetRedisClient())
                {
                    int batchSize = 100;
                    int partitions = this.PartitionCount;

                    for (int i = pk; i < recordCount; i += partitions * batchSize)
                    {
                        int upperBound = Math.Min(recordCount, i + partitions * batchSize);
                        using (IRedisPipeline pipe = redisClient.CreatePipeline())
                        {
                            for (int j = i; j < upperBound; j += partitions)
                            {
                                object recordKey = j;
                                string hashId = recordKey.ToString();
                                if (this.redisVersionDbMode == RedisVersionDbMode.Cluster)
                                {
                                    hashId = RedisVersionDb.PACK_KEY(RedisVersionDb.VER_KEY_PREFIX, hashId);
                                }

                                VersionEntry versionEntry = new VersionEntry();
                                VersionEntry.InitFirstVersionEntry(new String('a', 100), versionEntry);

                                byte[] key = BitConverter.GetBytes(VersionEntry.VERSION_KEY_START_INDEX + 1);
                                byte[] value = VersionEntry.Serialize(versionEntry);

                                pipe.QueueCommand(r => ((RedisNativeClient)r).HSet(hashId, key, value));
                                pipe.QueueCommand(r => ((RedisNativeClient)r).HSet(hashId, RedisVersionDb.LATEST_VERSION_PTR_FIELD, key));

                                loaded++;
                            }
                            pipe.Flush();
                        }
                    }
                }
                pk++;

                if (this.redisVersionDbMode != RedisVersionDbMode.Cluster)
                {
                    connPool = null;
                }
            }

            Console.WriteLine("Loaded {0} records Successfully", loaded);
        }

        internal override void AddPartition(int partitionCount)
        {
            int prePartitionCount = this.PartitionCount;

            RedisConnectionPool clientPool = null;
            if (this.redisVersionDbMode == RedisVersionDbMode.Cluster)
            {
                clientPool = this.singletonConnPool;
            }

            Array.Resize(ref this.tableVisitors, partitionCount);
            Array.Resize(ref this.requestQueues, partitionCount);
            Array.Resize(ref this.flushQueues, partitionCount);
            Array.Resize(ref this.queueLatches, partitionCount);

            for (int pk = prePartitionCount; pk < partitionCount; pk++)
            {
                if (clientPool == null)
                {
                    clientPool = this.RedisManager.GetClientPool(
                        this.redisDbIndex, RedisVersionDb.GetRedisInstanceIndex(pk));
                }
                this.tableVisitors[pk] = new RedisVersionTableVisitor(
                    clientPool, this.LuaManager, this.responseVisitor, this.redisVersionDbMode);

                this.requestQueues[pk] = new Queue<VersionEntryRequest>(VersionDb.REQUEST_QUEUE_CAPACITY);
                this.flushQueues[pk] = new Queue<VersionEntryRequest>(VersionDb.REQUEST_QUEUE_CAPACITY);
                this.queueLatches[pk] = 0;
            }

            base.AddPartition(partitionCount);
        }

        /// <summary>
        /// Reshuffle records in the version db, which will recalculate the partition of record keys,
        /// and put them to right partitions under the new number of partition
        /// </summary>
        /// <param name="perPartitionCount">The number of partitions before adding partitions</param>
        /// <param name="partitionCount">The current number of partitions</param>
        private void ReshuffleRecords(int prePartitionCount, int partitionCount)
        {
            List<Tuple<byte[], byte[][]>>[] reshuffledRecords = new List<Tuple<byte[], byte[][]>>[partitionCount];
            for (int npk = 0; npk < partitionCount; npk++)
            {
                reshuffledRecords[npk] = new List<Tuple<byte[], byte[][]>>();
            }

            RedisConnectionPool connPool = null;
            if (this.redisVersionDbMode == RedisVersionDbMode.Cluster)
            {
                connPool = this.singletonConnPool;
            }

            for (int pk = 0; pk < prePartitionCount; pk++)
            {
                if (connPool == null)
                {
                    connPool = this.RedisManager.GetClientPool(this.redisDbIndex, RedisVersionDb.GetRedisInstanceIndex(pk));
                }
                using (RedisClient redisClient = connPool.GetRedisClient())
                {
                    byte[][] keys = redisClient.Keys("*");
                    foreach (byte[] key in keys)
                    {
                        byte[][] values = redisClient.HGetAll(Encoding.ASCII.GetString(key));

                        string recordKey = Encoding.ASCII.GetString(key);
                        // TODO: Only For Benchmark Test as the recordKey is an integer
                        int intRecordKey = int.Parse(recordKey);

                        int npk = this.VersionDb.PhysicalPartitionByKey(intRecordKey);
                        reshuffledRecords[npk].Add(Tuple.Create(key, values));
                    }

                    redisClient.FlushDb();
                }
            }

            // flush the redis db
            for (int pk = 0; pk < partitionCount; pk++)
            {
                using (RedisClient redisClient = this.RedisManager.GetClient(
                   this.redisDbIndex, RedisVersionDb.GetRedisInstanceIndex(pk)))
                {
                    redisClient.FlushDb();
                }
            }
            Console.WriteLine("Reshuffled Records into Memory");

            for (int pk = 0; pk < partitionCount; pk++)
            {
                Console.WriteLine("Reshuffled Partition {0}", pk);
                List<Tuple<byte[], byte[][]>> records = reshuffledRecords[pk];
                using (RedisClient redisClient = this.RedisManager.GetClient(
                    this.redisDbIndex, RedisVersionDb.GetRedisInstanceIndex(pk)))
                {
                    foreach (Tuple<byte[], byte[][]> versions in records)
                    {
                        string hashId = Encoding.ASCII.GetString(versions.Item1);
                        byte[][] keys = new byte[versions.Item2.Length / 2][];
                        byte[][] values = new byte[versions.Item2.Length / 2][];

                        for (int i = 0; 2 * i < versions.Item2.Length; i++)
                        {
                            keys[i] = versions.Item2[2 * i];
                            values[i] = versions.Item2[2 * i + 1];
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
                VersionEntry entry = VersionEntry.Deserialize(versionKey, returnBytes[i + 1]);
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
            long versionKey = VersionEntry.VERSION_KEY_START_INDEX;
            VersionEntry emptyEntry = VersionEntry.InitEmptyVersionEntry();

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
            string sha1 = this.LuaManager.GetLuaScriptSha1(LuaScriptName.REPLACE_VERSION_ENTRY);
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
            return VersionEntry.Deserialize(versionKey, returnBytes[1]);
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
            string sha1 = this.LuaManager.GetLuaScriptSha1(LuaScriptName.UPDATE_VERSION_MAX_COMMIT_TS);
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
            return VersionEntry.Deserialize(versionKey, returnBytes[1]);
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
            return VersionEntry.Deserialize(versionKey, valueBytes);
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
                VersionEntry entry = VersionEntry.Deserialize(versionKey, valuesBytes[i]);
                versionEntries.Add(new VersionPrimaryKey(recordKey, versionKey), entry);
            }
            return versionEntries;
        }
    }
}
