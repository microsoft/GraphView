namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using NonBlocking;
    //using System.Collections.Concurrent;

    /// <summary>
    /// A version table implementation in single machine environment.
    /// Use a Dictionary.
    /// </summary>
    internal partial class SingletonDictionaryVersionTable : VersionTable
    {
        private readonly ConcurrentDictionary<object, ConcurrentDictionary<long, VersionEntry>> dict;

        public static readonly long TAIL_KEY = -1L;

        private static readonly int RECORD_CAPACITY = 1000000;

        public static readonly int VERSION_CAPACITY = 4;

        internal int PartitionCount { get; set; }

        public SingletonDictionaryVersionTable(VersionDb versionDb, string tableId,
            int partitionCount, List<TxResourceManager> txResourceManagers)
            : base(versionDb, tableId, partitionCount)
        {
            int maxConcurrency = Math.Max(1, this.VersionDb.PartitionCount / 2);
            this.dict = new ConcurrentDictionary<object, ConcurrentDictionary<long, VersionEntry>>(
                maxConcurrency, SingletonDictionaryVersionTable.RECORD_CAPACITY);

            this.PartitionCount = partitionCount;

            for (int i = 0; i < partitionCount; i++)
            {
                this.tableVisitors[i] = new SingletonVersionTableVisitor(this.dict);
            }
        }

        internal override void EnqueueVersionEntryRequest(VersionEntryRequest req, int execPartition = 0)
        {
            // Interlocked.Increment(ref VersionDb.EnqueuedRequests);
            this.tableVisitors[execPartition].Invoke(req);
        }

        internal override bool DeleteVersionEntry(object recordKey, long versionKey)
        {
            ConcurrentDictionary<long, VersionEntry> versionList = null;
            if (!this.dict.TryGetValue(recordKey, out versionList))
            {
                return true;
            }

            VersionEntry versionEntry = null;
            return versionList.TryRemove(versionKey, out versionEntry);
        }

        internal override VersionEntry GetVersionEntryByKey(object recordKey, long versionKey)
        {
            ConcurrentDictionary<long, VersionEntry> versionList = null;
            if (!this.dict.TryGetValue(recordKey, out versionList))
            {
                return null;
            }

            VersionEntry versionEntry = null;
            if (!versionList.TryGetValue(versionKey, out versionEntry))
            {
                return null;
            }

            return versionEntry;
        }

        internal override void AddPartition(int partitionCount)
        {
            int prePartitionCount = this.PartitionCount;
            base.AddPartition(partitionCount);

            Array.Resize(ref this.tableVisitors, partitionCount);
            for (int pk = prePartitionCount; pk < partitionCount; pk++)
            {
                this.tableVisitors[pk] = new SingletonVersionTableVisitor(this.dict);
            }

            this.PartitionCount = partitionCount;
        }

        internal override IEnumerable<VersionEntry> InitializeAndGetVersionList(object recordKey)
        {
            ConcurrentDictionary<long, VersionEntry> versionList = null;
            if (!this.dict.TryGetValue(recordKey, out versionList))
            {
                ConcurrentDictionary<long, VersionEntry> newVersionList =
                    new ConcurrentDictionary<long, VersionEntry>(1, SingletonDictionaryVersionTable.VERSION_CAPACITY);
                // Adds a special entry whose key is -1 when the list is initialized.
                // The entry uses beginTimestamp as a pointer pointing to the newest verion in the list.
                newVersionList.TryAdd(SingletonDictionaryVersionTable.TAIL_KEY, new VersionEntry(recordKey, -1, -1, -1, null, -1, -1));

                if (this.dict.TryAdd(recordKey, newVersionList))
                {
                    // The version list is newly created by this tx. 
                    // No meaningful versions exist, except for the artificial entry as a tail pointer. 
                    return null;
                }
            }

            // Retrieves the tail pointer. 
            VersionEntry tailEntry = null;
            if (!versionList.TryGetValue(SingletonDictionaryVersionTable.TAIL_KEY, out tailEntry))
            {
                throw new TransactionException("The tail pointer is missing from the version list.");
            }
            long lastVersionKey = Interlocked.Read(ref tailEntry.VersionKey);

            List<VersionEntry> localList = new List<VersionEntry>(2);

            // Only returns top 2 newest versions. This is enough for serializability. 
            // For other isolation levels, more versions may need to be returned.
            // When old versions may be truncated, it is desirable to maintain a head pointer as well,
            // so as to increase the lower bound of version keys and reduce the number of iterations. 
            while (lastVersionKey >= 0 && localList.Count <= 2)
            {
                VersionEntry verEntry = null;
                if (versionList.TryGetValue(lastVersionKey, out verEntry))
                {
                    localList.Add(verEntry);
                    if (verEntry.TxId == VersionEntry.EMPTY_TXID)
                    {
                        break;
                    }
                }

                lastVersionKey--;
            }

            return localList;
        }

        internal override VersionEntry ReplaceVersionEntry(
            object recordKey,
            long versionKey,
            long beginTimestamp,
            long endTimestamp,
            long txId,
            long readTxId,
            long expectedEndTimestamp)
        {
            ConcurrentDictionary<long, VersionEntry> versionList = null;
            if (!this.dict.TryGetValue(recordKey, out versionList))
            {
                throw new TransactionException("The specified record does not exist.");
            }

            VersionEntry entry = null;
            if (!versionList.TryGetValue(versionKey, out entry))
            {
                throw new TransactionException("The specified version does not exist.");
            }

            if (entry.TxId == readTxId && entry.EndTimestamp == expectedEndTimestamp)
            {
                VersionEntry newEntry = new VersionEntry(
                    recordKey, versionKey, beginTimestamp, endTimestamp, entry.Record, txId, entry.MaxCommitTs);

                if (versionList.TryUpdate(versionKey, newEntry, entry))
                {
                    // Successfully replaces the version. Returns the new version entry.
                    return newEntry;
                }
                else
                {
                    // The version entry has been updated since the prior retrieval,  
                    // causing the replacement failed. Re-read to get a new image. 
                    versionList.TryGetValue(versionKey, out entry);
                }
            }

            return entry;
        }

        internal override bool UploadNewVersionEntry(object recordKey, long versionKey, VersionEntry versionEntry)
        {
            ConcurrentDictionary<long, VersionEntry> versionList = null;
            if (!this.dict.TryGetValue(recordKey, out versionList))
            {
                throw new TransactionException("The specified record does not exist.");
            }

            if (versionList.TryAdd(versionKey, versionEntry))
            {
                // The new version has been inserted successfully. Re-directs the tail pointer to the new version.  

                VersionEntry tailEntry = null;
                versionList.TryGetValue(SingletonDictionaryVersionTable.TAIL_KEY, out tailEntry);

                if (tailEntry == null)
                {
                    throw new TransactionException("Tail pointer is missing from the version list.");
                }

                long tailKey = tailEntry.VersionKey;
                while (tailKey < versionKey)
                {
                    // Here we use Interlocked to atomically update the tail entry, instead of ConcurrentDict.TryUpdate().
                    // This is because once created, the whole tail entry always stays and is never replaced.
                    // All concurrent tx's only access the tail pointer, i.e., the beginTimestamp field.  
                    Interlocked.CompareExchange(ref tailEntry.VersionKey, versionKey, tailKey);
                    tailKey = tailEntry.VersionKey;
                }

                return true;
            }
            else
            {
                // The same version key has been added before or by a concurrent tx. 
                // The new version cannot be inserted.
                return false;
            }
        }

        internal override VersionEntry UpdateVersionMaxCommitTs(object recordKey, long versionKey, long commitTs)
        {
            ConcurrentDictionary<long, VersionEntry> versionList = null;
            if (!this.dict.TryGetValue(recordKey, out versionList))
            {
                throw new TransactionException("The specified record does not exist.");
            }

            VersionEntry verEntry = null;
            if (!versionList.TryGetValue(versionKey, out verEntry))
            {
                throw new TransactionException("The specified version does not exist.");
            }

            while (verEntry.MaxCommitTs < commitTs)
            {
                VersionEntry newEntry = new VersionEntry(recordKey, versionKey,
                    verEntry.BeginTimestamp, verEntry.EndTimestamp, verEntry.Record, verEntry.TxId, commitTs);

                if (versionList.TryUpdate(versionKey, newEntry, verEntry))
                {
                    verEntry = newEntry;
                    break;
                }
                else
                {
                    versionList.TryGetValue(versionKey, out verEntry);
                }
            }

            return verEntry;
        }

        internal override IEnumerable<VersionEntry> GetVersionList(object recordKey)
        {
            ConcurrentDictionary<long, VersionEntry> versionList = null;
            if (!this.dict.TryGetValue(recordKey, out versionList))
            {
                return null;
            }

            // The value at -1 in the version list is a special entry, 
            // whose beginTimestamp points to the newest version. 
            VersionEntry tailEntry = null;
            versionList.TryGetValue(SingletonDictionaryVersionTable.TAIL_KEY, out tailEntry);
            long lastVersionKey = Interlocked.Read(ref tailEntry.VersionKey);

            List<VersionEntry> localList = new List<VersionEntry>(2);

            // Only returns top 2 newest versions. This is enough for serializability. 
            // For other isolation levels, more versions may need to be returned.
            // When old versions may be truncated, it is desirable to maintain a head pointer as well,
            // so as to increase the lower bound of version keys and reduce the number of iterations. 
            while (lastVersionKey >= 0 && localList.Count <= 2)
            {
                VersionEntry verEntry = null;
                if (versionList.TryGetValue(lastVersionKey, out verEntry))
                {
                    localList.Add(verEntry);
                    if (verEntry.TxId == VersionEntry.EMPTY_TXID)
                    {
                        break;
                    }
                }

                lastVersionKey--;
            }

            return localList;
        }

        internal override void Clear()
        {
            //int recordCount = this.dict.Count;
            //int prePartitionCount = this.VersionDb.PartitionCount;
            //int recordsPerPartition = recordCount / prePartitionCount;

            this.dict.Clear();

            // fill the dict to avoid resizing
            //int comingRecordCount = recordsPerPartition * (prePartitionCount + 1);
            //for (int i = 0; i < comingRecordCount; i++)
            //{
            //    this.dict.TryAdd(i, TransactionExecutor.dummyVersionEntryArray[i]);
            //}
        }

        internal override void MockLoadData(Tuple<int, int>[] partitionRange)
        {
            int prePartitionCount = this.VersionDb.PartitionCount - 1;
            PartitionByKeyDelegate prePartitionByKey = key => Math.Abs(key.GetHashCode()) % prePartitionCount;

            int recycleVersionEntry = 0;
            int pk = 0;
            // Load Data
            if (this.VersionDb.PartitionCount > 1)
            {
                return;
            }

            foreach (Tuple<int, int> range in partitionRange)
            {
                Console.WriteLine("Loading Partition {0}", pk);
                int startKey = range.Item1;
                int endKey = range.Item2;

                for (int i = startKey; i < endKey; i += this.VersionDb.PartitionCount)
                {
                    object recordKey = i;
                    ConcurrentDictionary<long, VersionEntry> versionList = null;
                    if (!this.dict.TryGetValue(recordKey, out versionList))
                    {
                        versionList = new ConcurrentDictionary<long, VersionEntry>();
                        this.dict.TryAdd(recordKey, versionList);
                    }

                    VersionEntry emptyEntry = versionList.ContainsKey(-1L) ? versionList[-1L] : new VersionEntry();
                    VersionEntry versionEntry = versionList.ContainsKey(0L) ? versionList[0L] : new VersionEntry();
                    // only recycle version entries when 
                    //if (this.VersionDb.PartitionCount > 1)
                    //{
                    //    int ppk = prePartitionByKey(recordKey);
                    //    // Clear the list and insert version entries
                    //    foreach (KeyValuePair<long, VersionEntry> kv in versionList)
                    //    {
                    //        if (kv.Key == -1L || kv.Key == 0L)
                    //        {
                    //            continue;
                    //        }

                    //        VersionEntry usedVersion = kv.Value;
                    //        recycleVersionEntry++;
                    //        this.VersionDb.txResourceManagers[ppk].RecycleVersionEntry(ref usedVersion);
                    //    }
                    //}

                    versionList.Clear();

                    VersionEntry.InitEmptyVersionEntry(-1, emptyEntry);
                    emptyEntry.BeginTimestamp = 0L;
                    emptyEntry.EndTimestamp = 0L;
                    versionList.TryAdd(-1L, emptyEntry);

                    VersionEntry.InitFirstVersionEntry(i, versionEntry.Record == null ? new String('a', 100) : versionEntry.Record, versionEntry);
                    versionList.TryAdd(0L, versionEntry);
                }

                pk++;
            }

            Console.WriteLine("Recycle {0} Version Entries");
        }
    }
}


