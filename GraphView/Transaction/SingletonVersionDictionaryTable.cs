namespace GraphView.Transaction
{
    using System.Collections.Generic;
    using System.Threading;
    using NonBlocking;

    /// <summary>
    /// A version table implementation in single machine environment.
    /// Use a Dictionary.
    /// </summary>
    internal partial class SingletonDictionaryVersionTable : VersionTable
    {
        private readonly NonBlocking.ConcurrentDictionary<object, NonBlocking.ConcurrentDictionary<long, VersionBlob>> dict;

        private static readonly long TAIL_KEY = -1L;

        private static readonly int RECORD_CAPACITY = 1000000;

        private static readonly int VERSION_CAPACITY = 32;

        public SingletonDictionaryVersionTable(VersionDb versionDb, string tableId)
            : base(versionDb, tableId)
        {
            this.dict = new ConcurrentDictionary<object, ConcurrentDictionary<long, VersionBlob>>(
                SingletonDictionaryVersionTable.RECORD_CAPACITY);
            
        }

        internal override bool DeleteVersionEntry(object recordKey, long versionKey)
        {
            ConcurrentDictionary<long, VersionBlob> versionList = null;
            if (!this.dict.TryGetValue(recordKey, out versionList))
            {
                return true;
            }

            VersionBlob versionBlob = null;
            return versionList.TryRemove(versionKey, out versionBlob);
        }

        internal override VersionEntry GetVersionEntryByKey(object recordKey, long versionKey)
        {
            ConcurrentDictionary<long, VersionBlob> versionList = null;
            if (!this.dict.TryGetValue(recordKey, out versionList))
            {
                return null;
            }

            VersionBlob versionBlob = null;
            if (!versionList.TryGetValue(versionKey, out versionBlob))
            {
                return null;
            }

            return new VersionEntry(
                recordKey,
                versionKey,
                versionBlob.beginTimestamp,
                versionBlob.endTimestamp,
                versionBlob.payload,
                versionBlob.txId,
                versionBlob.maxCommitTs);
        }

        internal override IEnumerable<VersionEntry> InitializeAndGetVersionList(object recordKey)
        {
            ConcurrentDictionary<long, VersionBlob> versionList = null;
            if (!this.dict.TryGetValue(recordKey, out versionList))
            {
                ConcurrentDictionary<long, VersionBlob> newVersionList = 
                    new ConcurrentDictionary<long, VersionBlob>(SingletonDictionaryVersionTable.VERSION_CAPACITY);
                // Adds a special entry whose key is -1 when the list is initialized.
                // The entry uses beginTimestamp as a pointer pointing to the newest verion in the list.
                newVersionList.Add(SingletonDictionaryVersionTable.TAIL_KEY, new VersionBlob(-1, -1, null, -1, -1));

                if (this.dict.TryAdd(recordKey, newVersionList))
                {
                    // The version list is newly created by this tx. 
                    // No meaningful versions exist, except for the artificial entry as a tail pointer. 
                    return null;
                }
            }

            // Retrieves the tail pointer. 
            VersionBlob tailBlob = null;
            if(!versionList.TryGetValue(SingletonDictionaryVersionTable.TAIL_KEY, out tailBlob))
            {
                throw new TransactionException("The tail pointer is missing from the version list.");
            }
            long lastVersionKey = Interlocked.Read(ref tailBlob.beginTimestamp);

            List<VersionEntry> localList = new List<VersionEntry>(2);

            // Only returns top 2 newest versions. This is enough for serializability. 
            // For other isolation levels, more versions may need to be returned.
            // When old versions may be truncated, it is desirable to maintain a head pointer as well,
            // so as to increase the lower bound of version keys and reduce the number of iterations. 
            while (lastVersionKey >= 0 && localList.Count <= 2)
            {
                VersionBlob verBlob = null;
                if (!versionList.TryGetValue(lastVersionKey, out verBlob))
                {
                    lastVersionKey--;
                    continue;
                }

                VersionEntry verEntry = new VersionEntry(
                    recordKey,
                    lastVersionKey,
                    verBlob.beginTimestamp,
                    verBlob.endTimestamp,
                    verBlob.payload,
                    verBlob.txId,
                    verBlob.maxCommitTs);
                localList.Add(verEntry);
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
            ConcurrentDictionary<long, VersionBlob> versionList = null;
            if (!this.dict.TryGetValue(recordKey, out versionList))
            {
                throw new TransactionException("The specified record does not exist.");
            }

            VersionBlob blob = null;
            if (!versionList.TryGetValue(versionKey, out blob))
            {
                throw new TransactionException("The specified version does not exist.");
            }

            if (blob.txId == readTxId && blob.endTimestamp == expectedEndTimestamp)
            {
                VersionBlob newBlob = new VersionBlob(
                    beginTimestamp, endTimestamp, blob.payload, txId, blob.maxCommitTs);

                if (versionList.TryUpdate(versionKey, newBlob, blob))
                {
                    // Successfully replaces the version. Returns the new version entry.
                    return new VersionEntry(
                        recordKey, 
                        versionKey, 
                        beginTimestamp, 
                        endTimestamp, 
                        newBlob.payload, 
                        txId, 
                        newBlob.maxCommitTs);
                }
                else
                {
                    // The version entry has been updated since the prior retrieval,  
                    // causing the replacement failed. Re-read to get a new image. 
                    versionList.TryGetValue(versionKey, out blob);
                }
            }
            
            return new VersionEntry(
                recordKey, 
                versionKey, 
                blob.beginTimestamp, 
                blob.endTimestamp, 
                blob.payload, 
                blob.txId, 
                blob.maxCommitTs);
        }

        internal override bool UploadNewVersionEntry(object recordKey, long versionKey, VersionEntry versionEntry)
        {
            ConcurrentDictionary<long, VersionBlob> versionList = null;
            if (!this.dict.TryGetValue(recordKey, out versionList))
            {
                throw new TransactionException("The specified record does not exist.");
            }

            VersionBlob versionBlob = new VersionBlob(
                versionEntry.BeginTimestamp, 
                versionEntry.EndTimestamp, 
                versionEntry.Record, 
                versionEntry.TxId, 
                versionEntry.MaxCommitTs);

            if (versionList.TryAdd(versionKey, versionBlob))
            {
                // The new version has been inserted successfully. Re-directs the tail pointer to the new version.  

                VersionBlob tailBlob = null;
                versionList.TryGetValue(SingletonDictionaryVersionTable.TAIL_KEY, out tailBlob);

                if (tailBlob == null)
                {
                    throw new TransactionException("Tail pointer is missing from the version list.");
                }

                long tailKey = tailBlob.beginTimestamp;
                while (tailKey < versionKey)
                {
                    // Here we use Interlocked to atomically update the tail entry, instead of ConcurrentDict.TryUpdate().
                    // This is because once created, the whole tail entry always stays and is never replaced.
                    // All concurrent tx's only access the tail pointer, i.e., the beginTimestamp field.  
                    Interlocked.CompareExchange(ref tailBlob.beginTimestamp, versionKey, tailKey);
                    tailKey = tailBlob.beginTimestamp;
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
            ConcurrentDictionary<long, VersionBlob> versionList = null;
            if (!this.dict.TryGetValue(recordKey, out versionList))
            {
                throw new TransactionException("The specified record does not exist.");
            }

            VersionBlob verBlob = null;
            if (!versionList.TryGetValue(versionKey, out verBlob))
            {
                throw new TransactionException("The specified version does not exist.");
            }

            while (verBlob.maxCommitTs < commitTs)
            {
                VersionBlob newBlob = new VersionBlob(
                    verBlob.beginTimestamp, verBlob.endTimestamp, verBlob.payload, verBlob.txId, commitTs);

                if (versionList.TryUpdate(versionKey, newBlob, verBlob))
                {
                    verBlob = newBlob;
                    break;
                }
                else
                {
                    versionList.TryGetValue(versionKey, out verBlob);
                }
            }

            return new VersionEntry(
                recordKey,
                versionKey,
                verBlob.beginTimestamp,
                verBlob.endTimestamp,
                verBlob.payload,
                verBlob.txId,
                verBlob.maxCommitTs);
        }

        internal override IEnumerable<VersionEntry> GetVersionList(object recordKey)
        {
            ConcurrentDictionary<long, VersionBlob> versionList = null;
            if (!this.dict.TryGetValue(recordKey, out versionList))
            {
                return null;
            }

            // The value at -1 in the version list is a special entry, 
            // whose beginTimestamp points to the newest version. 
            VersionBlob tailBlob = null;
            versionList.TryGetValue(SingletonDictionaryVersionTable.TAIL_KEY, out tailBlob);
            long lastVersionKey = Interlocked.Read(ref tailBlob.beginTimestamp);

            List<VersionEntry> localList = new List<VersionEntry>(2);

            // Only returns top 2 newest versions. This is enough for serializability. 
            // For other isolation levels, more versions may need to be returned.
            // When old versions may be truncated, it is desirable to maintain a head pointer as well,
            // so as to increase the lower bound of version keys and reduce the number of iterations. 
            while (lastVersionKey >= 0 && localList.Count <= 2)
            {
                VersionBlob verBlob = null;
                if (versionList.TryGetValue(lastVersionKey, out verBlob))
                {
                    VersionEntry verEntry = new VersionEntry(
                        recordKey,
                        lastVersionKey,
                        verBlob.beginTimestamp,
                        verBlob.endTimestamp,
                        verBlob.payload,
                        verBlob.txId,
                        verBlob.maxCommitTs);

                    localList.Add(verEntry);
                }

                lastVersionKey--;
            }

            return localList;
        }

        internal void Clear()
        {
            this.dict.Clear();
        }
    }
}


