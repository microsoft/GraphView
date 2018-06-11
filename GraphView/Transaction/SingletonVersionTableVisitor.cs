namespace GraphView.Transaction
{
    using System.Threading;
    using System.Collections.Concurrent;

    internal class SingletonVersionTableVisitor : VersionTableVisitor
    {
        private readonly ConcurrentDictionary<object, ConcurrentDictionary<long, VersionEntry>> dict;

        public SingletonVersionTableVisitor(ConcurrentDictionary<object, ConcurrentDictionary<long, VersionEntry>> dict)
        {
            this.dict = dict;
        }

        internal override void Visit(DeleteVersionRequest req)
        {
            ConcurrentDictionary<long, VersionEntry> versionList = null;
            if (!this.dict.TryGetValue(req.RecordKey, out versionList))
            {
                req.Result = 1L;
                req.Finished = true;
                return;
            }

            VersionEntry versionEntry = null;
            if (versionList.TryRemove(req.VersionKey, out versionEntry))
            {
                req.Result = 1L;
                req.Finished = true;
            }
            else
            {
                req.Result = 0L;
                req.Finished = true;
            }
        }

        internal override void Visit(InitiGetVersionListRequest req)
        {
            ConcurrentDictionary<long, VersionEntry> versionList = null;
            if (!this.dict.TryGetValue(req.RecordKey, out versionList))
            {
                ConcurrentDictionary<long, VersionEntry> newVersionList = new ConcurrentDictionary<long, VersionEntry>();
                // Adds a special entry whose key is -1 when the list is initialized.
                // The entry uses beginTimestamp as a pointer pointing to the newest verion in the list.
                VersionEntry entry = VersionEntry.InitEmptyVersionEntry(req.RecordKey);
                newVersionList.TryAdd(SingletonDictionaryVersionTable.TAIL_KEY, entry);

                if (this.dict.TryAdd(req.RecordKey, newVersionList))
                {
                    // The version list is newly created by this tx. 
                    // No meaningful versions exist, except for the artificial entry as a tail pointer. 
                    req.Result = 1L;
                    req.Finished = true;
                    return;
                }
            }
            else
            {
                req.Result = 0L;
                req.Finished = true;
            }
        }

        internal override void Visit(ReplaceVersionRequest req)
        {
            ConcurrentDictionary<long, VersionEntry> versionList = null;
            if (!this.dict.TryGetValue(req.RecordKey, out versionList))
            {
                throw new TransactionException("The specified record does not exist.");
            }

            VersionEntry entry = null;
            if (!versionList.TryGetValue(req.VersionKey, out entry))
            {
                throw new TransactionException("The specified version does not exist.");
            }

            if (entry.TxId == req.ReadTxId && entry.EndTimestamp == req.ExpectedEndTs)
            {
                while (Interlocked.CompareExchange(ref entry.latch, 1, 0) != 0) ;

                entry.BeginTimestamp = req.BeginTs;
                entry.EndTimestamp = req.EndTs;
                entry.TxId = req.TxId;
                VersionEntry.CopyValue(entry, req.VersionEntry);

                Interlocked.Exchange(ref entry.latch, 0);
            }

            req.Result = req.VersionEntry;
            req.Finished = true;
        }

        internal override void Visit(UploadVersionRequest req)
        {
            ConcurrentDictionary<long, VersionEntry> versionList = null;
            if (!this.dict.TryGetValue(req.RecordKey, out versionList))
            {
                throw new TransactionException("The specified record does not exist.");
            }

            if (versionList.TryAdd(req.VersionKey, req.VersionEntry))
            {
                // The new version has been inserted successfully. Re-directs the tail pointer to the new version.  
                VersionEntry tailEntry = null;
                versionList.TryGetValue(SingletonDictionaryVersionTable.TAIL_KEY, out tailEntry);

                if (tailEntry == null)
                {
                    throw new TransactionException("Tail pointer is missing from the version list.");
                }

                long tailKey = tailEntry.VersionKey;
                while (tailKey < req.VersionKey)
                {
                    // Here we use Interlocked to atomically update the tail entry, instead of ConcurrentDict.TryUpdate().
                    // This is because once created, the whole tail entry always stays and is never replaced.
                    // All concurrent tx's only access the tail pointer, i.e., the beginTimestamp field.  
                    Interlocked.CompareExchange(ref tailEntry.VersionKey, req.VersionKey, tailKey);
                    tailKey = tailEntry.VersionKey;
                }

                req.Result = 1L;
                req.Finished = true;
                return;
            }
            else
            {
                // The same version key has been added before or by a concurrent tx. 
                // The new version cannot be inserted.
                req.Result = 0L;
                req.Finished = true;
            }
        }

        internal override void Visit(UpdateVersionMaxCommitTsRequest req)
        {
            ConcurrentDictionary<long, VersionEntry> versionList = null;
            if (!this.dict.TryGetValue(req.RecordKey, out versionList))
            {
                throw new TransactionException("The specified record does not exist.");
            }

            VersionEntry verEntry = null;
            if (!versionList.TryGetValue(req.VersionKey, out verEntry))
            {
                throw new TransactionException("The specified version does not exist.");
            }

            while (Interlocked.CompareExchange(ref verEntry.latch, 1, 0) != 0) ;

            verEntry.MaxCommitTs = req.MaxCommitTs;
            VersionEntry.CopyValue(verEntry, req.VersionEntry);

            Interlocked.Exchange(ref verEntry.latch, 0);

            req.Result = req.VersionEntry;
            req.Finished = true;
        }

        internal override void Visit(GetVersionListRequest req)
        {
            ConcurrentDictionary<long, VersionEntry> versionList = null;
            if (!this.dict.TryGetValue(req.RecordKey, out versionList))
            {
                req.Result = req.Container;
                req.Finished = true;
                return;
            }

            // The value at -1 in the version list is a special entry, 
            // whose beginTimestamp points to the newest version. 
            VersionEntry tailEntry = null;
            versionList.TryGetValue(SingletonDictionaryVersionTable.TAIL_KEY, out tailEntry);
            long lastVersionKey = Interlocked.Read(ref tailEntry.VersionKey);

            TxList<VersionEntry> localList = req.Container;

            int entryCount = 0;
            // Only returns top 2 newest versions. This is enough for serializability. 
            // For other isolation levels, more versions may need to be returned.
            // When old versions may be truncated, it is desirable to maintain a head pointer as well,
            // so as to increase the lower bound of version keys and reduce the number of iterations. 
            while (lastVersionKey >= 0 && entryCount <= 2)
            {
                VersionEntry verEntry = null;
                if (versionList.TryGetValue(lastVersionKey, out verEntry))
                {
                    while (Interlocked.CompareExchange(ref verEntry.latch, 1, 0) != 0) ;
                    VersionEntry.CopyValue(verEntry, localList[entryCount++]);
                    Interlocked.Exchange(ref verEntry.latch, 0);

                    if (verEntry.TxId == VersionEntry.EMPTY_TXID)
                    {
                        break;
                    }
                }

                lastVersionKey--;
            }

            req.Finished = true;
        }

        internal override void Visit(ReadVersionRequest req)
        {
            ConcurrentDictionary<long, VersionEntry> versionList = null;
            if (!this.dict.TryGetValue(req.RecordKey, out versionList))
            {
                req.Result = null;
                req.Finished = true;
                return;
            }

            VersionEntry versionEntry = null;
            if (!versionList.TryGetValue(req.VersionKey, out versionEntry))
            {
                req.Result = null;
                req.Finished = true;
            }
            else
            {
                while (Interlocked.CompareExchange(ref versionEntry.latch, 1, 0) != 0) ;
                VersionEntry.CopyValue(versionEntry, req.VersionEntry);
                Interlocked.Exchange(ref versionEntry.latch, 0);

                req.Result = req.VersionEntry;
                req.Finished = true;
            }
        }
    }
}
