using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using NonBlocking;

namespace GraphView.Transaction
{
    internal class SingletonVersionTableVisitor : VersionTableVisitor
    {
        private readonly NonBlocking.ConcurrentDictionary<object, NonBlocking.ConcurrentDictionary<long, VersionEntry>> dict;
        private readonly TxResourceManager txResourceManager;

        public SingletonVersionTableVisitor(NonBlocking.ConcurrentDictionary<object, NonBlocking.ConcurrentDictionary<long, VersionEntry>> dict, 
            TxResourceManager txResourceManager)
        {
            this.dict = dict;
            this.txResourceManager = txResourceManager;
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
                this.txResourceManager.RecycleVersionEntry(ref versionEntry);
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
                ConcurrentDictionary<long, VersionEntry> newVersionList = this.txResourceManager.GetConcurrentDictionary();
                // Adds a special entry whose key is -1 when the list is initialized.
                // The entry uses beginTimestamp as a pointer pointing to the newest verion in the list.
                VersionEntry entry = this.txResourceManager.GetVersionEntry();
                entry.RecordKey = req.RecordKey;
                entry.VersionKey = -1;
                entry.BeginTimestamp = -1;
                entry.EndTimestamp = -1;
                entry.Record = null;
                entry.TxId = -1;
                entry.MaxCommitTs = -1;
                newVersionList.Add(SingletonDictionaryVersionTable.TAIL_KEY, entry);

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
                VersionEntry newEntry = this.txResourceManager.GetVersionEntry();
                newEntry.RecordKey = req.RecordKey;
                newEntry.VersionKey = req.VersionKey;
                newEntry.BeginTimestamp = req.BeginTs;
                newEntry.EndTimestamp = req.EndTs;
                newEntry.Record = entry.Record;
                newEntry.TxId = req.TxId;
                newEntry.MaxCommitTs = entry.MaxCommitTs;

                if (versionList.TryUpdate(req.VersionKey, newEntry, entry))
                {
                    // Successfully replaces the version. Returns the new version entry.
                    this.txResourceManager.RecycleVersionEntry(ref entry);
                    entry = newEntry;
                }
                else
                {
                    // The version entry has been updated since the prior retrieval,  
                    // causing the replacement failed. Re-read to get a new image. 
                    versionList.TryGetValue(req.VersionKey, out entry);
                }
            }

            req.Result = entry;
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

            while (verEntry.MaxCommitTs < req.MaxCommitTs)
            {
                VersionEntry newEntry = this.txResourceManager.GetVersionEntry();
                newEntry.RecordKey = req.RecordKey;
                newEntry.VersionKey = req.VersionKey;
                newEntry.BeginTimestamp = verEntry.BeginTimestamp;
                newEntry.EndTimestamp = verEntry.EndTimestamp;
                newEntry.Record = verEntry.Record;
                newEntry.TxId = verEntry.TxId;
                newEntry.MaxCommitTs = req.MaxCommitTs;

                if (versionList.TryUpdate(req.VersionKey, newEntry, verEntry))
                {
                    this.txResourceManager.RecycleVersionEntry(ref verEntry);
                    verEntry = newEntry;
                    break;
                }
                else
                {
                    versionList.TryGetValue(req.VersionKey, out verEntry);
                }
            }

            req.Result = verEntry;
            req.Finished = true;
        }

        internal override void Visit(GetVersionListRequest req)
        {
            ConcurrentDictionary<long, VersionEntry> versionList = null;
            if (!this.dict.TryGetValue(req.RecordKey, out versionList))
            {
                req.Result = null;
                req.Finished = true;
                return;
            }

            // The value at -1 in the version list is a special entry, 
            // whose beginTimestamp points to the newest version. 
            VersionEntry tailEntry = null;
            versionList.TryGetValue(SingletonDictionaryVersionTable.TAIL_KEY, out tailEntry);
            long lastVersionKey = Interlocked.Read(ref tailEntry.VersionKey);

            List<VersionEntry> localList = this.txResourceManager.GetVersionList();

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
                }

                lastVersionKey--;
            }

            req.Result = localList;
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
                req.Result = versionEntry;
                req.Finished = true;
            }
        }
    }
}
