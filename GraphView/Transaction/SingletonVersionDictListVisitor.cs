
namespace GraphView.Transaction
{
    using System;
    using System.Threading;
    using NonBlocking;

    internal class SingletonVersionDictListVisitor : VersionTableVisitor
    {
        private readonly ConcurrentDictionary<object, VersionList> dict;

        public SingletonVersionDictListVisitor(ConcurrentDictionary<object, VersionList> dict)
        {
            this.dict = dict;
        }

        static void ResetTailEntry(VersionEntry tailEntry)
        {
            tailEntry.BeginTimestamp = VersionEntry.DEFAULT_BEGIN_TIMESTAMP;
            tailEntry.EndTimestamp = VersionEntry.DEFAULT_END_TIMESTAMP;
        }

        internal override void Visit(DeleteVersionRequest req)
        {
            VersionList versionList = req.RemoteVerList as VersionList;
            // Only get the version list location when version list is null
            if (versionList == null)
            {
                if (!this.dict.TryGetValue(req.RecordKey, out versionList))
                {
                    req.Result = true;
                    req.Finished = true;
                    return;
                }
            }

            versionList.TryRemove(req.SenderId, req.VersionKey);
            req.Result = true;
            req.Finished = true;
        }

        internal override void Visit(InitiGetVersionListRequest req)
        {
            if (!this.dict.TryGetValue(req.RecordKey, out VersionList versionList))
            {
                VersionList newVersionList = new VersionList();

                if (this.dict.TryAdd(req.RecordKey, newVersionList))
                {
                    // The version list is newly created by this tx. 
                    // No meaningful versions exist, except for the artificial entry as a tail pointer. 
                    req.RemoteVerList = newVersionList;
                    req.Result = true;
                    req.Finished = true;
                    return;
                }
            }
            else
            {
                req.RemoteVerList = null;
                req.Result = false;
                req.Finished = true;
            }
        }

        internal override void Visit(ReplaceVersionRequest req)
        {
            VersionEntry entry = req.RemoteVerEntry;
            if (entry == null)
            {
                throw new TransactionException("The version entry should be referenced for main-memory k-v.");
            }

            if (entry.TxId == req.SenderId && 
                entry.EndTimestamp == req.ExpectedEndTs && 
                entry.VersionKey == req.VersionKey)
            {
                while (Interlocked.CompareExchange(ref entry.latch, 1, 0) != 0) ;

                entry.BeginTimestamp = req.BeginTs;
                entry.EndTimestamp = req.EndTs;
                entry.TxId = req.TxId;
                VersionEntry.CopyValue(entry, req.LocalVerEntry);

                Interlocked.Exchange(ref entry.latch, 0);
            }

            req.Result = req.LocalVerEntry;
            req.Finished = true;
        }

        internal override void Visit(UploadVersionRequest req)
        {
            VersionList versionList = req.RemoteVerList as VersionList;
            if (versionList == null)
            {
                if (!this.dict.TryGetValue(req.RecordKey, out versionList))
                {
                    throw new TransactionException("The specified record does not exist.");
                }
            }

            if (versionList.TryAdd(req.VersionEntry, out VersionEntry remoteEntry))
            {
                req.RemoteVerEntry = remoteEntry;
                req.Result = true;
                req.Finished = true;
                return;
            }
            else
            {
                // The same version key has been added before or by a concurrent tx. 
                // The new version cannot be inserted.
                req.RemoteVerEntry = req.VersionEntry;
                req.Result = false;
                req.Finished = true;
            }
        }

        internal override void Visit(UpdateVersionMaxCommitTsRequest req)
        {
            VersionEntry verEntry = req.RemoteVerEntry;
            if (verEntry == null)
            {
                throw new TransactionException("The version entry should be referenced for main-memory k-v.");
            }

            if (verEntry.VersionKey != req.VersionKey)
            {
                throw new TransactionException("The referenced version entry has been recycled for new data.");
            }

            while (Interlocked.CompareExchange(ref verEntry.latch, 1, 0) != 0) ;

            verEntry.MaxCommitTs = Math.Max(req.MaxCommitTs, verEntry.MaxCommitTs);
            VersionEntry.CopyValue(verEntry, req.LocalVerEntry);

            Interlocked.Exchange(ref verEntry.latch, 0);

            req.Result = req.LocalVerEntry;
            req.Finished = true;
        }

        internal override void Visit(GetVersionListRequest req)
        {
            if (!this.dict.TryGetValue(req.RecordKey, out VersionList versionList))
            {
                req.RemoteVerList = null;
                req.Result = 0;
                req.Finished = true;
                return;
            }

            TxList<VersionEntry> localList = req.LocalContainer;
            TxList<VersionEntry> remoteList = req.RemoteContainer;
            int entryCount = 0;

            versionList.TryPeek(
                out VersionEntry lastVersion, 
                out VersionEntry secondToLastEntry);

            while (Interlocked.CompareExchange(ref lastVersion.latch, 1, 0) != 0) ;
            VersionEntry.CopyValue(lastVersion, localList[entryCount]);
            Interlocked.Exchange(ref lastVersion.latch, 0);

            // Add a reference to a version entry. No need to take the latch.
            remoteList.Add(lastVersion);
            entryCount++;

            if (lastVersion.TxId != VersionEntry.EMPTY_TXID)
            {
                while (Interlocked.CompareExchange(ref secondToLastEntry.latch, 1, 0) != 0) ;
                VersionEntry.CopyValue(secondToLastEntry, localList[entryCount]);
                Interlocked.Exchange(ref secondToLastEntry.latch, 0);

                // Add a reference to a version entry. No need to take the latch.
                remoteList.Add(secondToLastEntry);
                entryCount++;
            }

            req.RemoteVerList = versionList;
            req.Result = entryCount;
            req.Finished = true;
        }

        internal override void Visit(ReadVersionRequest req)
        {
            VersionEntry versionEntry = req.RemoteVerEntry;
            if (versionEntry == null)
            {
                throw new TransactionException("The version entry should be referenced for main-memory k-v.");
            }

            if (versionEntry.VersionKey != req.VersionKey)
            {
                throw new TransactionException("The referenced version entry has been recycled for new data.");
            }

            while (Interlocked.CompareExchange(ref versionEntry.latch, 1, 0) != 0) ;
            VersionEntry.CopyValue(versionEntry, req.LocalVerEntry);
            Interlocked.Exchange(ref versionEntry.latch, 0);

            req.Result = req.LocalVerEntry;
            req.Finished = true;
        }
    }
}
