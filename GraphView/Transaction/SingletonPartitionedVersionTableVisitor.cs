namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Concurrent;

    internal class SingletonPartitionedVersionTableVisitor : VersionTableVisitor
    {
        // A reference to the dict in version table
        private readonly Dictionary<object, Dictionary<long, VersionEntry>> dict;

        public SingletonPartitionedVersionTableVisitor(Dictionary<object, Dictionary<long, VersionEntry>> dict)
        {
            this.dict = dict;
        }

        internal override void Visit(DeleteVersionRequest req)
        { 
            Dictionary<long, VersionEntry> versionList = req.RemoteVerList as
                Dictionary<long, VersionEntry>;
            if (versionList == null)
            {
                if (!dict.TryGetValue(req.RecordKey, out versionList))
                {
                    req.Result = true; 
                    req.Finished = true;
                }
            }

            VersionEntry verEntry = versionList[req.VersionKey];
            VersionEntry tailEntry = versionList[SingletonDictionaryVersionTable.TAIL_KEY];

            long tailKey = tailEntry.BeginTimestamp;
            long headKey = tailEntry.EndTimestamp;

            tailEntry.BeginTimestamp = tailKey - 1;
            if (headKey > 0)
            {
                versionList.Add(headKey - 1, verEntry);
                tailEntry.EndTimestamp = headKey - 1;
            }

            if (versionList.Remove(req.VersionKey))
            {
                // should reset the lastVersionKey, set the lastVersionKey as the current - 1
                req.Result = true;
            }
            else
            {
                req.Result = false;
            }

            req.Finished = true;
        }

        internal override void Visit(GetVersionListRequest req)
        {
            Dictionary<long, VersionEntry> versionList = null;
            if (!this.dict.TryGetValue(req.RecordKey, out versionList))
            {
                req.RemoteVerList = null;
                req.Result = 0;
                req.Finished = true;
                return;
            }

            VersionEntry tailPointer = versionList[SingletonDictionaryVersionTable.TAIL_KEY];
            long lastVersionKey = tailPointer.BeginTimestamp;

            TxList<VersionEntry> localList = req.LocalContainer;
            TxList<VersionEntry> remoteList = req.RemoteContainer;
            int entryCount = 0;
            // Only returns top 2 newest versions. This is enough for serializability. 
            // For other isolation levels, more versions may need to be returned.
            // When old versions may be truncated, it is desirable to maintain a head pointer as well,
            // so as to increase the lower bound of version keys and reduce the number of iterations. 
            while (lastVersionKey >= 0 && entryCount < 2)
            {
                // To make it run under .Net 4.5
                VersionEntry verEntry = null;
                versionList.TryGetValue(lastVersionKey, out verEntry);

                if (verEntry != null)
                {
                    VersionEntry.CopyValue(verEntry, localList[entryCount]);
                    remoteList.Add(verEntry);
                    entryCount++;

                    if (verEntry.TxId == VersionEntry.EMPTY_TXID)
                    {
                        break;
                    }
                }

                lastVersionKey--;
            }

            req.RemoteVerList = versionList;
            req.Result = entryCount;
            req.Finished = true;
        }

        internal override void Visit(InitiGetVersionListRequest req)
        {
            // The version list is empty
            if (!this.dict.ContainsKey(req.RecordKey))
            {
                Dictionary<long, VersionEntry> newVersionList =
                    new Dictionary<long, VersionEntry>(SingletonPartitionedVersionTable.VERSION_CAPACITY);

                // Adds a special entry whose key is TAIL_KEY when the list is initialized.
                // The entry uses beginTimestamp as a pointer pointing to the newest verion in the list.
                newVersionList.Add(
                    SingletonDictionaryVersionTable.TAIL_KEY, VersionEntry.InitEmptyVersionEntry());

                this.dict.Add(req.RecordKey, newVersionList);

                // The version list is newly created by this tx. 
                // No meaningful versions exist, except for the artificial entry as a tail pointer. 
                req.RemoteVerList = newVersionList;
                req.Result = true;
                req.Finished = true;
                return;
            }
            else
            {
                req.RemoteVerList = null;
                req.Result = false;
                req.Finished = true;
            }
        }

        internal override void Visit(ReadVersionRequest req)
        {
            VersionEntry verEntry = req.RemoteVerEntry;
            if (verEntry == null)
            {
                Dictionary<long, VersionEntry> versionList = null;
                if (!this.dict.TryGetValue(req.RecordKey, out versionList) 
                    || !versionList.TryGetValue(req.VersionKey, out verEntry))
                {
                    req.Result = null;
                    req.Finished = true;
                }
            }

            VersionEntry.CopyValue(verEntry, req.LocalVerEntry);

            req.Result = req.LocalVerEntry;
            req.Finished = true;
        }

        internal override void Visit(ReplaceVersionRequest req)
        {
            VersionEntry verEntry = req.RemoteVerEntry;
            if (verEntry == null)
            {
                Dictionary<long, VersionEntry> versionList = null;
                if (!this.dict.TryGetValue(req.RecordKey, out versionList) ||
                    !versionList.TryGetValue(req.VersionKey, out verEntry))
                {
                    throw new TransactionException("The specified version does not exist.");
                }    
            }

            if (verEntry.TxId == req.SenderId && verEntry.EndTimestamp == req.ExpectedEndTs)
            {
                verEntry.BeginTimestamp = req.BeginTs;
                verEntry.EndTimestamp = req.EndTs;
                verEntry.TxId = req.TxId;
            }
            VersionEntry.CopyValue(verEntry, req.LocalVerEntry);

            req.Result = req.LocalVerEntry;
            req.Finished = true;
        }

        internal override void Visit(ReplaceWholeVersionRequest req)
        {
            // Replace the whole version, no need to use remoteVersionEntry
            Dictionary<long, VersionEntry> versionList = null;
            if (!this.dict.TryGetValue(req.RecordKey, out versionList) ||
                !versionList.ContainsKey(req.VersionKey))
            {
                throw new TransactionException("The specified version does not exist.");
            }

            versionList[req.VersionKey] = req.VersionEntry;
            req.Result = true;
            req.Finished = true;
        }

        internal override void Visit(UpdateVersionMaxCommitTsRequest req)
        {
            VersionEntry verEntry = req.RemoteVerEntry;
            if (verEntry == null)
            {
                Dictionary<long, VersionEntry> versionList = null;
                if (!this.dict.TryGetValue(req.RecordKey, out versionList) ||
                    !versionList.TryGetValue(req.VersionKey, out verEntry))
                {
                    throw new TransactionException("The specified version does not exist.");
                }
            }

            // Only update the max commit time when uploaded commit time is larger than the version's
            verEntry.MaxCommitTs = Math.Max(verEntry.MaxCommitTs, req.MaxCommitTs);
            VersionEntry.CopyValue(verEntry, req.LocalVerEntry);

            req.Result = req.LocalVerEntry;
            req.Finished = true;
        }

        internal override void Visit(UploadVersionRequest req)
        {
            Dictionary<long, VersionEntry> versionList = req.RemoteVerList as 
                Dictionary<long, VersionEntry>;

            if (versionList == null)
            {
                if (!this.dict.TryGetValue(req.RecordKey, out versionList))
                {
                    throw new TransactionException("The specified record does not exist.");
                }
            }

            if (versionList.ContainsKey(req.VersionKey))
            {
                req.RemoteVerEntry = req.VersionEntry;
                req.Result = false;  
            }
            else
            { 
                versionList.Add(req.VersionKey, req.VersionEntry);

                // Take the dirty version entry to store the current largest version key
                VersionEntry tailEntry = versionList[SingletonDictionaryVersionTable.TAIL_KEY];
                tailEntry.BeginTimestamp = req.VersionKey;

                VersionEntry oldVersion = null;
                if (versionList.Count > VersionTable.VERSION_CAPACITY)
                {
                    long headKey = tailEntry.EndTimestamp;
                    tailEntry.EndTimestamp = headKey + 1;

                    versionList.TryGetValue(headKey, out oldVersion);
                    versionList.Remove(headKey);
                }

                req.RemoteVerEntry = oldVersion == null ? new VersionEntry() : oldVersion;
                req.Result = true;
            }
            req.Finished = true;
        }
    }
}
