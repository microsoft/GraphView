
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;

    internal class PartitionedVersionTableVisitor : VersionTableVisitor
    {
        // A reference to the dict in version table
        private readonly Dictionary<object, Dictionary<long, VersionEntry>> dict;

        public PartitionedVersionTableVisitor(Dictionary<object, Dictionary<long, VersionEntry>> dict)
        {
            this.dict = dict;
        }

        internal override void Visit(DeleteVersionRequest req)
        {
            Dictionary<long, VersionEntry> versionList = null;
            if (dict.TryGetValue(req.RecordKey, out versionList) &&
                versionList.ContainsKey(req.VersionKey))
            {
                versionList.Remove(req.VersionKey);

                // should reset the lastVersionKey, set the lastVersionKey as the current - 1
                VersionEntry tailEntry = versionList[VersionEntry.VERSION_KEY_STRAT_INDEX];
                tailEntry.BeginTimestamp -= 1;
            }

            req.Result = true;
            req.Finished = true;
        }

        internal override void Visit(GetVersionListRequest req)
        {
            Dictionary<long, VersionEntry> versionList = null;
            if (!this.dict.TryGetValue(req.RecordKey, out versionList))
            {
                req.Result = req.Container;
                req.Finished = true;
                return;
            }

            VersionEntry tailPointer = versionList[VersionEntry.VERSION_KEY_STRAT_INDEX];
            long lastVersionKey = tailPointer.BeginTimestamp;

            TxList<VersionEntry> localList = req.Container;
            // Only returns top 2 newest versions. This is enough for serializability. 
            // For other isolation levels, more versions may need to be returned.
            // When old versions may be truncated, it is desirable to maintain a head pointer as well,
            // so as to increase the lower bound of version keys and reduce the number of iterations. 
            while (lastVersionKey >= 0 && localList.Count <= 2)
            {
                // To make it run under .Net 4.5
                VersionEntry verEntry = null;
                versionList.TryGetValue(lastVersionKey, out verEntry);

                if (verEntry != null)
                {
                    localList.Add(verEntry);
                    if (verEntry.TxId == VersionEntry.EMPTY_TXID)
                    {
                        break;
                    }
                }

                lastVersionKey--;
            }

            req.Finished = true;
        }

        internal override void Visit(InitiGetVersionListRequest req)
        {
            // The version list is empty
            if (!this.dict.ContainsKey(req.RecordKey))
            {
                Dictionary<long, VersionEntry> newVersionList =
                    new Dictionary<long, VersionEntry>(SingletonPartitionedVersionTable.VERSION_CAPACITY);

                // Adds a special entry whose key is -1 when the list is initialized.
                // The entry uses beginTimestamp as a pointer pointing to the newest verion in the list.
                newVersionList.Add(
                    VersionEntry.VERSION_KEY_STRAT_INDEX, VersionEntry.InitEmptyVersionEntry(req.RecordKey));

                this.dict.Add(req.RecordKey, newVersionList);

                // The version list is newly created by this tx. 
                // No meaningful versions exist, except for the artificial entry as a tail pointer. 
                req.Result = 1L;
                req.Finished = true;
                return;
            }
        }

        internal override void Visit(ReadVersionRequest req)
        {
            Dictionary<long, VersionEntry> versionList = null;
            VersionEntry verEntry = null;
            if (this.dict.TryGetValue(req.RecordKey, out versionList) && 
                versionList.TryGetValue(req.VersionKey, out verEntry))
            {
                req.Result = verEntry;
            }
            else
            {
                req.Result = null;
            }
            req.Finished = true;
        }

        internal override void Visit(ReplaceVersionRequest req)
        {
            Dictionary<long, VersionEntry> versionList = null;
            VersionEntry verEntry = null;

            if (!this.dict.TryGetValue(req.RecordKey, out versionList) || 
                !versionList.TryGetValue(req.VersionKey, out verEntry))
            {
                throw new TransactionException("The specified version does not exist.");
            }

            if (verEntry.TxId == req.ReadTxId && verEntry.EndTimestamp == req.ExpectedEndTs)
            {
                verEntry.BeginTimestamp = req.BeginTs;
                verEntry.EndTimestamp = req.EndTs;
                verEntry.TxId = req.TxId;
            }

            req.Result = verEntry;
            req.Finished = true;
        }

        internal override void Visit(ReplaceWholeVersionRequest req)
        {
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
            Dictionary<long, VersionEntry> versionList = null;
            VersionEntry verEntry = null;

            if (!this.dict.TryGetValue(req.RecordKey, out versionList) ||
                !versionList.TryGetValue(req.VersionKey, out verEntry))
            {
                throw new TransactionException("The specified version does not exist.");
            }

            // Only update the max commit time when uploaded commit time is larger than the version's
            verEntry.MaxCommitTs = Math.Max(verEntry.MaxCommitTs, req.MaxCommitTs);
            
            req.Result = verEntry;
            req.Finished = true;
        }

        internal override void Visit(UploadVersionRequest req)
        {
            Dictionary<long, VersionEntry> versionList = null;
            if (!this.dict.TryGetValue(req.RecordKey, out versionList))
            {
                throw new TransactionException("The specified record does not exist.");
            }

            if (versionList.ContainsKey(req.VersionKey))
            {
                req.Result = false;  
            }
            else
            {
                versionList.Add(req.VersionKey, req.VersionEntry);
                // Take the dirty version entry to store the current largest version key
                VersionEntry tailEntry = versionList[VersionEntry.VERSION_KEY_STRAT_INDEX];
                tailEntry.BeginTimestamp = req.VersionKey;

                req.Result = true;
            }
            req.Finished = true;
        }
    }
}
