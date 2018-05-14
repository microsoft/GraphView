
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;

    internal class PartitionVersionEntryRequestVisitor : TxRequestVisitor
    {
        // A reference to the dict in version table
        private readonly Dictionary<object, Dictionary<long, VersionEntry>> dict;

        public PartitionVersionEntryRequestVisitor(Dictionary<object, Dictionary<long, VersionEntry>> dict)
        {
            this.dict = dict;
        }

        internal void Invoke(VersionEntryRequest req)
        {
            req.Accept(this);
        }

        internal override void Visit(DeleteVersionRequest req)
        {
            if (dict.ContainsKey(req.RecordKey) && 
                dict[req.RecordKey].ContainsKey(req.VersionKey))
            {
                dict[req.RecordKey].Remove(req.VersionKey);

                // should reset the lastVersionKey, set the lastVersionKey as the current - 1
                VersionEntry tailEntry = dict[req.RecordKey][VersionEntry.VERSION_KEY_STRAT_INDEX];
                tailEntry.BeginTimestamp -= 1;
            }

            req.Result = true;
            req.Finished = true;
        }

        internal override void Visit(GetVersionListRequest req)
        {
            if (!this.dict.ContainsKey(req.RecordKey))
            {
                req.Result = null;
                req.Finished = true;
                return;
            }

            VersionEntry tailPointer = this.dict[req.RecordKey][VersionEntry.VERSION_KEY_STRAT_INDEX];
            long lastVersionKey = tailPointer.BeginTimestamp;

            List<VersionEntry> localList = req.Container != null ? req.Container : new List<VersionEntry>(2);
            // Only returns top 2 newest versions. This is enough for serializability. 
            // For other isolation levels, more versions may need to be returned.
            // When old versions may be truncated, it is desirable to maintain a head pointer as well,
            // so as to increase the lower bound of version keys and reduce the number of iterations. 
            while (lastVersionKey >= 0 && localList.Count <= 2)
            {
                // To make it run under .Net 4.5
                VersionEntry verEntry = null;
                this.dict[req.RecordKey].TryGetValue(lastVersionKey, out verEntry);

                if (verEntry != null)
                {
                    localList.Add(verEntry);
                }

                lastVersionKey--;
            }

            req.Result = localList;
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
                req.Result = null;
                req.Finished = true;
                return;
            }

            Dictionary<long, VersionEntry> versionList = this.dict[req.RecordKey];
            VersionEntry tailEntry = versionList[VersionEntry.VERSION_KEY_STRAT_INDEX];
            if (tailEntry == null)
            {
                throw new TransactionException("The tail pointer is missing from the version list.");
            }
            long lastVersionKey = tailEntry.BeginTimestamp;

            List<VersionEntry> localList = new List<VersionEntry>(2);
            // Only returns top 2 newest versions. This is enough for serializability. 
            // For other isolation levels, more versions may need to be returned.
            // When old versions may be truncated, it is desirable to maintain a head pointer as well,
            // so as to increase the lower bound of version keys and reduce the number of iterations. 
            while (lastVersionKey >= 0 && localList.Count <= 2)
            {
                VersionEntry verEntry = null;
                this.dict[req.RecordKey].TryGetValue(lastVersionKey, out verEntry);

                if (verEntry != null)
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
            if (this.dict.ContainsKey(req.RecordKey) && 
                this.dict[req.RecordKey].ContainsKey(req.VersionKey))
            {
                req.Result = this.dict[req.RecordKey][req.VersionKey];
            }
            else
            {
                req.Result = null;
            }
            req.Finished = true;
        }

        internal override void Visit(ReplaceVersionRequest req)
        {
            if (!this.dict.ContainsKey(req.RecordKey) || 
                !this.dict[req.RecordKey].ContainsKey(req.VersionKey))
            {
                throw new TransactionException("The specified version does not exist.");
            }

            VersionEntry verEntry = this.dict[req.RecordKey][req.VersionKey];
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
            if (!this.dict.ContainsKey(req.RecordKey) ||
                !this.dict[req.RecordKey].ContainsKey(req.VersionKey))
            {
                throw new TransactionException("The specified version does not exist.");
            }

            this.dict[req.RecordKey][req.VersionKey] = req.VersionEntry;
            req.Result = true;
            req.Finished = true;
        }

        internal override void Visit(UpdateVersionMaxCommitTsRequest req)
        {
            if (!this.dict.ContainsKey(req.RecordKey) ||
                !this.dict[req.RecordKey].ContainsKey(req.VersionKey))
            {
                throw new TransactionException("The specified version does not exist.");
            }

            VersionEntry version = this.dict[req.RecordKey][req.VersionKey];
            // Only update the max commit time when uploaded commit time is larger than the version's
            version.MaxCommitTs = Math.Max(version.MaxCommitTs, req.MaxCommitTs);
            
            req.Result = version;
            req.Finished = true;
        }

        internal override void Visit(UploadVersionRequest req)
        {
            if (!this.dict.ContainsKey(req.RecordKey))
            {
                throw new TransactionException("The specified record does not exist.");
            }

            Dictionary<long, VersionEntry> versionList = this.dict[req.RecordKey];
            if (versionList.ContainsKey(req.VersionKey))
            {
                req.Result = false;  
            }
            else
            {
                versionList.Add(req.VersionKey, req.VersionEntry);
                // Take the dirty version entry to store the current largest version key
                VersionEntry tailEntry = this.dict[req.RecordKey][VersionEntry.VERSION_KEY_STRAT_INDEX];
                tailEntry.BeginTimestamp = req.VersionKey;

                req.Result = true;
            }
            req.Finished = true;
        }
    }
}
