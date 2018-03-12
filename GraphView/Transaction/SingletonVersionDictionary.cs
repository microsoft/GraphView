namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using GraphView.RecordRuntime;
    using Newtonsoft.Json.Linq;

    /// <summary>
    /// A version table implementation in single machine environment.
    /// Use a Dictionary.
    /// </summary>
    internal partial class SingletonVersionDictionary : SingletonVersionTable
    {
        private readonly Dictionary<object, VersionList> dict;
        private readonly object listLock;

        public SingletonVersionDictionary(string tableId)
            : base(tableId)
        {
            this.dict = new Dictionary<object, VersionList>();
            this.listLock = new object();
        }

        internal override IEnumerable<VersionEntry> GetVersionList(object recordKey)
        {
            if (!this.dict.ContainsKey(recordKey))
            {
                return null;
            }

            List<VersionEntry> versionList = new List<VersionEntry>();
            while (true)
            {
                VersionNode current = this.dict[recordKey].Head;
                versionList.Clear();
                do
                {
                    if (current.NextNode == null)
                    {
                        //arrive at the end of the list, return the version list
                        return versionList;
                    }

                    if ((current.State & 0x0F).Equals(0x0F))
                    {
                        //if current node is being deleted, rescan the list from head.
                        break;
                    }

                    versionList.Add(current.VersionEntry);
                    current = current.NextNode;
                } while (true);
            }
        }

        internal override bool InsertAndUploadVersion(object recordKey, VersionEntry version)
        {
            //the version list does not exist, create a new list
            if (!this.dict.ContainsKey(recordKey))
            {
                lock (this.listLock)
                {
                    if (!this.dict.ContainsKey(recordKey))
                    {
                        this.dict[recordKey] = new VersionList();
                    }
                }
            }

            this.dict[recordKey].PushFront(version);
            return true;
        }

        internal override bool UpdateAndUploadVersion(object recordKey, long versionKey, VersionEntry toBeChangedVersion, VersionEntry newVersion)
        {
            return this.dict[recordKey].ChangeNodeValue(recordKey, versionKey, toBeChangedVersion, newVersion);
        }

        internal override bool DeleteVersionEntry(object recordKey, long versionKey)
        {
            this.dict[recordKey].DeleteNode(recordKey, versionKey);
            return true;
        }
    }

    internal partial class SingletonVersionDictionary
    {
        /// <summary>
        /// Read a record from the given recordKey
        /// return null if not found
        /// </summary>
        public override JObject GetJson(object key, Transaction tx)
        {
            VersionEntry versionEntry = this.GetVersionEntry(key, tx.BeginTimestamp);
            if (versionEntry != null)
            {
                tx.AddReadSet(this.tableId, key, versionEntry.BeginTimestamp);
                tx.AddScanSet(this.tableId, key, tx.BeginTimestamp, true);
                return versionEntry.JsonRecord;
            }

            return null;
        }

        /// <summary>
        /// Read a list of records where their keys are between lowerKey and upperKey
        /// </summary>
        public override IList<JObject> GetRangeJsons(object lowerKey, object upperKey, Transaction tx)
        {
            List<JObject> jObjectValues = new List<JObject>();
            IComparable lowerComparableKey = lowerKey as IComparable;
            IComparable upperComparebleKey = upperKey as IComparable;

            if (lowerComparableKey == null || upperComparebleKey == null)
            {
                throw new ArgumentException("lowerKey and upperKey must be comparable");
            }

            foreach (var key in this.dict.Keys)
            {
                IComparable comparableKey = key as IComparable;
                if (comparableKey == null)
                {
                    throw new RecordServiceException("recordKey must be comparable");
                }

                if (lowerComparableKey.CompareTo(comparableKey) <= 0 
                    && upperComparebleKey.CompareTo(comparableKey) >= 0)
                {
                    VersionEntry versionEntry = this.GetVersionEntry(key, tx.BeginTimestamp);
                    if (versionEntry == null)
                    {
                        // false means no visiable version for this key
                        tx.AddScanSet(this.tableId, key, tx.BeginTimestamp, false);
                    }
                    else
                    {
                        jObjectValues.Add(versionEntry.JsonRecord);
                        // true means we found a visiable version for this key
                        tx.AddScanSet(this.tableId, key, tx.BeginTimestamp, true);
                        tx.AddReadSet(this.tableId, key, versionEntry.BeginTimestamp);
                    }
                }
            }

            return jObjectValues;
        }

        /// <summary>
        /// Get the union set of keys list for every value between lowerValue and uppperValue in index-table
        /// index format: value => [key1, key2, ...]
        /// </summary>
        public override IList<object> GetRangeRecordKeyList(object lowerValue, object upperValue, Transaction tx)
        {
            HashSet<object> keyHashset = new HashSet<object>();
            IComparable lowerComparableValue = lowerValue as IComparable;
            IComparable upperComparableValue = upperValue as IComparable;

            if (lowerComparableValue == null || upperComparableValue == null)
            {
                throw new ArgumentException("lowerValue and upperValue must be comparable");
            }

            foreach (var key in this.dict.Keys)
            {
                IComparable comparableKey = key as IComparable;
                if (comparableKey == null)
                {
                    throw new ArgumentException("recordKey must be comparable");
                }

                if (lowerComparableValue.CompareTo(comparableKey) <= 0
                    && upperComparableValue.CompareTo(comparableKey) >= 0)
                {
                    VersionEntry versionEntry = this.GetVersionEntry(key, tx.BeginTimestamp);
                    if (versionEntry == null)
                    {
                        // false means no visiable version for this key
                        tx.AddScanSet(this.tableId, key, tx.BeginTimestamp, false);
                    }
                    else
                    {
                        JObject record = versionEntry.JsonRecord;
                        IndexValue indexValue = record.ToObject<IndexValue>();
                        if (indexValue == null)
                        {
                            throw new RecordServiceException(@"wrong format of index, should be 
                                {""keys"":[""key1"", ""key2"", ...]}");
                        }

                        keyHashset.UnionWith(indexValue.Keys);
                        // true means we found a visiable version for this key
                        tx.AddScanSet(this.tableId, key, tx.BeginTimestamp, true);
                        tx.AddReadSet(this.tableId, key, versionEntry.BeginTimestamp);
                    }
                }
            }

            return keyHashset.ToList<object>();
        }

        /// <summary>
        ///  This method will return all keys for a value in a index-table
        /// index format: value => [key1, key2, ...]
        /// </summary>
        public override IList<object> GetRecordKeyList(object value, Transaction tx)
        {
            VersionEntry versionEntry = this.GetVersionEntry(value, tx.BeginTimestamp);
            if (versionEntry != null)
            {
                JObject record = versionEntry.JsonRecord;
                IndexValue indexValue = record.ToObject<IndexValue>();
                if (indexValue == null)
                {
                    throw new RecordServiceException(@"wrong format of index, should be 
                                {""keys"":[""key1"", ""key2"", ...]}");
                }

                tx.AddReadSet(this.tableId, value, versionEntry.BeginTimestamp);
                tx.AddScanSet(this.tableId, value, tx.BeginTimestamp, true);

                return indexValue.Keys;
            }

            return null;
        }

        public override bool InsertJson(object key, JObject record, Transaction tx)
        {
            VersionEntry versionEntry = this.GetVersionEntry(key, tx.BeginTimestamp);
            
            // insert the version if we have not found a visiable version
            if (versionEntry != null)
            {
                return false;
            }

            bool hasInserted = this.InsertVersion(key, record, tx.TxId, tx.BeginTimestamp);
            if (hasInserted)
            {
                tx.AddScanSet(this.tableId, key, tx.BeginTimestamp, false);
                tx.AddWriteSet(this.tableId, key, tx.TxId, false);
            }
            return hasInserted;
        }

        public override bool UpdateJson(object key, JObject record, Transaction tx)
        {
            VersionEntry versionEntry = this.GetVersionEntry(key, tx.BeginTimestamp);
            if (versionEntry == null)
            {
                return false;
            }

            VersionEntry oldVersionEntry = null, newVersionEntry = null;
            bool hasUpdated = this.UpdateVersion(key, record, tx.TxId, tx.BeginTimestamp, 
                out oldVersionEntry, out newVersionEntry);
            if (hasUpdated)
            {
                // pass the old version' begin timestamp to find old version
                tx.AddWriteSet(this.tableId, key, oldVersionEntry.BeginTimestamp, true);
                // pass the new version's begin timestamp to find the new version
                tx.AddWriteSet(this.tableId, key, tx.TxId, false);
            }
            return hasUpdated;
        }

        public override bool DeleteJson(object key, Transaction tx)
        {
            VersionEntry versionEntry = this.GetVersionEntry(key, tx.BeginTimestamp);
            if (versionEntry == null)
            {
                return false;
            }

            VersionEntry deletedVersionEntry = null;
            bool hasDeleted = this.DeleteVersion(key, tx.TxId, tx.BeginTimestamp, out deletedVersionEntry);
            if (hasDeleted)
            {
                // pass the old version's begin timestamp to find the old version
                tx.AddWriteSet(this.tableId, key, versionEntry.BeginTimestamp, true);
            }
            return hasDeleted;
        }
        
        /// <summary>
        /// Get a visiable version entry from versionTable with the
        /// </summary>
        internal VersionEntry GetVersionEntry(object key, long readTimestamp)
        {
            IEnumerable<VersionEntry> versionEntryList = this.GetVersionList(key);
            if (versionEntryList == null)
            {
                return null;
            }

            foreach (VersionEntry versionEntry in versionEntryList)
            {
                if (this.CheckVersionVisibility(versionEntry, readTimestamp))
                {
                    return versionEntry;
                }
            }

            return null;
        }
    }
}
