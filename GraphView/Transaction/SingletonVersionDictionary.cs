
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using GraphView.RecordRuntime;
    using Newtonsoft.Json.Linq;



    internal abstract class SingletonVersionTable : VersionTable, IVersionedTableStore
    {
        public JObject GetJson(object key, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public IList<JObject> GetRangeJsons(object lowerKey, object upperKey, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public IList<object> GetRangeRecordKeyList(object lowerValue, object upperValue, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public IList<object> GetRecordKeyList(object value, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public bool InsertJson(object key, JObject record, Transaction tx)
        {
            throw new NotImplementedException();
        }
    }

    /// <summary>
    /// A version table implementation in single machine environment.
    /// Use a Dictionary.
    /// </summary>
    internal partial class SingletonVersionDictionary : SingletonVersionTable
    {
        private readonly Dictionary<object, List<VersionEntry>> dict;
        private readonly object listLock;

        public SingletonVersionDictionary(string tableId)
        {
            this.dict = new Dictionary<object, List<VersionEntry>>();
            this.listLock = new object();
            this.TableId = tableId;
        }

        /// <summary>
        /// Given a version and a read timestamp (or a TxId), check this version's visibility.
        /// </summary>
        internal bool CheckVersionVisibility(VersionEntry version, long readTimestamp)
        {
            //case 1: both begin and end fields are timestamp
            //just checl whether readTimestamp is in the interval of the version's beginTimestamp and endTimestamp 
            if (!version.IsBeginTxId && !version.IsEndTxId)
            {
                return readTimestamp > version.BeginTimestamp && readTimestamp < version.EndTimestamp;
            }
            //case 2: begin field is a TxId, end field is a timestamp
            //just check whether this version is created by the same transaction
            else if (version.IsBeginTxId && !version.IsEndTxId)
            {
                return version.BeginTimestamp == readTimestamp;
            }
            //case 3: begin field is a TxId, end field is a TxId
            //this must must be deleted by the same transaction, not visible
            else if (version.IsBeginTxId && version.IsEndTxId)
            {
                return false;
            }
            //case 4: begin field is a timestamp, end field is a TxId
            //first check whether the readTimestamp > version's beginTimestamp
            //then, check the version's end field
            else
            {
                if (readTimestamp > version.BeginTimestamp)
                {
                    //this is the old version deleted by the same Transaction
                    if (version.EndTimestamp == readTimestamp)
                    {
                        return false;
                    }
                    //other transaction can see this old version
                    else
                    {
                        return true;
                    }
                }
                else
                {
                    return false;
                }
            }
        }

        /// <summary>
        /// Given the recordKey and the readTimestamp, this method will first scan a list to check each version's visibility.
        /// If it finds the legal version, it will return the version, or, it will return null.
        /// </summary>
        internal override VersionEntry ReadVersion(object recordKey, long readTimestamp)
        {
            if (!this.dict.ContainsKey(recordKey))
            {
                return null;
            }

            foreach (VersionEntry version in this.dict[recordKey])
            {
                if (this.CheckVersionVisibility(version, readTimestamp))
                {
                    return version;
                }
            }

            return null;
        }

        /// <summary>
        /// Given the recordKey, the record to insert, the transactionId, and the readTimestamp,
        /// this method will first tranverse the version list to check each version's visibility.
        /// If no version is visible, create and insert a new version to the dictionary and return true, or, it will return false.
        /// </summary>
        internal override bool InsertVersion(object recordKey, JObject record, long txId, long readTimestamp)
        {
            //the version list does not exist, create a new list
            if (!this.dict.ContainsKey(recordKey))
            {
                lock (this.listLock)
                {
                    if (!this.dict.ContainsKey(recordKey))
                    {
                        this.dict[recordKey] = new List<VersionEntry>();
                    }
                }
            }
            //tranverse the list to check each version's visibility
            foreach (VersionEntry version in this.dict[recordKey])
            {
                if (this.CheckVersionVisibility(version, readTimestamp))
                {
                    return false;
                }
            }
            //no version is visible, insert the new version
            this.dict[recordKey].Add(new VersionEntry(true, txId, false, long.MaxValue, record));
            return true;
        }

        /// <summary>
        /// Update a version.
        /// Find the version, atomically change it to old version, and insert a new version.
        /// </summary>
        internal override bool UpdateVersion(
            object recordKey,
            JObject record, 
            long txId, 
            long readTimestamp, 
            out VersionEntry oldVersion, 
            out VersionEntry newVersion)
        {
            //can not find any versions with the given versionKey, can not perform update, return false
            if (!this.dict.ContainsKey(recordKey))
            {
                oldVersion = null;
                newVersion = null;
                return false;
            }
            //tranverse the version list, try to find the updatable version
            foreach (VersionEntry version in this.dict[recordKey])
            {
                //first check the version's visibility
                if (!this.CheckVersionVisibility(version, readTimestamp))
                {
                    continue;
                }
                //check the version's updatability
                if (!version.IsEndTxId && version.EndTimestamp == long.MaxValue)
                {
                    //if the versiin's begin field is a timestamp, ATOMICALLY set the version's end timestamp to TxId,
                    //creat and insert a new version
                    if (!version.IsBeginTxId)
                    {
                        if (long.MaxValue != Interlocked.CompareExchange(ref version.endTimestamp, txId, long.MaxValue))
                        {
                            //other transaction has already set the version's end field
                            oldVersion = version;
                            newVersion = null;
                            return false;
                        }
                        //ATOMICALLY change the old version's end field successfully
                        version.IsEndTxId = true;
                        oldVersion = version;
                        //creat a new version and add it to the versionTable.
                        newVersion = new VersionEntry(true, txId, false, long.MaxValue, record);
                        this.dict[recordKey].Add(newVersion);
                        return true;
                    }
                    //if the version's begin field is a TxId, change the record directly on this version
                    else
                    {
                        oldVersion = null;
                        version.Record = record;
                        newVersion = version;
                        return true;
                    }
                }
                //a version is visible but not updatable, can not perform update, return false
                else
                {
                    oldVersion = null;
                    newVersion = null;
                    return false;
                }
            }
            //can not find the legal version to perform update
            oldVersion = null;
            newVersion = null;
            return false;
        }

        /// <summary>
        /// Find and delete a version.
        /// </summary>
        internal override bool DeleteVersion(
            object recordKey, 
            long txId, 
            long readTimestamp, 
            out VersionEntry deletedVersion)
        {
            //can not find any versions with the given versionKey, can not perform delete, return false
            if (!this.dict.ContainsKey(recordKey))
            {
                deletedVersion = null;
                return false;
            }
            //tranverse the version list, try to find the deletable version
            foreach (VersionEntry version in this.dict[recordKey])
            {
                //first check the version's visibility
                if (!this.CheckVersionVisibility(version, readTimestamp))
                {
                    continue;
                }
                //check the version's updatability
                if (!version.IsEndTxId && version.EndTimestamp == long.MaxValue)
                {
                    //if the versiin's begin field is a timestamp, ATOMICALLY set the version's end timestamp to TxId
                    if (!version.IsBeginTxId)
                    {
                        if (long.MaxValue != Interlocked.CompareExchange(ref version.endTimestamp, txId, long.MaxValue))
                        {
                            //other transaction has already set the version's end field
                            deletedVersion = version;
                            return false;
                        }
                        //ATOMICALLY change the old version's end field successfully
                        //change the version's IsEndTxId to true
                        version.IsEndTxId = true;
                        deletedVersion = version;
                        return true;
                    }
                    //if the version's begin field is a TxId, set the version's end timestamp to TxId directly
                    else
                    {
                        deletedVersion = version;
                        return true;
                    }

                }
                //a version is visible but not deletable, can not perform delete, return false
                else
                {
                    deletedVersion = null;
                    return false;
                }
            }
            //can not find the legal version to perform delete
            deletedVersion = null;
            return false;
        }

        /// <summary>
        /// Check visibility of the version read before, used in validation phase.
        /// Given a record's recordKey, the version's beginTimestamp, the current readTimestamp, and the transaction Id
        /// First find the version then check whether it is still visible. 
        /// </summary>
        internal override bool CheckReadVisibility(object recordKey, long readVersionBeginTimestamp, long readTimestamp, long txId)
        {
            if (!this.dict.ContainsKey(recordKey))
            {
                return false;
            }
            foreach (VersionEntry version in this.dict[recordKey])
            {
                //case 1: the versin's begin field is a TxId, and this Id equals to the transaction's Id
                if (version.IsBeginTxId && version.BeginTimestamp == txId)
                {
                    //check the visibility of this version, using the transaction's endTimestamp
                    if (this.CheckVersionVisibility(version, readTimestamp))
                    {
                        return true;
                    }
                    else
                    {
                        continue;
                    }
                }
                //case 2: the version's begin field is a timestamp, and this timestamp equals to the read version's begin timestamp
                else if (!version.IsBeginTxId && version.BeginTimestamp == readVersionBeginTimestamp)
                {
                    return this.CheckVersionVisibility(version, readTimestamp);
                }
            }
            return false;
        }

        /// <summary>
        /// Check for Phantom of a scan.
        /// Look for versions that came into existence during T’s lifetime and are visible as of the end of the transaction.
        /// </summary>
        internal override bool CheckPhantom(object recordKey, long oldScanTime, long newScanTime)
        {
            if (!this.dict.ContainsKey(recordKey))
            {
                return true;
            }
            foreach (VersionEntry version in this.dict[recordKey])
            {
                if (!version.IsBeginTxId)
                {
                    if (version.BeginTimestamp > oldScanTime && version.BeginTimestamp < newScanTime)
                    {
                        if (this.CheckVersionVisibility(version, newScanTime))
                        {
                            return false;
                        }
                    }
                }
            }
            return true;
        }

        /// <summary>
        /// After a transaction T commit,
        /// propagate a T's end timestamp to the Begin and End fields of new and old versions
        /// </summary>
        internal override void UpdateCommittedVersionTimestamp(object recordKey, long txId, long endTimestamp)
        {
            if (!this.dict.ContainsKey(recordKey))
            {
                return;
            }
            //tranverse the whole version list and seach for versions that were modified by the transaction
            foreach (VersionEntry version in this.dict[recordKey])
            {
                if (version.IsBeginTxId && version.BeginTimestamp == txId)
                {
                    version.BeginTimestamp = endTimestamp;
                    version.IsBeginTxId = false;
                }
                if (version.IsEndTxId && version.EndTimestamp == txId)
                {
                    version.EndTimestamp = endTimestamp;
                    version.IsEndTxId = false;
                }
            }
        }

        /// <summary>
        /// After a transaction T abort,
        /// T sets the Begin field of its new versions to infinity, thereby making them invisible to all transactions,
        /// and reset the End fields of its old versions to infinity.
        /// </summary>
        internal override void UpdateAbortedVersionTimestamp(object recordKey, long txId)
        {
            if (!this.dict.ContainsKey(recordKey))
            {
                return;
            }
            //tranverse the whole version list and seach for versions that were modified by the transaction
            foreach (VersionEntry version in this.dict[recordKey])
            {
                //new version
                if (version.IsBeginTxId && version.BeginTimestamp == txId)
                {
                    version.BeginTimestamp = long.MaxValue;
                    version.IsBeginTxId = false;
                }
                //old version
                else if (version.IsEndTxId && version.EndTimestamp == txId)
                {
                    version.EndTimestamp = long.MaxValue;
                    version.IsEndTxId = false;
                }
            }
        }
    }

    internal partial class SingletonVersionDictionary : IVersionedTableStore
    {
        public new JObject GetJson(object key, Transaction tx)
        {
            VersionEntry versionEntry = this.GetVersionEntry(key, tx.BeginTimestamp);
            if (versionEntry != null)
            {
                tx.AddReadSet(this.TableId, key, versionEntry.BeginTimestamp);
                return (JObject)versionEntry.Record;
            }

            return null;
        }

        public new IList<JObject> GetRangeJsons(object lowerKey, object upperKey, Transaction tx)
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
                    throw new ArgumentException("recordKey must be comparable");
                }

                if (lowerComparableKey.CompareTo(comparableKey) <= 0 
                    && upperComparebleKey.CompareTo(comparableKey) >= 0)
                {
                    VersionEntry versionEntry = this.GetVersionEntry(key, tx.BeginTimestamp);
                    if (versionEntry == null)
                    {
                        // false means no visiable version for this key
                        tx.AddScanSet(this.TableId, key, tx.BeginTimestamp, false);
                    }
                    else
                    {
                        jObjectValues.Add((JObject)versionEntry.Record);
                        // true means we found a visiable version for this key
                        tx.AddScanSet(this.TableId, key, tx.BeginTimestamp, true);
                        tx.AddReadSet(this.TableId, key, tx.BeginTimestamp);
                    }
                }
            }

            return jObjectValues;
        }

        public new IList<object> GetRangeRecordKeyList(object lowerValue, object upperValue, Transaction tx)
        {
            List<object> keyList = new List<object>();
            IComparable lowerComparableValue = lowerValue as IComparable;
            IComparable upperComparableValue = upperValue as IComparable;

            if (lowerComparableValue == null || upperComparableValue == null)
            {
                throw new ArgumentException("lowerValue and upperValue must be comparable");
            }

            foreach (var key in this.dict.Keys)
            {
                VersionEntry versionEntry = this.GetVersionEntry(key, tx.BeginTimestamp);
                if (versionEntry != null)
                {
                    IComparable comparableValue = versionEntry.Record as IComparable;
                    if (comparableValue == null)
                    {
                        throw new ArgumentException("record must be comparable");
                    }

                    if (lowerComparableValue.CompareTo(comparableValue) <= 0 
                        && upperComparableValue.CompareTo(comparableValue) >= 0)
                    {
                        keyList.Add(key);
                        tx.AddReadSet(this.TableId, key, tx.BeginTimestamp);
                        tx.AddScanSet(this.TableId, keyList, tx.BeginTimestamp, true);
                    }
                }
            }

            return keyList;
        }

        public new IList<object> GetRecordKeyList(object value, Transaction tx)
        {
            List<object> keyList = new List<object>();
            foreach (var key in this.dict.Keys)
            {
                VersionEntry versionEntry = this.GetVersionEntry(key, tx.BeginTimestamp);
                if (versionEntry != null && versionEntry.Record.Equals(value))
                {
                    keyList.Add(key);
                    tx.AddReadSet(this.TableId, key, tx.BeginTimestamp);
                    // TODO: add to scanSet?
                    tx.AddScanSet(this.TableId, key, tx.BeginTimestamp, true);
                }
            }

            return keyList;
        }

        internal VersionEntry GetVersionEntry(object key, long readTimestamp)
        {
            if (!this.dict.ContainsKey(key))
            {
                return null;
            }

            foreach (VersionEntry versionEntry in this.dict[key])
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
