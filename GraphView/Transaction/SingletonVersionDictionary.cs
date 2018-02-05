
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
    }

    /// <summary>
    /// A version table implementation in single machine environment.
    /// Use a Dictionary.
    /// </summary>
    internal partial class SingletonVersionDictionary : SingletonVersionTable
    {
        private readonly Dictionary<object, List<VersionEntry>> dict;

        public SingletonVersionDictionary()
        {
            this.dict = new Dictionary<object, List<VersionEntry>>();
        }

        /// <summary>
        /// Given the recordKey and the readTimestamp, this method will first scan a list to check each version's visibility.
        /// If it finds the legal version, it will return the version, or, it will return null.
        /// </summary>
        internal override VersionEntry GetVersion(object recordKey, long readTimestamp)
        {
            if (!this.dict.ContainsKey(recordKey))
            {
                return null;
            }

            foreach (VersionEntry version in this.dict[recordKey])
            {
                // case 1: both the version's begin and the end fields are timestamp.
                // The version is visibly only if the read time is between its begin and end timestamp. 
                if (!version.IsBeginTxId && !version.IsEndTxId)
                {
                    if (readTimestamp > version.BeginTimestamp && readTimestamp < version.EndTimestamp)
                    {
                        return version;
                    }
                }
                // case 2: the version's begin field is TxId.
                // The version is visible only if this version is created by the same transaction.
                else if (version.IsBeginTxId)
                {
                    if (readTimestamp == version.BeginTimestamp)
                    {
                        return version;
                    }
                }
                // case 3: only the version's end field is TxId.
                // The version is visible only if its begin timestamp is smaller than the read time.
                else
                {
                    if (readTimestamp >= version.BeginTimestamp)
                    {
                        return version;
                    }
                }
            }

            return null;
        }

        /// <summary>
        /// Given the recordKey, the record to insert, the transactionId, and the readTimestamp,
        /// this method will first invoke GetVersion() to check whether the record already exists.
        /// If the record does not exist, create and insert a new version to the dictionary and return true, or, it will return false.
        /// </summary>
        internal override bool InsertVersion(object recordKey, JObject record, long txId, long readTimestamp)
        {
            if (this.GetVersion(recordKey, readTimestamp) == null)
            {
                //insert
                if (!this.dict.ContainsKey(recordKey))
                {
                    this.dict[recordKey] = new List<VersionEntry>();
                }
                this.dict[recordKey].Add(new VersionEntry(true, txId, false, long.MaxValue, record));
                return true;
            }

            return false;
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
            if (!this.dict.ContainsKey(recordKey))
            {
                //can not find any versions with the given versionKey
                oldVersion = null;
                newVersion = null;
                return false;
            }
            foreach (VersionEntry version in this.dict[recordKey])
            {
                //A visible version is updatable if its end field equals infinity
                if (!version.IsEndTxId && version.EndTimestamp == long.MaxValue && !version.IsBeginTxId && readTimestamp > version.BeginTimestamp)
                {
                    //the version is visible and updatable
                    //Atomically set the version's end field to the transactionId
                    if (long.MaxValue != Interlocked.CompareExchange(ref version.endTimestamp, txId, long.MaxValue))
                    {
                        //other transaction has already set the version's end field
                        oldVersion = version;
                        newVersion = null;
                        return false;
                    }
                    //change the old version's end field successfully
                    version.IsEndTxId = true;
                    oldVersion = version;
                    //creat a new version and add it to the versionTable.
                    newVersion = new VersionEntry(true, txId, false, long.MaxValue, record);
                    this.dict[recordKey].Add(newVersion);
                    return true;
                }
                //If this is already a new version created by the same transaction, just update on this new version.
                else if (!version.IsEndTxId && version.EndTimestamp == long.MaxValue && version.IsBeginTxId && version.BeginTimestamp == txId)
                {
                    oldVersion = null;
                    version.Record = record;
                    newVersion = version;
                    return true;
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
            if (!this.dict.ContainsKey(recordKey))
            {
                //can not find any versions with the given versionKey
                deletedVersion = null;
                return true;
            }

            foreach (VersionEntry version in this.dict[recordKey])
            {
                if (!version.IsEndTxId && version.EndTimestamp == long.MaxValue && readTimestamp >= version.BeginTimestamp)
                {
                    //the version is visible and updatable
                    //Atomically set the version's end field to the transactionId
                    if (long.MaxValue != Interlocked.CompareExchange(ref version.endTimestamp, txId, long.MaxValue))
                    {
                        //other transaction has already set the version's end field
                        deletedVersion = null;
                        return false;
                    }
                    //change the version's IsEndTxId to true
                    version.IsEndTxId = true;
                    deletedVersion = version;
                    return true;
                }
            }
            //can not find the legal version to perform delete
            deletedVersion = null;
            return true;
        }

        /// <summary>
        /// Check visibility of the version read before, used in validation phase.
        /// Given a record's recordKey, the version's beginTimestamp, and the readTimestamp,
        /// First find the version then check whether it is still visible. 
        /// </summary>
        internal override bool CheckVersionVisibility(object recordKey, long readVersionBeginTimestamp, long readTimestamp)
        {
            if (!this.dict.ContainsKey(recordKey))
            {
                return false;
            }
            foreach (VersionEntry version in this.dict[recordKey])
            {
                if (version.BeginTimestamp == readVersionBeginTimestamp)
                {
                    if (!version.IsBeginTxId && !version.IsEndTxId)
                    {
                        return readTimestamp < version.EndTimestamp;
                    }
                    else
                    {
                        return true;
                    }
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
                //test whether the version's beginTimestamp is between two scan.
                if (!version.IsBeginTxId && version.BeginTimestamp > oldScanTime && version.BeginTimestamp < newScanTime)
                {
                    //case 1: the version's end field contains TxId
                    //visible
                    if (version.IsEndTxId)
                    {
                        return false;
                    }
                    //case 2:
                    else if (version.EndTimestamp > newScanTime)
                    {
                        return false;
                    }
                }
            }
            return true;
        }

        /// <summary>
        /// After a transaction T commit,
        /// propagate a T's end timestamp to the Begin and End fields of new and old versions
        /// </summary>
        internal override void UpdateCommittedVersionTimestamp(object recordKey, long txId, long endTimestamp, bool isOld)
        {
            if (!this.dict.ContainsKey(recordKey))
            {
                return;
            }
            foreach (VersionEntry version in this.dict[recordKey])
            {
                //old version
                if (version.IsEndTxId && version.EndTimestamp == txId && isOld)
                {
                    version.EndTimestamp = endTimestamp;
                    version.IsEndTxId = false;
                    return;
                }
                //new version
                else if (version.IsBeginTxId && version.BeginTimestamp == txId && !isOld)
                {
                    version.BeginTimestamp = endTimestamp;
                    version.IsBeginTxId = false;
                    return;
                }
            }
        }

        /// <summary>
        /// After a transaction T abort,
        /// T sets the Begin field of its new versions to infinity, thereby making them invisible to all transactions,
        /// and reset the End fields of its old versions to infinity.
        /// </summary>
        internal override void UpdateAbortedVersionTimestamp(object recordId, long txId, bool isOld)
        {
            if (!this.dict.ContainsKey(recordId))
            {
                return;
            }
            foreach (VersionEntry version in this.dict[recordId])
            {
                //new version
                if (version.IsBeginTxId && version.BeginTimestamp == txId && !isOld)
                {
                    version.BeginTimestamp = long.MaxValue;
                    version.IsBeginTxId = false;
                    return;
                }
                //old version
                else if (version.IsEndTxId && version.EndTimestamp == txId && isOld)
                {
                    version.EndTimestamp = long.MaxValue;
                    version.IsEndTxId = false;
                    return;
                }
            }
        }
    }

    internal partial class SingletonVersionDictionary : IVersionedTableStore
    {
        public new JObject GetJson(object key, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public new IList<JObject> GetRangeJsons(object lowerKey, object upperKey, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public new IList<object> GetRangeRecordKeyList(object lowerValue, object upperValue, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public new IList<object> GetRecordKeyList(object value, Transaction tx)
        {
            throw new NotImplementedException();
        }
    }
}
