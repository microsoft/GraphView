using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Runtime.Remoting.Messaging;
using System.Threading;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Newtonsoft.Json.Linq;

namespace GraphView.Transaction
{
    public class VersionEntry
    {
        private bool isBeginTxId;
        public long beginTimestamp;
        private bool isEndTxId;
        public long endTimestamp;
        private JObject record;

        public bool IsBeginTxId
        {
            get
            {
                return this.isBeginTxId;
            }
            set
            {
                this.isBeginTxId = value;
            }
        }

        public long BeginTimestamp
        {
            get
            {
                return this.beginTimestamp;
            }
            set
            {
                this.beginTimestamp = value;
            }
        }

        public bool IsEndTxId
        {
            get
            {
                return this.isEndTxId;
            }
            set
            {
                this.isEndTxId = value;
            }
        }

        public long EndTimestamp
        {
            get
            {
                return this.endTimestamp;
            }
            set
            {
                this.endTimestamp = value;
            }
        }

        public JObject Record
        {
            get
            {
                return this.record;
            }
            set
            {
                this.record = value;
            }
        }

        public VersionEntry(bool isBeginTxId, long beginTimestamp, bool isEndTxId, long endTimestamp, JObject jObject)
        {
            this.isBeginTxId = isBeginTxId;
            this.beginTimestamp = beginTimestamp;
            this.isEndTxId = isEndTxId;
            this.endTimestamp = endTimestamp;
            this.record = jObject;
        }
    }

    /// <summary>
    /// A version table for concurrency control.
    /// </summary>
    public interface IVersionTable
    {
        VersionEntry GetVersion(VersionKey versionKey, long readTimestamp);
        bool InsertVersion(VersionKey versionKey, JObject record, long txId, long readTimestamp);
        bool DeleteVersion(VersionKey versionKey, long txId, long readTimestamp, out VersionEntry deletedVersion);
        bool UpdateVersion(VersionKey versionKey, JObject record, long txId, long readTimestamp, out VersionEntry oldVersion, out VersionEntry newVersion);
        bool CheckVersionVisibility(VersionKey readVersionKey, long readVersionBeginTimestamp, long readTimestamp);
        bool CheckPhantom(VersionKey scanVersionKey, long oldScanTime, long newScanTime);
        bool UpdateCommittedVersionTimestamp(VersionKey writeVersionKey, long txId, long endTimestamp, bool isOld);
        void UpdateAbortedVersionTimestamp(VersionKey writeVersionKey, long txId, bool isOld);
    }

    /// <summary>
    /// A singleton version table implementation in single machine environment.
    /// Use a Dictionary.
    /// </summary>
    public class SingletonVersionDictionary : IVersionTable
    {
        private static volatile SingletonVersionDictionary instance;
        private static readonly object initlock = new object();
        private Dictionary<VersionKey, List<VersionEntry>> dict;

        private SingletonVersionDictionary()
        {
            this.dict = new Dictionary<VersionKey, List<VersionEntry>>();
        }

        internal static SingletonVersionDictionary Instance
        {
            get
            {
                if (SingletonVersionDictionary.instance == null)
                {
                    lock (initlock)
                    {
                        if (SingletonVersionDictionary.instance == null)
                        {
                            SingletonVersionDictionary.instance = new SingletonVersionDictionary();
                        }
                    }
                }
                return SingletonVersionDictionary.instance;
            }
        }

        /// <summary>
        /// Given the versionKey and the readTimestamp, this method will first scan a list to check each version's visibility.
        /// If it finds the legal version, it will return the version, or, it will throw an exception.
        /// </summary>
        internal VersionEntry FindVersion(VersionKey versionKey, long readTimestamp)
        {
            if (!this.dict.ContainsKey(versionKey))
            {
                throw new KeyNotFoundException();
            }
            foreach (VersionEntry version in this.dict[versionKey])
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
            throw new ObjectNotFoundException();
        }

        /// <summary>
        /// Given the versionKey and the readTimestamp, this method will invoke FindVersion(), return the legal version or null. 
        /// </summary>
        public VersionEntry GetVersion(VersionKey versionKey, long readTimestamp)
        {
            VersionEntry version = null;
            try
            {
                version = this.FindVersion(versionKey, readTimestamp);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return null;
            }
            return version;
        }

        /// <summary>
        /// Given the versionKey, the record to insert, the transactionId, and the readTimestamp,
        /// this method will first invoke FindVersion() to check whether the record already exists.
        /// If the record does not exist, create and insert a new version to the dictionary and return true, or, it will return false.
        /// </summary>
        public bool InsertVersion(VersionKey versionKey, JObject record, long txId, long readTimestamp)
        {
            VersionEntry version = null;
            try
            {
                version = this.FindVersion(versionKey, readTimestamp);
            }
            catch (Exception e)
            {
                this.dict[versionKey].Add(new VersionEntry(true, txId, false, long.MaxValue, record));
                return true;
            }
            return false;
        }

        /// <summary>
        /// Update a version.
        /// Find the version, atomically change it to old version, and insert a new version.
        /// </summary>
        public bool UpdateVersion(VersionKey versionKey, JObject record, long txId, long readTimestamp, out VersionEntry oldVersion, out VersionEntry newVersion)
        {
            if (!this.dict.ContainsKey(versionKey))
            {
                //can not find any versions with the given versionKey
                oldVersion = null;
                newVersion = null;
                return false;
            }
            foreach (VersionEntry version in this.dict[versionKey])
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
                    this.dict[versionKey].Add(newVersion);
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
        public bool DeleteVersion(VersionKey versionKey, long txId, long readTimestamp, out VersionEntry deletedVersion)
        {
            if (!this.dict.ContainsKey(versionKey))
            {
                //can not find any versions with the given versionKey
                deletedVersion = null;
                return true;
            }
            foreach (VersionEntry version in this.dict[versionKey])
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
        /// Check visibility of the versions read, used in validation phase.
        /// Given a version's versionKey, its beginTimestamp, and the readTimestamp,
        /// First find the version then check whether it is still visible. 
        /// </summary>
        public bool CheckVersionVisibility(VersionKey readVersionKey, long readVersionBeginTimestamp, long readTimestamp)
        {
            if (!this.dict.ContainsKey(readVersionKey))
            {
                return false;
            }
            foreach (VersionEntry version in this.dict[readVersionKey])
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
        public bool CheckPhantom(VersionKey scanVersionKey, long oldScanTime, long newScanTime)
        {
            if (!this.dict.ContainsKey(scanVersionKey))
            {
                return true;
            }
            foreach (VersionEntry version in this.dict[scanVersionKey])
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
        /// Before a transaction T commit,
        /// propagate a T's end timestamp to the Begin and End fields of new and old versions
        /// </summary>
        public bool UpdateCommittedVersionTimestamp(VersionKey writeVersionKey, long txId, long endTimestamp, bool isOld)
        {
            if (!this.dict.ContainsKey(writeVersionKey))
            {
                return false;
            }
            foreach (VersionEntry version in this.dict[writeVersionKey])
            {
                //old version
                if (version.IsEndTxId && version.EndTimestamp == txId && isOld)
                {
                    version.EndTimestamp = endTimestamp;
                    version.IsEndTxId = false;
                }
                //new version
                else if (version.IsBeginTxId && version.BeginTimestamp == txId && !isOld)
                {
                    version.BeginTimestamp = endTimestamp;
                    version.IsBeginTxId = false;
                }
            }
            return true;
        }

        /// <summary>
        /// Before a transaction T abort,
        /// T sets the Begin field of its new versions to infinity, thereby making them invisible to all transactions,
        /// and reset the End fields of its old versions to infinity.
        /// </summary>
        public void UpdateAbortedVersionTimestamp(VersionKey writeVersionKey, long txId, bool isOld)
        {
            if (!this.dict.ContainsKey(writeVersionKey))
            {
                return;
            }
            foreach (VersionEntry version in this.dict[writeVersionKey])
            {
                //new version
                if (version.IsBeginTxId && version.BeginTimestamp == txId && !isOld)
                {
                    version.BeginTimestamp = long.MaxValue;
                    version.IsBeginTxId = false;
                }
                //old version
                else if (version.IsEndTxId && version.EndTimestamp == txId && isOld)
                {
                    version.EndTimestamp = long.MaxValue;
                    version.IsEndTxId = false;
                }
            }
        }
    }
}