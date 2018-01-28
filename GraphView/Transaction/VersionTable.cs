using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Runtime.Remoting.Messaging;
using Newtonsoft.Json.Linq;

namespace GraphView.Transaction
{
    public class VersionEntry
    {
        private bool isBeginTxId;
        private long beginTimestamp;
        private bool isEndTxId;
        private long endTimestamp;
        private readonly JObject record;

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
            List<VersionEntry> versions = new List<VersionEntry>();
            if (!dict.TryGetValue(versionKey, out versions))
            {
                throw new KeyNotFoundException();
            }
            foreach (VersionEntry version in versions)
            {
                // case 1: both the version's begin and the end fields are timestamp.
                // The version is visibly only if the read time is between its begin and end timestamp. 
                if (!version.IsBeginTxId && !version.IsEndTxId)
                {
                    if (readTimestamp >= version.BeginTimestamp && readTimestamp < version.EndTimestamp)
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
        /// Delete a version.
        /// </summary>
        public void DeleteVersion(VersionKey versionKey, long readTimestamp)
        {
            throw new NotImplementedException();
        }
    }
}