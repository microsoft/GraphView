

using System;
using System.Collections.Generic;
using System.Runtime.Remoting.Messaging;
using Newtonsoft.Json.Linq;

namespace GraphView.Transaction
{
    public class VersionEntry
    {
        public bool IsBeginTxId;
        public long BeginTimestamp;
        public bool IsEndTxId;
        public long EndTimestamp;
        public JObject Record;

        public VersionEntry(bool isBeginTxId, long beginTimestamp, bool isEndTxId, long endTimestamp, JObject jObject)
        {
            this.IsBeginTxId = isBeginTxId;
            this.BeginTimestamp = beginTimestamp;
            this.IsEndTxId = isEndTxId;
            this.EndTimestamp = endTimestamp;
            this.Record = jObject;
        }
    }

    /// <summary>
    /// A lock free hash table for concurrency control
    /// </summary>
    public abstract class LockFreeHashTable
    {
        public virtual List<VersionEntry> GetScanList(string recordId)
        {
            throw new NotImplementedException();
        }
    }

    /// <summary>
    /// A lock free dictionary implementation in single machine environment.
    /// </summary>
    public class LockFreeDictionary : LockFreeHashTable
    {
        private Dictionary<string, List<VersionEntry>> dict;

        public LockFreeDictionary()
        {
            this.dict = new Dictionary<string, List<VersionEntry>>();
        }

        public override List<VersionEntry> GetScanList(string recordId)
        {
            List<VersionEntry> versionList = new List<VersionEntry>();
            if (dict.TryGetValue(recordId, out versionList))
            {
                return versionList;
            }
            throw new KeyNotFoundException();
        }
    }
}