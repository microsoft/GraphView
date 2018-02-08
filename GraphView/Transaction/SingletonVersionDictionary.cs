
using System.Windows.Forms;

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

    internal class VersionNode
    {
        public VersionEntry VersionEntry;
        public VersionNode Next;
    }

    internal class VersionList
    {
        private VersionNode head;

        public VersionList()
        {
            this.head = new VersionNode();
            head.VersionEntry = new VersionEntry(false, long.MaxValue, false, long.MaxValue, null);
            head.Next = null;
        }

        public void AddFirst(VersionEntry versionEntry)
        {
            VersionNode newNode = new VersionNode();
            newNode.VersionEntry = versionEntry;

            do
            {
                newNode.Next = this.head;
            }
            while (newNode.Next != Interlocked.CompareExchange(ref this.head, newNode, newNode.Next));
        }

        public bool ChangeNodeValue(VersionEntry oldVersion, VersionEntry newVersion)
        {
            VersionNode node = this.head;
            while (node != null && node.VersionEntry.Record != null)
            {
                //try to find the old version
                if (node.VersionEntry == oldVersion)
                {
                    return oldVersion == Interlocked.CompareExchange(ref node.VersionEntry, newVersion, oldVersion);
                }
                node = node.Next;
            }

            return false;
        }

        public IList<VersionEntry> CopyToList()
        {
            IList<VersionEntry> versionList = new List<VersionEntry>();
            VersionNode node = this.head;
            while (node != null && node.VersionEntry.Record != null)
            {
                versionList.Add(node.VersionEntry);
                node = node.Next;
            }

            return versionList;
        }
    }

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
        private readonly Dictionary<object, VersionList> dict;
        private readonly object listLock;

        public SingletonVersionDictionary(string tableId)
        {
            this.dict = new Dictionary<object, VersionList>();
            this.listLock = new object();
            this.TableId = tableId;
        }

        internal override IList<VersionEntry> GetVersionList(object recordKey)
        {
            if (!this.dict.ContainsKey(recordKey))
            {
                return null;
            }

            return this.dict[recordKey].CopyToList();
        }

        internal override void InsertAndUploadVersion(object recordKey, VersionEntry version)
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

            this.dict[recordKey].AddFirst(version);
        }

        internal override bool UpdateAndUploadVersion(object recordKey, VersionEntry oldVersion, VersionEntry newVersion)
        {
            return this.dict[recordKey].ChangeNodeValue(oldVersion, newVersion);
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

            foreach (VersionEntry versionEntry in this.dict[key].CopyToList())
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
