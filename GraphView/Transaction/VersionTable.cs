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
    internal class VersionEntry
    {
        private bool isBeginTxId;
        public long beginTimestamp;
        private bool isEndTxId;
        public long endTimestamp;
        private object record;

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
                return (JObject) this.record;
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
    /// A version Db for concurrency control.
    /// </summary>
    public abstract class VersionDb
    {
        internal virtual VersionEntry ReadVersion(
            string tableId, 
            object recordKey, 
            long readTimestamp)
        {
            throw new NotImplementedException();
        }

        internal virtual bool InsertVersion(
            string tableId, 
            object recordKey, 
            JObject record, 
            long txId, 
            long readTimestamp)
        {
            throw new NotImplementedException();
        }

        internal virtual bool DeleteVersion(
            string tableId,
            object recordKey,
            long txId,
            long readTimestamp,
            out VersionEntry deletedVersion)
        {
            throw new NotImplementedException();
        }

        internal virtual bool UpdateVersion(
            string tableId,
            object recordKey,
            JObject record,
            long txId,
            long readTimestamp,
            out VersionEntry oldVersion,
            out VersionEntry newVersion)
        {
            throw new NotImplementedException();
        }

        internal virtual bool CheckReadVisibility(
            string tableId,
            object recordKey,
            long readVersionBeginTimestamp,
            long readTimestamp,
            long txId)
        {
            throw new NotImplementedException();
        }

        internal virtual bool CheckPhantom(
            string tableId,
            object recordKey,
            long oldScanTime, 
            long newScanTime)
        {
            throw new NotImplementedException();
        }

        internal virtual void UpdateCommittedVersionTimestamp(
            string tableId,
            object recordKey,
            long txId,
            long endTimestamp)
        {
            throw new NotImplementedException();
        }

        internal virtual void UpdateAbortedVersionTimestamp(
            string tableId,
            object recordKey,
            long txId)
        {
            throw new NotImplementedException();
        }
    }

    /// <summary>
    /// A version table for concurrency control.
    /// </summary>
    public abstract class VersionTable
    {
        internal virtual VersionEntry ReadVersion(object recordKey, long readTimestamp)
        {
            throw new NotImplementedException();
        }

        internal virtual bool InsertVersion(object recordKey, JObject record, long txId, long readTimestamp)
        {
            throw new NotImplementedException();
        }

        internal virtual bool DeleteVersion(
            object recordKey, 
            long txId, 
            long readTimestamp, 
            out VersionEntry deletedVersion)
        {
            throw new NotImplementedException();
        }

        internal virtual bool UpdateVersion(
            object recordKey, 
            JObject record, 
            long txId, 
            long readTimestamp, 
            out VersionEntry oldVersion, 
            out VersionEntry newVersion)
        {
            throw new NotImplementedException();
        }

        internal virtual bool CheckReadVisibility(
            object recordKey, 
            long readVersionBeginTimestamp, 
            long readTimestamp,
            long txId)
        {
            throw new NotImplementedException();
        }

        internal virtual bool CheckPhantom(object recordKey, long oldScanTime, long newScanTime)
        {
            throw new NotImplementedException();
        }

        internal virtual void UpdateCommittedVersionTimestamp(
            object recordKey, 
            long txId, 
            long endTimestamp)
        {
            throw new NotImplementedException();
        }

        internal virtual void UpdateAbortedVersionTimestamp(object recordKey, long txId)
        {
            throw new NotImplementedException();
        }
    }

}