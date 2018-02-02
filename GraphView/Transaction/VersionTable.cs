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
                return (JObject)this.record;
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

    public abstract class VersionDb
    {
        internal virtual VersionEntry GetVersion(string tableId, object key, long readTimestamp)
        {
            throw new NotImplementedException();
        }

        internal virtual bool InsertVersion(RecordKey versionKey, JObject record, long txId, long readTimestamp)
        {
            throw new NotImplementedException();
        }

        internal virtual bool DeleteVersion(
            RecordKey versionKey,
            long txId,
            long readTimestamp,
            out VersionEntry deletedVersion)
        {
            throw new NotImplementedException();
        }

        internal virtual bool UpdateVersion(
            RecordKey versionKey,
            JObject record,
            long txId,
            long readTimestamp,
            out VersionEntry oldVersion,
            out VersionEntry newVersion)
        {
            throw new NotImplementedException();
        }

        internal virtual bool CheckVersionVisibility(
            RecordKey readVersionKey,
            long readVersionBeginTimestamp,
            long readTimestamp)
        {
            throw new NotImplementedException();
        }

        internal virtual bool CheckPhantom(RecordKey scanVersionKey, long oldScanTime, long newScanTime)
        {
            throw new NotImplementedException();
        }

        internal virtual bool UpdateCommittedVersionTimestamp(
            RecordKey writeVersionKey,
            long txId,
            long endTimestamp,
            bool isOld)
        {
            throw new NotImplementedException();
        }

        internal virtual void UpdateAbortedVersionTimestamp(RecordKey writeVersionKey, long txId, bool isOld)
        {
            throw new NotImplementedException();
        }
    }

    /// <summary>
    /// A version table for concurrency control.
    /// </summary>
    public abstract class VersionTable
    {
        internal virtual VersionEntry GetVersion(RecordKey versionKey, long readTimestamp)
        {
            throw new NotImplementedException();
        }

        internal virtual bool InsertVersion(RecordKey versionKey, JObject record, long txId, long readTimestamp)
        {
            throw new NotImplementedException();
        }

        internal virtual bool DeleteVersion(
            RecordKey versionKey, 
            long txId, 
            long readTimestamp, 
            out VersionEntry deletedVersion)
        {
            throw new NotImplementedException();
        }

        internal virtual bool UpdateVersion(
            RecordKey versionKey, 
            JObject record, 
            long txId, 
            long readTimestamp, 
            out VersionEntry oldVersion, 
            out VersionEntry newVersion)
        {
            throw new NotImplementedException();
        }

        internal virtual bool CheckVersionVisibility(
            RecordKey readVersionKey, 
            long readVersionBeginTimestamp, 
            long readTimestamp)
        {
            throw new NotImplementedException();
        }

        internal virtual bool CheckPhantom(RecordKey scanVersionKey, long oldScanTime, long newScanTime)
        {
            throw new NotImplementedException();
        }

        internal virtual bool UpdateCommittedVersionTimestamp(
            RecordKey writeVersionKey, 
            long txId, 
            long endTimestamp, 
            bool isOld)
        {
            throw new NotImplementedException();
        }

        internal virtual void UpdateAbortedVersionTimestamp(RecordKey writeVersionKey, long txId, bool isOld)
        {
            throw new NotImplementedException();
        }
    }

}