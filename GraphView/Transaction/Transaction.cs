using System.Runtime.CompilerServices;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Data.Entity;
    using System.Threading.Tasks;
    using GraphView.GraphViewDBPortal;
    using Newtonsoft.Json.Linq;
    using System.Runtime.Serialization;

    internal class ReadSetEntry
    {
        internal long VersionKey { get; private set; }
        internal long BeginTimestamp { get; private set; }
        internal object Record { get; private set; }

        public ReadSetEntry(long versionKey, long beginTimestamp, object record)
        {
            this.VersionKey = versionKey;
            this.BeginTimestamp = beginTimestamp;
            this.Record = record;
        }
    }

    public partial class Transaction
    {
        /// <summary>
        /// Data store for loggingl
        /// </summary>
        private readonly LogStore logStore;

        /// <summary>
        /// Version Db for concurrency control
        /// </summary>
        private readonly VersionDb versionDb;


        /// <summary>
        /// Transaction id assigned to this transaction
        /// </summary>
        private readonly long txId;

        /// <summary>
        /// The status of this transaction.
        /// </summary>
        private TxStatus txStatus;

        private long commitTs;

        /// <summary>
        /// Read set, using for checking visibility of the versions read.
        /// </summary>
        private readonly Dictionary<string, Dictionary<object, ReadSetEntry>> readSet;

        /// <summary>
        /// Write set, using for
        /// 1) logging new versions during commit
        /// 2) updating the old and new versions' timestamps during commit
        /// 3) locating old versions for garbage collection
        /// Add the versions updated (old and new), versions deleted (old), and versions inserted (new) to the writeSet.
        /// </summary>
        private readonly Dictionary<string, Dictionary<object, object>> writeSet;

        /// <summary>
        /// A set of version entries that need to be rolled back upon abortion
        /// </summary>
        private readonly Dictionary<string, Dictionary<object, long>> rollbackSet;

        public Transaction(LogStore logStore, VersionDb versionDb)
        {
            this.logStore = logStore;
            this.versionDb = versionDb;
            this.readSet = new Dictionary<string, Dictionary<object, ReadSetEntry>>();
            this.writeSet = new Dictionary<string, Dictionary<object, object>>();
            this.rollbackSet = new Dictionary<string, Dictionary<object, long>>();

            this.txId = this.versionDb.InsertNewTx();
            this.txStatus = TxStatus.Ongoing;

            this.commitTs = -1;
        }

    }

    // For record operations
    public partial class Transaction
    {
        internal long GetBeginTimestamp()
        {
            long maxReadTimestamp = 0;
            //Tranverse the readSet to get the begin timestamp
            foreach (string tableId in this.readSet.Keys)
            {
                foreach (object recordKey in this.readSet[tableId].Keys)
                {
                    long currentBeginTimestamp = readSet[tableId][recordKey].BeginTimestamp;
                    if (maxReadTimestamp < currentBeginTimestamp)
                    {
                        maxReadTimestamp = currentBeginTimestamp;
                    } 
                }
            }

            return maxReadTimestamp;
        }

        internal bool UploadLocalWriteRecords()
        {
            //foreach (string tableId in this.writeSet.Keys)
            //{
            //    foreach (object recordKey in this.writeSet[tableId])
            //    {
            //        if (this.readSet.ContainsKey(tableId) && this.readSet[tableId].ContainsKey(recordKey))
            //        {
            //            // Upload the new version entry when the new image is not null
            //            if (this.writeSet[tableId][recordKey] != null)
            //            {
            //                VersionEntry newImageEntry = new VersionEntry(
            //                    recordKey,
            //                    this.readSet[tableId][recordKey].VersionKey + 1,
            //                    this.writeSet[tableId][recordKey],
            //                    txId,
            //                    -1,
            //                    -1,
            //                    0);

            //                // Call VersionDB API to upload
            //                if (!this.versionDb.UploadRecordByKey(tableId, recordKey, null, newImageEntry))
            //                {
            //                    return false;
            //                }
            //            }

            //            // Replace the old tail in the version list with an entry whose txId is set to the current tx
            //            VersionEntry newTailEntry = new VersionEntry(
            //                recordKey,
            //                this.readSet[tableId][recordKey].VersionKey,
            //                this.readSet[tableId][recordKey].Record,
            //                this.txId,
            //                this.readSet[tableId][recordKey].BeginTimestamp,
            //                this.readSet[tableId][recordKey].EndTimestamp,
            //                this.readSet[tableId][recordKey].MaxCommitTs);

            //            if (!this.versionDb.UploadRecordByKey(tableId, recordKey, this.readSet[tableId][recordKey], newTailEntry))
            //            {
            //                return false;
            //            }
            //        }
            //        else
            //        {

            //        }
            //    }
            //}
            return false;
        }

        internal void GetCommitTimestamp()
        {
            throw new NotImplementedException();
        }

        internal bool Validate()
        {
            throw new NotImplementedException();
        }

        internal void Abort()
        {
            this.txStatus = TxStatus.Aborted;
            this.versionDb.UpdateTxStatus(this.txId, TxStatus.Aborted);

            this.PostProcessing();
        }

        internal void PostProcessing()
        {
            throw new NotImplementedException();
        }

        internal bool Commit()
        {
            if (!this.UploadLocalWriteRecords())
            {
                return false;
            }

            this.GetCommitTimestamp();

            if (!this.Validate())
            {
                return false;
            }

            this.WriteChangetoLog();
            this.txStatus = TxStatus.Committed;
            this.versionDb.UpdateTxStatus(this.txId, TxStatus.Committed);
            
            this.PostProcessing();

            return true;
        }

        internal void WriteChangetoLog()
        {
            throw new NotImplementedException();
        }
    }

    public partial class Transaction
    {
        public bool Insert(string tableId, object recordKey, object record)
        {
           throw new NotImplementedException();
        }

        public object Read(string tableId, object recordKey)
        {
            throw new NotImplementedException();
        }

        public bool Update(string tableId, object recordKey, object record)
        {
           throw new NotImplementedException();
        }

        public bool Delete(string tableId, object recordKey)
        {
            throw new NotImplementedException();
        }

        public bool ReadAndInitialize(string tableId, object recordKey)
        {
            throw new NotImplementedException();
        }
    }
}

