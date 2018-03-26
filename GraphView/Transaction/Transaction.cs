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

        /// <summary>
        /// Read set, using for checking visibility of the versions read.
        /// For every read operation, add the recordId, the begin and the end timestamp of the version we read to the readSet.
        /// </summary>
        private readonly Dictionary<string, Dictionary<object, VersionEntry>> readSet;

        /// <summary>
        /// Write set, using for
        /// 1) logging new versions during commit
        /// 2) updating the old and new versions' timestamps during commit
        /// 3) locating old versions for garbage collection
        /// Add the versions updated (old and new), versions deleted (old), and versions inserted (new) to the writeSet.
        /// </summary>
        private readonly Dictionary<string, Dictionary<object, List<VersionEntry>>> writeSet;

    }

    // For low-level operations
    public partial class Transaction
    {
        
    }

    // For Json operations
    public partial class Transaction
    {
        internal long GetBeginTimestamp()
        {
            throw new NotImplementedException();
        }

        internal bool UploadLocalWriteRecords()
        {
            throw new NotImplementedException();
        }

        internal long GetCommitTimestamp()
        {
            throw new NotImplementedException();
        }

        internal bool Validate()
        {
            throw new NotImplementedException();
        }

        internal bool UpdateVersionMaxCommitTs()
        {
            throw new NotImplementedException();
        }

        internal bool UpdateTxCommitLowerBound()
        {
            throw new NotImplementedException();
        }

        internal void Abort()
        {
            throw new NotImplementedException();
        }

        internal void PostProcessing()
        {
            throw new NotImplementedException();
        }

        internal void Commit()
        {
            throw new NotImplementedException();
        }
    }

    public partial class Transaction
    {
        public bool InsertJson(string tableId, object recordKey, JObject record)
        {
            throw new NotImplementedException();
        }

        public JObject ReadJson(string tableId, object recordKey)
        {
            throw new NotImplementedException();
        }

        public bool UpdateJson(string tableId, object recordKey, JObject record)
        {
            throw new NotImplementedException();
        }

        public bool DeleteJson(string tableId, object recordKey)
        {
            throw new NotImplementedException();
        }
    }
}

