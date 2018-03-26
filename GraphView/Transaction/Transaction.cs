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

    public class Transaction
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
        /// The version db is used to store transaction status
        /// It's can be easily to try different combinations, 
        /// like version entries in cassandra(versionDb), transaction in redis(transactionDb)
        /// </summary>
        private readonly VersionDb transactionDb;

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
        private readonly Dictionary<string, HashSet<ReadSetEntry>> readSet;

        /// <summary>
        /// Write set, using for
        /// 1) logging new versions during commit
        /// 2) updating the old and new versions' timestamps during commit
        /// 3) locating old versions for garbage collection
        /// Add the versions updated (old and new), versions deleted (old), and versions inserted (new) to the writeSet.
        /// </summary>
        private readonly Dictionary<string, HashSet<WriteSetEntry>> writeSet;
    }
}
