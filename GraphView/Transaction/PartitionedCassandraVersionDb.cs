
namespace GraphView.Transaction
{
    using Cassandra;
    using System.Threading;
    using System.Collections.Generic;

    internal partial class PartitionedCassandraVersionDb : VersionDb
    {
        /// <summary>
        /// default keyspace
        /// </summary>
        public static readonly string DEFAULT_KEYSPACE = "versiondb";

        /// <summary>
        /// singleton instance
        /// </summary>
        private static volatile PartitionedCassandraVersionDb instance;

        /// <summary>
        /// lock to init the singleton instance
        /// </summary>
        private static readonly object initlock = new object();

        internal CassandraSessionManager SessionManager
        {
            get
            {
                return CassandraSessionManager.Instance;
            }
        }

        RequestQueue<TxEntryRequest>[] partitionedQueues;
        /// <summary>
        /// A visitor that translates tx entry requests to CQL queries, 
        /// sends them to Cassandra, and collects results and fill the request's result fields.
        /// 
        /// Since the visitor maintains no states across individual invokations, 
        /// only one instance suffice for all invoking threads. 
        /// </summary>
        PartitionedCassandraVersionDbVisitor cassandraVisitor;

        private PartitionedCassandraVersionDb(int partitionCount)
        {
            this.partitionedQueues = new RequestQueue<TxEntryRequest>[partitionCount];
            for (int pk = 0; pk < partitionCount; pk++)
            {
                this.partitionedQueues[pk] = new RequestQueue<TxEntryRequest>(partitionCount);
            }
            this.cassandraVisitor = new PartitionedCassandraVersionDbVisitor();

            for (int pk = 0; pk < partitionCount; pk++)
            {
                Thread thread = new Thread(this.Monitor);
                thread.Start(pk);
            }
        }

        internal static PartitionedCassandraVersionDb Instance(int partitionCount = 4)
        {
            if (PartitionedCassandraVersionDb.instance == null)
            {
                lock (initlock)
                {
                    if (PartitionedCassandraVersionDb.instance == null)
                    {
                        PartitionedCassandraVersionDb.instance = new PartitionedCassandraVersionDb(partitionCount);
                    }
                }
            }
            return PartitionedCassandraVersionDb.instance;
        }

        private void Monitor(object pk)
        {
            int partitionKey = (int)pk;
            while (true)
            {
                TxEntryRequest txReq = null;
                if (this.partitionedQueues[partitionKey].TryDequeue(out txReq))
                {
                    cassandraVisitor.Visit(txReq);
                } 
            }
        }

        internal override void EnqueueTxEntryRequest(long txId, TxEntryRequest txEntryRequest, int executorPK = 0)
        {
            int pk = this.PhysicalTxPartitionByKey(txId);

            partitionedQueues[pk].Enqueue(txEntryRequest, executorPK);

            while (!txEntryRequest.Finished)
            {
                lock(txEntryRequest)
                {
                    if (!txEntryRequest.Finished)
                    {
                        System.Threading.Monitor.Wait(txEntryRequest);
                    }
                }
            }
        }
    }

    /// <summary>
    /// Cassandra CQL statements
    /// </summary>
    internal partial class PartitionedCassandraVersionDb
    {
        // note: by default, Cassandra converts the columns' names into lowercase        
        public static readonly string CQL_CREATE_VERSION_TABLE =
                "CREATE TABLE IF NOT EXISTS {0} (" +
                    "recordKey          ascii," +
                    "versionKey         bigint," +
                    "beginTimestamp     bigint," +
                    "endTimestamp       bigint," +
                    "record             blob," +
                    "txId               bigint," +
                    "maxCommitTs        bigint," +
                    "PRIMARY KEY(recordKey, versionKey)" +
                ")";


        public static readonly string CQL_CREATE_TX_TABLE =
            "CREATE TABLE IF NOT EXISTS {0} (" +
                "txId               bigint PRIMARY KEY," +
                "status             tinyint," +
                "commitTime         bigint," +
                // this column indicates the value in `commitTime` 
                // is "commitTimestamp" or "commitLowerBound"
                // 0 - "commitLowerBound", default
                // 1 - "commitTimestamp"
                "isCommitTsOrLB     tinyint" +      // tinyint <-> sbyte
            ")";

        public static readonly string CQL_GET_ALL_TABLES =
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name='{0}'";

        public static readonly string CQL_DROP_TABLE =
            "DROP TABLE IF EXISTS {0}";

        public static readonly string CQL_DROP_KEYSPACE =
            "DROP KEYSPACE IF EXISTS {0}";

        public static readonly string CQL_INSERT_NEW_TX =
            "INSERT INTO {0} (txId, status, commitTime, isCommitTsOrLB) " +
            "VALUES ({1}, {2}, {3}, {4}) ";

        public static readonly string CQL_GET_TX_TABLE_ENTRY =
            "SELECT * FROM {0} WHERE txId = {1}";

        public static readonly string CQL_UPDATE_TX_STATUS =
            "UPDATE {0} SET status={1} WHERE txId={2}";

        // set commit timestamp with two steps
        public static readonly string CQL_SET_COMMIT_TIME =     // LWT(light weight Transaction)
            "UPDATE {0} SET commitTime={1}, isCommitTsOrLB={2} " +
            "WHERE txId={3} AND commitTime<{1}";
        public static readonly string CQL_SET_COMMIT_TIME_SET_FLAG =    // LWT, change flag bit
            "UPDATE {0} SET isCommitTsOrLB={1} WHERE txId={2} AND isCommitTsOrLB={3}";

        public static readonly string CQL_UPDATE_COMMIT_LB =
            "UPDATE {0} SET commitTime = {1} " +
            "WHERE txId = {2} " +
            "AND isCommitTsOrLB = {3} AND commitTime < {1}";

        public static readonly string CQL_REMOVE_TX =
            "DELETE FROM {0} WHERE txId={1}";

        public static readonly string CQL_RECYCLE_TX =
            "UPDATE {0} SET status={1}, commitTime={2}, isCommitTsOrLB={3} " +
            "WHERE txId={4}";
    }

    /// <summary>
    /// VersionTable related
    /// </summary>
    internal partial class PartitionedCassandraVersionDb
    {
        /// <summary>
        /// Execute the `cql` statement
        /// </summary>
        /// <param name="cql"></param>
        /// <returns></returns>
        internal RowSet CQLExecute(string cql)
        {
            //CassandraSessionManager.CqlCnt += 1;
            //Console.WriteLine(cql);
            return this.SessionManager.GetSession(PartitionedCassandraVersionDb.DEFAULT_KEYSPACE).Execute(cql);
        }

        /// <summary>
        /// Execute statements with Light Weight Transaction (IF),
        /// result RowSet just has one row, whose `[applied]` column indicates 
        /// the execution's state
        /// NOTE: `CREATE TABLE IF ...` can not be executed with this function, 
        /// catch `AlreadyExistsException` instead
        /// </summary>
        /// <param name="cql"></param>
        /// <returns>applied or not</returns>
        internal bool CQLExecuteWithIfApplied(string cql)
        {
            //CassandraSessionManager.CqlIfCnt += 1;
            //Console.WriteLine(cql);
            var rs = this.SessionManager.GetSession(PartitionedCassandraVersionDb.DEFAULT_KEYSPACE).Execute(cql);
            var rse = rs.GetEnumerator();
            rse.MoveNext();
            return rse.Current.GetValue<bool>("[applied]");
        }

        internal override VersionTable CreateVersionTable(string tableId, long redisDbIndex = 0)
        {
            try
            {
                this.CQLExecute(string.Format(PartitionedCassandraVersionDb.CQL_CREATE_VERSION_TABLE, tableId));
            }
            catch (AlreadyExistsException e)  // if `tableId` exists
            {
                return null;
            }

            return this.GetVersionTable(tableId);
        }

        internal override VersionTable GetVersionTable(string tableId)
        {
            if (!this.versionTables.ContainsKey(tableId))
            {
                lock (this.versionTables)
                {
                    if (!this.versionTables.ContainsKey(tableId))
                    {
                        // PartitionedCassandraVersionTable vtable = new PartitionedCassandraVersionTable(this, tableId, this.PartitionCount);
                        // this.versionTables.Add(tableId, vtable);
                    }
                }
            }

            return this.versionTables[tableId];
        }

        internal override IEnumerable<string> GetAllTables()
        {
            RowSet rs = this.CQLExecute(string.Format(PartitionedCassandraVersionDb.CQL_GET_ALL_TABLES, PartitionedCassandraVersionDb.DEFAULT_KEYSPACE));
            IList<string> tables = new List<string>();
            foreach (var row in rs)
            {
                tables.Add(row.GetValue<string>("table_name"));
            }

            return tables;
        }

        internal override bool DeleteTable(string tableId)
        {
            this.CQLExecute(string.Format(PartitionedCassandraVersionDb.CQL_DROP_TABLE, tableId));
            // we do not care whether the tableId exists or not

            if (this.versionTables.ContainsKey(tableId))
            {
                lock (this.versionTables)
                {
                    if (this.versionTables.ContainsKey(tableId))
                    {
                        this.versionTables.Remove(tableId);
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
            }
            return true;
        }

        // clear keyspace, not drop
        internal override void Clear()
        {
            // drop all tables in keyspace
            IEnumerable<string> tables = this.GetAllTables();
            foreach (var tid in tables)
            {
                this.DeleteTable(tid);
            }

            // recreate `tx_table`
            this.CQLExecute(string.Format(PartitionedCassandraVersionDb.CQL_CREATE_TX_TABLE, VersionDb.TX_TABLE));

            // 
            this.versionTables.Clear();

            for (int pid = 0; pid < this.PartitionCount; pid++)
            {
                this.txEntryRequestQueues[pid].Clear();
                this.flushQueues[pid].Clear();
            }
        }

        internal override void ClearTxTable()
        {
            this.DeleteTable(VersionDb.TX_TABLE);
            // recreate `tx_table`
            this.CQLExecute(string.Format(PartitionedCassandraVersionDb.CQL_CREATE_TX_TABLE, VersionDb.TX_TABLE));
        }
    }

    /// <summary>
    /// tx releated
    /// </summary>
    internal partial class PartitionedCassandraVersionDb
    {
        internal override long InsertNewTx(long txId = -1)
        {
            if (txId < 0)
            {
                txId = StaticRandom.RandIdentity();
            }

            while (true)
            {
                // we assume txId conflicts rarely,
                // otherwise, it is the cause of txId generator
                bool applied = this.CQLExecuteWithIfApplied(
                    string.Format(PartitionedCassandraVersionDb.CQL_INSERT_NEW_TX,
                                  VersionDb.TX_TABLE,
                                  txId,
                                  (sbyte)TxStatus.Ongoing,     // default status
                                  TxTableEntry.DEFAULT_COMMIT_TIME,
                                  (sbyte)IsCommitTsOrLB.CommitLowerBound));
                if (applied)
                {
                    break;
                }
                else
                {
                    // txId exists
                    txId = StaticRandom.RandIdentity();
                }
            }

            return txId;
        }

        internal override TxTableEntry GetTxTableEntry(long txId)
        {
            var rs = this.CQLExecute(string.Format(PartitionedCassandraVersionDb.CQL_GET_TX_TABLE_ENTRY,
                                                    VersionDb.TX_TABLE, txId));
            var rse = rs.GetEnumerator();
            rse.MoveNext();
            Row row = rse.Current;
            if (row == null)
            {
                return null;
            }

            IsCommitTsOrLB isCommitTsOrLB = (IsCommitTsOrLB)row.GetValue<sbyte>("iscommittsorlb");
            long commitTime = row.GetValue<long>("committime");
            long realCommitTime = isCommitTsOrLB ==
                IsCommitTsOrLB.CommitTs ? commitTime : TxTableEntry.DEFAULT_COMMIT_TIME;

            return new TxTableEntry(
                txId,
                (TxStatus)row.GetValue<sbyte>("status"),
                realCommitTime,
                commitTime);
        }

        internal override void UpdateTxStatus(long txId, TxStatus status)
        {
            this.CQLExecuteWithIfApplied(string.Format(PartitionedCassandraVersionDb.CQL_UPDATE_TX_STATUS,
                                                VersionDb.TX_TABLE, (sbyte)status, txId));
        }

        internal override long SetAndGetCommitTime(long txId, long proposedCommitTime)
        {
            bool applied = this.CQLExecuteWithIfApplied(string.Format(PartitionedCassandraVersionDb.CQL_SET_COMMIT_TIME,
                                                                    VersionDb.TX_TABLE,
                                                                    proposedCommitTime,
                                                                    (sbyte)IsCommitTsOrLB.CommitTs,
                                                                    txId));
            if (!applied)
            {
                this.CQLExecuteWithIfApplied(string.Format(PartitionedCassandraVersionDb.CQL_SET_COMMIT_TIME_SET_FLAG,
                                                            VersionDb.TX_TABLE,
                                                            (sbyte)IsCommitTsOrLB.CommitTs,
                                                            txId,
                                                            (sbyte)IsCommitTsOrLB.CommitLowerBound));
            }

            TxTableEntry entry = this.GetTxTableEntry(txId);
            if (entry == null)
            {
                return -1L;
            }

            return entry.CommitTime;
        }

        internal override long UpdateCommitLowerBound(long txId, long lowerBound)
        {
            this.CQLExecuteWithIfApplied(string.Format(PartitionedCassandraVersionDb.CQL_UPDATE_COMMIT_LB,
                                                VersionDb.TX_TABLE, lowerBound, txId, (sbyte)IsCommitTsOrLB.CommitLowerBound));

            TxTableEntry entry = this.GetTxTableEntry(txId);
            if (entry == null)
            {
                return -2L;
            }

            return entry.CommitTime;
        }

        internal override bool RemoveTx(long txId)
        {
            return this.CQLExecuteWithIfApplied(string.Format(PartitionedCassandraVersionDb.CQL_REMOVE_TX,
                                                       VersionDb.TX_TABLE, txId));
        }

        internal override bool RecycleTx(long txId)
        {
            return this.CQLExecuteWithIfApplied(string.Format(PartitionedCassandraVersionDb.CQL_RECYCLE_TX,
                                                       VersionDb.TX_TABLE,
                                                       (sbyte)TxStatus.Ongoing,     // default status
                                                       TxTableEntry.DEFAULT_COMMIT_TIME,
                                                       (sbyte)IsCommitTsOrLB.CommitLowerBound,
                                                       txId));
        }
    }
}
