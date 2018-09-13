namespace GraphView.Transaction
{
    using Cassandra;
    using RecordRuntime;
    using System;
    using System.Collections.Generic;
    using System.Threading;

    /// <summary>
    /// use `isCommitTsOrLB` column to indicate
    /// the content in `commitTime`
    /// </summary>
    public enum IsCommitTsOrLB
    {
        CommitLowerBound,
        CommitTs
    }

    /// <summary>
    /// In cassandraVersionDb, we assume a cassandra table associates with a GraphView
    /// table, which takes (recordKey, versionKey) as the primary keys. A cassandra 
    /// keyspace associcates with a cassandraVersionDb. Every table name is in
    /// the format of 'keyspace.tablename', such as 'UserDb.Profile'
    /// </summary>
    internal partial class CassandraVersionDb : VersionDb
    {
        /// <summary>
        /// default keyspace
        /// </summary>
        public static readonly string DEFAULT_KEYSPACE = "versiondb";

        /// <summary>
        /// singleton instance
        /// </summary>
        private static volatile CassandraVersionDb instance;

        /// <summary>
        /// lock to init the singleton instance
        /// </summary>
        private static readonly object initlock = new object();

        internal TxResourceManager[] resourceManagers;

        internal CassandraSessionManager SessionManager
        {
            get
            {
                return CassandraSessionManager.Instance;
            }
        }

        public int threadId = 0;

        private CassandraVersionDb(int partitionCount, int threadId)
            : base(partitionCount)
        {
            this.threadId = threadId;

            this.resourceManagers = new TxResourceManager[partitionCount];
            for (int pid = 0; pid < partitionCount; pid++)
            {
                this.dbVisitors[pid] = new CassandraVersionDbVisitor();
                this.resourceManagers[pid] = new TxResourceManager();
            }

            // make sure tx_table exists
            this.CQLExecute(string.Format(CassandraVersionDb.CQL_CREATE_TX_TABLE, VersionDb.TX_TABLE));

            this.PhysicalPartitionByKey = key => StaticRandom.Seed() % this.PartitionCount;
        }

        internal static CassandraVersionDb Instance(int partitionCount, int threadId)
        {
            if (CassandraVersionDb.instance == null)
            {
                lock (initlock)
                {
                    if (CassandraVersionDb.instance == null)
                    {
                        CassandraVersionDb.instance = new CassandraVersionDb(partitionCount, threadId);
                    }
                }
            }
            return CassandraVersionDb.instance;
        }

        internal override TxResourceManager GetResourceManager(int partition)
        {
            if (partition >= this.PartitionCount)
            {
                throw new ArgumentException("partition should be smaller then partitionCount");
            }
            return this.resourceManagers[partition];
        }

        internal override void EnqueueTxEntryRequest(long txId, TxEntryRequest txEntryRequest, int execPartition = 0)
        {
            //int pk = this.PhysicalPartitionByKey(txId);
            //this.dbVisitors[pk].Invoke(txEntryRequest);
            this.dbVisitors[execPartition].Invoke(txEntryRequest);            
        }
    }

    /// <summary>
    /// Cassandra CQL statements
    /// </summary>
    internal partial class CassandraVersionDb
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
            "VALUES ({1}, {2}, {3}, {4}) " +
            "IF NOT EXISTS";
        
        public static readonly string CQL_GET_TX_TABLE_ENTRY =
            "SELECT * FROM {0} WHERE txId = {1}";
        
        public static readonly string CQL_UPDATE_TX_STATUS =
            "UPDATE {0} SET status={1} WHERE txId={2} IF EXISTS";
        
        // set commit timestamp with two steps
        public static readonly string CQL_SET_COMMIT_TIME =     // LWT(light weight Transaction)
            "UPDATE {0} SET commitTime={1}, isCommitTsOrLB={2} " +
            "WHERE txId={3} IF commitTime<{1}";
        public static readonly string CQL_SET_COMMIT_TIME_SET_FLAG =    // LWT, change flag bit
            "UPDATE {0} SET isCommitTsOrLB={1} WHERE txId={2} IF isCommitTsOrLB={3}";

        public static readonly string CQL_UPDATE_COMMIT_LB =
            "UPDATE {0} SET commitTime = {1} " +
            "WHERE txId = {2} " +
            "IF isCommitTsOrLB = {3} AND commitTime < {1}";
        
        public static readonly string CQL_REMOVE_TX =
            "DELETE FROM {0} WHERE txId={1} IF EXISTS";
        
        public static readonly string CQL_RECYCLE_TX =
            "UPDATE {0} SET status={1}, commitTime={2}, isCommitTsOrLB={3} " +
            "WHERE txId={4} " +
            "IF EXISTS";
    }

    /// <summary>
    /// VersionTable related
    /// </summary>
    internal partial class CassandraVersionDb
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
            //return this.SessionManager.GetSession(this.threadId, CassandraVersionDb.DEFAULT_KEYSPACE).Execute(cql);
            return this.SessionManager.GetSession(CassandraVersionDb.DEFAULT_KEYSPACE).Execute(cql);

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
            //var rs = this.SessionManager.GetSession(this.threadId, CassandraVersionDb.DEFAULT_KEYSPACE).Execute(cql);
            var rs = this.SessionManager.GetSession(CassandraVersionDb.DEFAULT_KEYSPACE).Execute(cql);
            var rse = rs.GetEnumerator();
            rse.MoveNext();
            return rse.Current.GetValue<bool>("[applied]");
        }

        internal override VersionTable CreateVersionTable(string tableId, long redisDbIndex = 0)
        {
            try
            {
                this.CQLExecute(string.Format(CassandraVersionDb.CQL_CREATE_VERSION_TABLE, tableId));
            } catch (AlreadyExistsException e)  // if `tableId` exists
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
                        CassandraVersionTable vtable = new CassandraVersionTable(this, tableId, this.PartitionCount);
                        this.versionTables.Add(tableId, vtable);
                    }
                }
            }

            return this.versionTables[tableId];
        }

        internal override IEnumerable<string> GetAllTables()
        {
            RowSet rs = this.CQLExecute(string.Format(CassandraVersionDb.CQL_GET_ALL_TABLES, CassandraVersionDb.DEFAULT_KEYSPACE));
            IList<string> tables = new List<string>();
            foreach (var row in rs)
            {
                tables.Add(row.GetValue<string>("table_name"));
            }

            return tables;
        }

        internal override bool DeleteTable(string tableId)
        {
            this.CQLExecute(string.Format(CassandraVersionDb.CQL_DROP_TABLE, tableId));
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
            this.CQLExecute(string.Format(CassandraVersionDb.CQL_CREATE_TX_TABLE, VersionDb.TX_TABLE));

            // 
            this.versionTables.Clear();
        }

        internal override void ClearTxTable()
        {
            this.DeleteTable(VersionDb.TX_TABLE);
            // recreate `tx_table`
            this.CQLExecute(string.Format(CassandraVersionDb.CQL_CREATE_TX_TABLE, VersionDb.TX_TABLE));
        }
    }

    /// <summary>
    /// tx releated
    /// </summary>
    internal partial class CassandraVersionDb
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
                    string.Format(CassandraVersionDb.CQL_INSERT_NEW_TX, 
                                  VersionDb.TX_TABLE,
                                  txId,
                                  (sbyte)TxStatus.Ongoing,     // default status
                                  TxTableEntry.DEFAULT_COMMIT_TIME,
                                  (sbyte)IsCommitTsOrLB.CommitLowerBound));
                if (applied)
                {
                    break;
                } else
                {
                    // txId exists
                    txId = StaticRandom.RandIdentity();
                }   
            }

            return txId;
        }

        internal override TxTableEntry GetTxTableEntry(long txId)
        {
            var rs = this.CQLExecute(string.Format(CassandraVersionDb.CQL_GET_TX_TABLE_ENTRY, 
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
            this.CQLExecuteWithIfApplied(string.Format(CassandraVersionDb.CQL_UPDATE_TX_STATUS,
                                                VersionDb.TX_TABLE, (sbyte)status, txId));
        }

        internal override long SetAndGetCommitTime(long txId, long proposedCommitTime)
        {           
            bool applied = this.CQLExecuteWithIfApplied(string.Format(CassandraVersionDb.CQL_SET_COMMIT_TIME,
                                                                    VersionDb.TX_TABLE,
                                                                    proposedCommitTime,
                                                                    (sbyte)IsCommitTsOrLB.CommitTs,
                                                                    txId));
            if (!applied)
            {
                this.CQLExecuteWithIfApplied(string.Format(CassandraVersionDb.CQL_SET_COMMIT_TIME_SET_FLAG,
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
            this.CQLExecuteWithIfApplied(string.Format(CassandraVersionDb.CQL_UPDATE_COMMIT_LB,
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
            return this.CQLExecuteWithIfApplied(string.Format(CassandraVersionDb.CQL_REMOVE_TX, 
                                                       VersionDb.TX_TABLE, txId));
        }

        internal override bool RecycleTx(long txId)
        {
            return this.CQLExecuteWithIfApplied(string.Format(CassandraVersionDb.CQL_RECYCLE_TX,
                                                       VersionDb.TX_TABLE,
                                                       (sbyte)TxStatus.Ongoing,     // default status
                                                       TxTableEntry.DEFAULT_COMMIT_TIME,
                                                       (sbyte)IsCommitTsOrLB.CommitLowerBound,
                                                       txId));
        }
    }
}
