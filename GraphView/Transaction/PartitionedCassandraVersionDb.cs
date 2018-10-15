
namespace GraphView.Transaction
{
    using Cassandra;
    using System.Threading;
    using System.Collections.Generic;
    using System;
    using System.Collections.Concurrent;
    using System.Threading.Tasks;

    internal partial class PartitionedCassandraVersionDb : VersionDb
    {
        /// <summary>
        /// default keyspace
        /// </summary>
        public static readonly string DEFAULT_KEYSPACE = "versiondb_txn";

        /// <summary>
        /// singleton instance
        /// </summary>
        private static volatile PartitionedCassandraVersionDb instance;

        // monitor run or not
        public bool Active { get; set; } = false;

        /// <summary>
        /// lock to init the singleton instance
        /// </summary>
        private static readonly object initlock = new object();

        // parameters
        internal int replication_factor = 1;
        internal ConsistencyLevel consistency_level = ConsistencyLevel.One;
        internal string contact_points = "127.0.0.1";

        private readonly object objLock = new object();

        internal CassandraSessionManager sessionManager = null;

        internal CassandraSessionManager SessionManager
        {
            get
            {
                if (this.sessionManager == null)
                {
                    lock(objLock)
                    {
                        if (this.sessionManager == null)
                        {
                            Console.WriteLine("session manager in Cassandra Session Manager");
                            this.sessionManager = CassandraSessionManager.Instance2(this.contact_points, this.replication_factor, this.consistency_level);
                        }
                    }
                }

                return this.sessionManager;
            }
        }

        //RequestQueue<TxEntryRequest>[] partitionedQueues;
        //ConcurrentQueue<TxEntryRequest>[] ccPartitionedQueues;
        Queue<TxEntryRequest>[] rawPartitionedQueues;
        //int[] latches;
        public ConcurrentQueue<int> ccTasksCnt;

        /// <summary>
        /// A visitor that translates tx entry requests to CQL queries, 
        /// sends them to Cassandra, and collects results and fill the request's result fields.
        /// 
        /// Since the visitor maintains no states across individual invokations, 
        /// only one instance suffice for all invoking threads. 
        /// </summary>
        PartitionedCassandraVersionDbVisitor cassandraVisitor;

        internal TxResourceManager[] resourceManagers;

        private PartitionedCassandraVersionDb(int partitionCount, string contactPoints, int replicationFactor, ConsistencyLevel consistencyLevel)
            :base(1)    // fake partitionCount to avoid memory overflow
        {
            Console.WriteLine("PartitionedCassandraVersionDb build");

            this.contact_points = contactPoints;
            this.replication_factor = replicationFactor;
            this.consistency_level = consistencyLevel;

            //this.partitionedQueues = new RequestQueue<TxEntryRequest>[partitionCount];
            //this.ccPartitionedQueues = new ConcurrentQueue<TxEntryRequest>[partitionCount];
            this.rawPartitionedQueues = new Queue<TxEntryRequest>[partitionCount];
            //this.latches = new int[partitionCount];
            this.ccTasksCnt = new ConcurrentQueue<int>();
            this.resourceManagers = new TxResourceManager[partitionCount];

            this.PartitionCount = partitionCount;
            this.dbVisitors = new VersionDbVisitor[partitionCount];


            for (int pk = 0; pk < partitionCount; pk++)
            {
                //this.partitionedQueues[pk] = new RequestQueue<TxEntryRequest>(partitionCount);
                //this.ccPartitionedQueues[pk] = new ConcurrentQueue<TxEntryRequest>();
                this.rawPartitionedQueues[pk] = new Queue<TxEntryRequest>(partitionCount);
                //this.rawPartitionedQueues[pk] = new Queue<TxEntryRequest>();
                //this.latches[pk] = 0;
                //this.resourceManagers[pk] = new TxResourceManager();
                this.dbVisitors[pk] = new PartitionedCassandraVersionDbVisitor(pk);
            }
        }

        internal static PartitionedCassandraVersionDb Instance(int partitionCount, string contactPoints, int replicationFactor, ConsistencyLevel consistencyLevel)
        {
            if (PartitionedCassandraVersionDb.instance == null)
            {
                lock (initlock)
                {
                    if (PartitionedCassandraVersionDb.instance == null)
                    {
                        PartitionedCassandraVersionDb.instance = new PartitionedCassandraVersionDb(partitionCount, contactPoints, replicationFactor, consistencyLevel);
                    }
                }
            }
            return PartitionedCassandraVersionDb.instance;
        }

        public void ShowLoadBalance()
        {
            Console.WriteLine("LOAD BALANCE of VersionDb ");

            int[] tasksCountTotal = new int[this.PartitionCount];
            for (int i = 0; i < this.PartitionCount; i++)
            {
                tasksCountTotal[i] = 0;
            }
            int pid = 0;
            while (this.ccTasksCnt.TryDequeue(out pid))
            {
                tasksCountTotal[pid] += 1;
            }
            for (int i = 0; i < this.PartitionCount; i++)
            {
                Console.WriteLine("Partition i={0}, tasks {1}", i, tasksCountTotal[i]);
            }

            Console.WriteLine();


            foreach (var vt in this.versionTables)
            {
                Console.WriteLine("LOAD BALANCE of VersionTable {0}", vt.Key);

                var q = (vt.Value as PartitionedCassandraVersionTable).ccTasksCnt;

                int[] tasksCountTotal2 = new int[this.PartitionCount];
                for (int i = 0; i < this.PartitionCount; i++)
                {
                    tasksCountTotal2[i] = 0;
                }
                int pid2 = 0;
                while (q.TryDequeue(out pid2))
                {
                    tasksCountTotal2[pid2] += 1;
                }
                for (int i = 0; i < this.PartitionCount; i++)
                {
                    Console.WriteLine("Partition i={0}, tasks {1}", i, tasksCountTotal2[i]);
                }
            }
        }

        // start threads running monitors
        public void StartMonitors()
        {
            this.Active = true;

            for (int pk = 0; pk < this.PartitionCount; pk++)
            {
                int pid = pk;

                Thread thread = new Thread(this.Monitor);
                thread.Start(pid);

                //Task.Factory.StartNew(() => this.Monitor(pid));
            }
            Console.WriteLine("VersionDB Monitors Running");

            foreach (var vt in this.versionTables)
            {
                (vt.Value as PartitionedCassandraVersionTable).StartMonitors();
            }
        }

        public void StopMonitors()
        {
            this.Active = false;
            Console.WriteLine("All monitors stopped");
        }

        private void Monitor(object partitionKey)
        {
            //long totalTicks = 0;
            //long idleTicks = 0;

            //long round = 0;
            //long idleRound = 0;

            int pk = (int)partitionKey;

            while (this.Active)
            {
                //long start = DateTime.Now.Ticks;
                //round++;
                //bool idle = true;

                // flush version db queue
                TxEntryRequest txReq = null;
                lock (this.rawPartitionedQueues[pk])
                {
                    if (this.rawPartitionedQueues[pk].Count > 0)
                    {
                        txReq = this.rawPartitionedQueues[pk].Dequeue();
                    } else
                    {
                        System.Threading.Monitor.Wait(this.rawPartitionedQueues[pk]);
                    }
                }
                if (txReq != null)
                {
                    this.dbVisitors[pk].Invoke(txReq);
                    lock (txReq)
                    {
                        System.Threading.Monitor.Pulse(txReq);
                    }
                }

                //if (this.partitionedQueues[pk].TryDequeue(out txReq))
                //if (this.ccPartitionedQueues[pk].TryDequeue(out txReq))
                //{
                //    //this.ccPartitionedQueues[pk].IsEmpty;
                //    //idle = false;
                //    this.dbVisitors[pk].Invoke(txReq);
                //    lock (txReq)
                //    {
                //        System.Threading.Monitor.Pulse(txReq);
                //    }
                //}

                // flush VersionTables Queue
                //VersionEntryRequest veReq = null;
                //if (this.versionTables.Count > 0)   // avoid lock mostly, just for test, not safe theoretically
                //{
                //    foreach (var item in this.versionTables)
                //    {
                //        //if ((item.Value as PartitionedCassandraVersionTable).partitionedQueues[pk].TryDequeue(out veReq))
                //        if ((item.Value as PartitionedCassandraVersionTable).ccPartitionedQueues[pk].TryDequeue(out veReq))
                //        {
                //            //idle = false;
                //            (item.Value as PartitionedCassandraVersionTable).tableVisitors[pk].Invoke(veReq);
                //            lock (veReq)
                //            {
                //                System.Threading.Monitor.Pulse(veReq);
                //            }
                //        }
                //    }
                //}

                //long end = DateTime.Now.Ticks;

                //long delta = end - start;
                //totalTicks += delta;

                //if (idle)
                //{
                //    idleRound++;
                //    idleTicks += delta;
                //}
            }

            //Console.WriteLine("Monitor: {0}, total round {1}, idle round {2}, idle rate {3} \n" +
            //                  "              total ticks {4}, idle ticks {5}, idle rate {6}", 
            //                  partitionKey, round, idleRound, idleRound * 1.0 / round,
            //                  totalTicks, idleTicks, idleTicks * 1.0 / totalTicks);
        }

        internal override TxResourceManager GetResourceManager(int partition)
        {
            if (partition >= this.PartitionCount)
            {
                throw new ArgumentException("partition should be smaller then partitionCount");
            }
            return this.resourceManagers[partition];
        }

        internal override void EnqueueTxEntryRequest(long txId, TxEntryRequest txEntryRequest, int executorPK = 0)
        {
            this.dbVisitors[executorPK].Invoke(txEntryRequest);
            return;

            ////////////////////////////////////////////////

            int pk = this.PhysicalTxPartitionByKey(txId);

            //while (Interlocked.CompareExchange(ref this.latches[pk], 1, 0) != 0) ;
            //partitionedQueues[pk].Enqueue(txEntryRequest, executorPK);
            //Interlocked.Exchange(ref this.latches[pk], 0);

            // count tasks per partition
            //this.ccTasksCnt.Enqueue(pk);

            lock (this.rawPartitionedQueues[pk])
            {
                this.rawPartitionedQueues[pk].Enqueue(txEntryRequest);
                System.Threading.Monitor.Pulse(this.rawPartitionedQueues[pk]);
            }

            //lock (partitionedQueues[pk])
            //{
            //    partitionedQueues[pk].Enqueue(txEntryRequest, executorPK);
            //}
            //this.ccPartitionedQueues[pk].Enqueue(txEntryRequest);

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

        public static readonly string CQL_INSERT_NEW_TX =       // todo: check
            "INSERT INTO {0} (txId, status, commitTime, isCommitTsOrLB) " +
            "VALUES ({1}, {2}, {3}, {4}) "; // " IF NOT EXISTS";

        public static readonly string CQL_GET_TX_TABLE_ENTRY =
            "SELECT * FROM {0} WHERE txId = {1}";

        public static readonly string CQL_UPDATE_TX_STATUS =    // todo: check
            "UPDATE {0} SET status={1} WHERE txId={2}";

        // set commit timestamp with two steps
        // STEP 1: get tx entry
        // step 2: update according to the value read
        public static readonly string CQL_SET_COMMIT_TIME =     // todo: check
            "UPDATE {0} SET commitTime={1}, isCommitTsOrLB={2} " +
            "WHERE txId={3}";   // IF commitTime<{1}";
        public static readonly string CQL_SET_COMMIT_TIME_SET_FLAG =
            "UPDATE {0} SET isCommitTsOrLB={1} WHERE txId={2}"; // IF isCommitTsOrLB={3}";

        // 2 STEPs
        public static readonly string CQL_UPDATE_COMMIT_LB =    // todo: check
            "UPDATE {0} SET commitTime = {1} " +
            "WHERE txId = {2} ";    // + " IF isCommitTsOrLB = {3} AND commitTime < {1}";

        public static readonly string CQL_REMOVE_TX =           // todo: check
            "DELETE FROM {0} WHERE txId={1}";

        public static readonly string CQL_RECYCLE_TX =          // todo: check
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
                        PartitionedCassandraVersionTable vtable = new PartitionedCassandraVersionTable(this, tableId, this.PartitionCount);
                        this.versionTables.Add(tableId, vtable);
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
                this.CQLExecute(
                    string.Format(CassandraVersionDb.CQL_INSERT_NEW_TX,
                                  VersionDb.TX_TABLE,
                                  txId,
                                  (sbyte)TxStatus.Ongoing,     // default status
                                  TxTableEntry.DEFAULT_COMMIT_TIME,
                                  (sbyte)IsCommitTsOrLB.CommitLowerBound));
                break;
                //if (applied)
                //{
                //    break;
                //}
                //else
                //{
                //    // txId exists
                //    txId = StaticRandom.RandIdentity();
                //}
            }

            return txId;
        }

        internal Row GetRawTxTableEntry(long txId)
        {
            var rs = this.CQLExecute(string.Format(PartitionedCassandraVersionDb.CQL_GET_TX_TABLE_ENTRY,
                                                    VersionDb.TX_TABLE, txId));
            var rse = rs.GetEnumerator();
            rse.MoveNext();
            Row row = rse.Current;
            return row;
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
            this.CQLExecute(string.Format(PartitionedCassandraVersionDb.CQL_UPDATE_TX_STATUS,
                                                VersionDb.TX_TABLE, (sbyte)status, txId));
        }

        internal override long SetAndGetCommitTime(long txId, long proposedCommitTime)
        {
            Row row = this.GetRawTxTableEntry(txId);
            if (row == null)
            {
                return -1L;
            }
            IsCommitTsOrLB isCommitTsOrLB = (IsCommitTsOrLB)row.GetValue<sbyte>("iscommittsorlb");
            long commitTime = row.GetValue<long>("committime");
            long realCommitTime = isCommitTsOrLB ==
                IsCommitTsOrLB.CommitTs ? commitTime : TxTableEntry.DEFAULT_COMMIT_TIME;
            if (realCommitTime < proposedCommitTime)
            {
                realCommitTime = proposedCommitTime;
                this.CQLExecute(string.Format(PartitionedCassandraVersionDb.CQL_SET_COMMIT_TIME,
                                                                    VersionDb.TX_TABLE,
                                                                    proposedCommitTime,
                                                                    (sbyte)IsCommitTsOrLB.CommitTs,
                                                                    txId));
            } else
            {
                if (isCommitTsOrLB == IsCommitTsOrLB.CommitLowerBound)
                {
                    this.CQLExecute(string.Format(PartitionedCassandraVersionDb.CQL_SET_COMMIT_TIME_SET_FLAG,
                                                        VersionDb.TX_TABLE,
                                                        (sbyte)IsCommitTsOrLB.CommitTs,
                                                        txId));
                }
            }

            return realCommitTime;
        }

        internal override long UpdateCommitLowerBound(long txId, long lowerBound)
        {
            // step 1: read first
            Row row = this.GetRawTxTableEntry(txId);
            if (row == null)
            {
                return -2L;
            }
            IsCommitTsOrLB isCommitTsOrLB = (IsCommitTsOrLB)row.GetValue<sbyte>("iscommittsorlb");
            long commitTime = row.GetValue<long>("committime");
            long realCommitTime = isCommitTsOrLB ==
                IsCommitTsOrLB.CommitTs ? commitTime : TxTableEntry.DEFAULT_COMMIT_TIME;

            if (isCommitTsOrLB == IsCommitTsOrLB.CommitLowerBound && commitTime < lowerBound)
            {
                this.CQLExecute(string.Format(PartitionedCassandraVersionDb.CQL_UPDATE_COMMIT_LB,
                                                VersionDb.TX_TABLE, lowerBound, txId));
            }

            return realCommitTime;
        }

        internal override bool RemoveTx(long txId)
        {
            this.CQLExecute(string.Format(PartitionedCassandraVersionDb.CQL_REMOVE_TX,
                                                       VersionDb.TX_TABLE, txId));
            return true;
        }

        internal override bool RecycleTx(long txId)
        {
            this.CQLExecute(string.Format(PartitionedCassandraVersionDb.CQL_RECYCLE_TX,
                                                       VersionDb.TX_TABLE,
                                                       (sbyte)TxStatus.Ongoing,     // default status
                                                       TxTableEntry.DEFAULT_COMMIT_TIME,
                                                       (sbyte)IsCommitTsOrLB.CommitLowerBound,
                                                       txId));
            return true;
        }
    }
}
