using TransactionBenchmarkTest.YCSB;

namespace GraphView.Transaction
{
    using Cassandra;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;

    public delegate int KeyGenerator();

    public static class StaticRandom
    {
        static int seed = Environment.TickCount;

        static readonly ThreadLocal<Random> random =
            new ThreadLocal<Random>(() => new Random(Interlocked.Increment(ref seed)));

        public static int Rand()
        {
            return random.Value.Next();
        }
    }

    internal class TransactionExecutor
    {
        private int executorId = 0;

        /// <summary>
        /// The size of current working transaction set
        /// </summary>
        private int workingSetSize = 100;      // in terms of # of tx's

        /// <summary>
        /// A queue of workloads accepted from clients
        /// </summary>
        internal Queue<TransactionRequest> workloadQueue;

        /// <summary>
        /// The version database instance
        /// </summary>
        private readonly VersionDb versionDb;

        /// <summary>
        /// The log store instance
        /// </summary>
        private readonly ILogStore logStore;

        /// <summary>
        /// ONLY FOR TEST
        /// </summary>
        internal int CommittedTxs = 0;

        /// <summary>
        /// ONLY FOR TEST
        /// </summary>
        internal int FinishedTxs = 0;

        /// <summary>
        /// The transaction timeout seconds
        /// </summary>
        private int txTimeoutSeconds;

        /// <summary>
        /// A flag to declare whether all requests have been finished
        /// </summary>
        internal bool AllRequestsFinished { get; private set; } = false;

        /// <summary>
        /// A flag to decalre whether the executor should still work to flush requests
        /// </summary>
        internal bool Active { get; set; } = true;

        /// <summary>
        /// A map of transactions, and each transaction has a queue of request from the client
        /// </summary>
        private Dictionary<string, Tuple<TransactionExecution, Queue<TransactionRequest>>> activeTxs;

        /// <summary>
        /// A pool of free tx execution runtime to accommodate new incoming txs 
        /// </summary>
        private Queue<Tuple<TransactionExecution, Queue<TransactionRequest>>> txRuntimePool;

        private List<string> workingSet;

        private TxRange txRange;

        private string[] flushTables;

        /// <summary>
        /// The partition of current executor flushed
        /// </summary>
        internal int Partition { get; private set; }

        internal TxRange TxRange { get; }

        internal TxResourceManager ResourceManager { get; }

        private TransactionExecution txExecution;

        private int taskCount;

        private int recordCount;

        internal long RunBeginTicks { get; set; }
        internal long RunEndTicks { get; set; }

        private StoredProcedureWorkload workload;

        private StoredProcedureType storedProcedureType;

        public TransactionExecutor(
            VersionDb versionDb,
            ILogStore logStore,
            Queue<TransactionRequest> workloadQueue = null,
            int partition = 0,
            int startRange = -1,
            int txTimeoutSeconds = 0,
            TxResourceManager resourceManager = null,
            string[] flushTables = null,
            ManualResetEventSlim startEventSlim = null,
            CountdownEvent countdownEvent = null,
            int recordCount = 0,
            int taskCount = 0,
            KeyGenerator nextKey = null,
            StoredProcedureWorkload workload = null,
            StoredProcedureType storedProcedureType = StoredProcedureType.YCSBStordProcedure,
            int workingSetSize = 100)
        {
            this.versionDb = versionDb;
            this.logStore = logStore;
            this.workloadQueue = workloadQueue ?? new Queue<TransactionRequest>();
            this.activeTxs = new Dictionary<string, Tuple<TransactionExecution, Queue<TransactionRequest>>>();

            this.txTimeoutSeconds = txTimeoutSeconds;
            this.txRange = startRange < 0 ? null : new TxRange(startRange);
            this.ResourceManager = resourceManager == null ? new TxResourceManager() : resourceManager;
            this.txRuntimePool = new Queue<Tuple<TransactionExecution, Queue<TransactionRequest>>>();
            this.workingSet = new List<string>(workingSetSize);

            this.Partition = partition;
            this.flushTables = flushTables;


            this.txExecution = new TransactionExecution(this.logStore, this.versionDb, null, this.txRange, this.Partition, this.ResourceManager);
            
            this.recordCount = recordCount;
            this.taskCount = taskCount;

            this.workload = workload;
            this.storedProcedureType = storedProcedureType;
            this.workingSetSize = workingSetSize;
        }

        public void Reset()
        {
            // TODO: do nothing
            this.CommittedTxs = 0;
            this.FinishedTxs = 0;
            this.RunBeginTicks = -1L;
            this.RunEndTicks = -1L;

            this.txRange.Reset();
            this.txRuntimePool.Clear();
            this.workingSet.Clear();
            this.versionDb.PartitionMounted[this.Partition] = true;
        }

        private Tuple<TransactionExecution, Queue<TransactionRequest>> AllocateNewTxExecution()
        {
            TransactionExecution exec = null;
            if (this.txRuntimePool.Count > 0)
            {
                Tuple<TransactionExecution, Queue<TransactionRequest>> runtimeTuple =
                    this.txRuntimePool.Dequeue();

                exec = runtimeTuple.Item1;
                Queue<TransactionRequest> reqQueue = runtimeTuple.Item2;

                while (reqQueue.Count > 0)
                { 
                    reqQueue.Dequeue();
                }

                return runtimeTuple;
            }
            else
            {
                exec = new TransactionExecution(
                    this.logStore,
                    this.versionDb,
                    null,
                    this.txRange,
                    this.Partition);

                Queue<TransactionRequest> reqQueue = new Queue<TransactionRequest>();

                return Tuple.Create(exec, reqQueue);
            }
        }

        internal static void PinThreadOnCores(long coreIndex)
        {
            int offset = ((int)coreIndex % 4) * 16 + ((int)coreIndex / 4) * 2;
            long allowMask = (1L << offset);
            Thread.BeginThreadAffinity();
            Process Proc = Process.GetCurrentProcess();
            foreach (ProcessThread pthread in Proc.Threads)
            {
                if (pthread.Id == AppDomain.GetCurrentThreadId())
                {
                    long AffinityMask = (long)Proc.ProcessorAffinity;
                    AffinityMask &= allowMask;
                    // AffinityMask &= 0x007F;
                    pthread.ProcessorAffinity = (IntPtr)AffinityMask;
                }
            }

            Thread.EndThreadAffinity();
        }

        public void ExecuteInSync()
        {
            // PinThreadOnCores(this.Partition);

            this.RunBeginTicks = DateTime.Now.Ticks;
            foreach (TransactionRequest req in this.workloadQueue)
            {
                //this.workloadAction(req.Workload, this.txExecution);
            }
            this.RunEndTicks = DateTime.Now.Ticks;
        }

        private int GenerateYCSBKey(int randomX, int indexBound)
        {
            if (!(this.versionDb is SingletonPartitionedVersionDb))
            {
                return randomX;
            }

            int k = this.versionDb.PhysicalPartitionByKey(randomX);
            randomX -= (k - this.Partition);
            if (randomX >= indexBound)
            {
                randomX -= this.versionDb.PartitionCount;
            }
            return randomX;
        }

        public void YCSBExecuteRead()
        {
            // PinThreadOnCores(this.Partition);
            this.RunBeginTicks = DateTime.Now.Ticks;

            Random rand = new Random();
            bool received = false;
            object payload = null;
            int indexBound = this.recordCount;
            for (int i = 0; i < this.taskCount; i++)
            {
                //string recordKey = YCSBKeys[rand.Next(0, indexBound)];
                // int recordKey = rand.Next(0, indexBound);
                int recordKey = this.GenerateYCSBKey(rand.Next(0, indexBound), indexBound);
                this.txExecution.Reset();
                this.txExecution.Read("ycsb_table", recordKey, out received, out payload);
                //recordKey = this.GenerateYCSBKey(rand.Next(0, indexBound), indexBound);
                //this.txExecution.Read("ycsb_table", recordKey, out received, out payload);
                this.txExecution.Commit();

                this.FinishedTxs += 1;
                if (this.txExecution.TxStatus == TxStatus.Committed)
                {
                    this.CommittedTxs += 1;
                }
            }

            this.RunEndTicks = DateTime.Now.Ticks;
        }

        public void YCSBExecuteUpdate()
        {
            // PinThreadOnCores(this.Partition);

            this.RunBeginTicks = DateTime.Now.Ticks;
            Random rand = new Random();
            bool received = false;
            object payload = null;
            int indexBound = this.recordCount;
            string updatePayload = new String('a', 100);
            int preRecordKey = this.Partition - this.versionDb.PartitionCount;
            for (int i = 0; i < this.taskCount; i++)
            {
                //string recordKey = YCSBKeys[rand.Next(0, indexBound)];
                // int recordKey = rand.Next(0, indexBound);
                int recordKey = this.GenerateYCSBKey(StaticRandom.Rand() % indexBound, indexBound);
                // int recordKey = this.versionDb.PartitionCount + preRecordKey;
                // preRecordKey = recordKey;

                this.txExecution.Reset();
                this.txExecution.Read("ycsb_table", recordKey, out received, out payload);
                payload = this.txExecution.ReadPayload;
                if (payload != null)
                {
                    this.txExecution.Update("ycsb_table", recordKey, updatePayload);
                }
                //recordKey = this.GenerateYCSBKey(rand.Next(0, indexBound), indexBound);
                //this.txExecution.Read("ycsb_table", recordKey, out received, out payload);
                this.txExecution.Commit();

                this.FinishedTxs += 1;
                if (this.txExecution.TxStatus == TxStatus.Committed)
                {
                    this.CommittedTxs += 1;
                }
            }
            this.RunEndTicks = DateTime.Now.Ticks;
        }

        public void YCSBExecuteInsert()
        {
            PinThreadOnCores(this.Partition);

            this.RunBeginTicks = DateTime.Now.Ticks;

            Random rand = new Random();
            bool received = false;
            object payload = null;
            // int indexBound = this.YCSBKeys.Length;
            int indexBound = this.taskCount;
            //string updatePayload = new String('a', 100);
            object updatePayload = 0;
            int preRecordKey = this.Partition - this.versionDb.PartitionCount;
            for (int i = 0; i < this.taskCount; i++)
            {
                //string recordKey = YCSBKeys[rand.Next(0, indexBound)];
                // int recordKey = rand.Next(0, indexBound);
                // int recordKey = this.GenerateYCSBKey(rand.Next(0, indexBound), indexBound);
                int recordKey = this.versionDb.PartitionCount + preRecordKey;
                preRecordKey = recordKey;

                this.txExecution.Reset();
                this.txExecution.ReadAndInitialize("ycsb_table", recordKey, out received, out payload);
                payload = this.txExecution.ReadPayload;
                if (payload == null)
                {
                    this.txExecution.Insert("ycsb_table", recordKey, updatePayload);
                }
                //recordKey = this.GenerateYCSBKey(rand.Next(0, indexBound), indexBound);
                //this.txExecution.Read("ycsb_table", recordKey, out received, out payload);
                this.txExecution.Commit();

                this.FinishedTxs += 1;
                if (this.txExecution.TxStatus == TxStatus.Committed)
                {
                    this.CommittedTxs += 1;
                }
            }

            this.RunEndTicks = DateTime.Now.Ticks;
        }

        // read string key
        public void YCSBExecuteRead2()
        {
            this.RunBeginTicks = DateTime.Now.Ticks;

            Random rand = new Random();
            bool received = false;
            object payload = null;
            int indexBound = this.recordCount;

            for (int i = 0; i < this.taskCount; i++)
            {
                if (!this.Active)
                {
                    break;
                }
                this.txExecution.Reset();

                object recordKey = (this.workloadQueue.Dequeue().Workload as YCSBWorkload).Key;                
                this.txExecution.Read("ycsb_table", recordKey, out received, out payload);

                //recordKey = rand.Next(0, indexBound);
                //this.txExecution.Read("ycsb_table", recordKey, out received, out payload);

                //string recordKey2 = (this.workload.Dequeue().Workload as YCSBWorkload).Key;
                //this.txExecution.Read("ycsb_table", recordKey2, out received, out payload);

                this.txExecution.Commit();
                if (this.txExecution.TxStatus == TxStatus.Committed)
                {
                    this.CommittedTxs++;
                }
                this.FinishedTxs++;
            }

            this.AllRequestsFinished = true;

            this.RunEndTicks = DateTime.Now.Ticks;
        }

        // key is int, not real key
        public void YCSBExecuteRead3()
        {
            Random rand = new Random();
            bool received = false;
            object payload = null;
            int indexBound = 200000;    // setting

            int[] intKeys = new int[this.taskCount];
            //for (int i=0; i<this.taskCount; i++)
            //{
            //    intKeys[i] = rand.Next(0, indexBound);
            //}
            intKeys[0] = this.Partition;
            for (int i = 1; i < this.taskCount; i++)
            {
                if (intKeys[i-1] + this.versionDb.PartitionCount >= indexBound)
                {
                    intKeys[i] = this.Partition;
                } else
                {
                    intKeys[i] = intKeys[i - 1] + this.versionDb.PartitionCount;
                }
            }

            this.RunBeginTicks = DateTime.Now.Ticks;

            for (int i = 0; i < this.taskCount; i++)
            {
                if (!this.Active)
                {
                    break;
                }
                this.txExecution.Reset();

                int recordKey = intKeys[i];
                this.txExecution.Read("ycsb_table", recordKey, out received, out payload);
                
                this.txExecution.Commit();
                if (this.txExecution.TxStatus == TxStatus.Committed)
                {
                    this.CommittedTxs++;
                }
                this.FinishedTxs++;
            }

            this.RunEndTicks = DateTime.Now.Ticks;

            this.AllRequestsFinished = true;

        }

        // key is int
        public void YCSBExecuteUpdate3()
        {
            Random rand = new Random();
            bool received = false;
            object payload = null;
            int indexBound = 200000;    // setting
            string updatePayload = new String('a', 100);

            int[] intKeys = new int[this.taskCount];
            //for (int i=0; i<this.taskCount; i++)
            //{
            //    intKeys[i] = rand.Next(0, indexBound);
            //}
            intKeys[0] = this.Partition;
            for (int i = 1; i < this.taskCount; i++)
            {
                if (intKeys[i - 1] + this.versionDb.PartitionCount >= indexBound)
                {
                    intKeys[i] = this.Partition;
                }
                else
                {
                    intKeys[i] = intKeys[i - 1] + this.versionDb.PartitionCount;
                }
            }

            this.RunBeginTicks = DateTime.Now.Ticks;

            for (int i = 0; i < this.taskCount; i++)
            {
                if (!this.Active)
                {
                    break;
                }
                this.txExecution.Reset();

                int recordKey = intKeys[i];
                this.txExecution.Read("ycsb_table", recordKey, out received, out payload);
                payload = this.txExecution.ReadPayload;
                if (payload != null)
                {
                    this.txExecution.Update("ycsb_table", recordKey, updatePayload);
                }

                this.txExecution.Commit();
                if (this.txExecution.TxStatus == TxStatus.Committed)
                {
                    this.CommittedTxs++;
                }
                this.FinishedTxs++;
            }

            this.RunEndTicks = DateTime.Now.Ticks;

            this.AllRequestsFinished = true;

        }

        // key is int
        public void YCSBExecuteReadUpdateHybrid()
        {
            Random rand = new Random();
            int readRate = 50;      // 50%
            int updateRate = 50;    // 50%

            bool received = false;
            object payload = null;
            int indexBound = 200000;    // setting
            string updatePayload = new String('a', 100);

            int[] intKeys = new int[this.taskCount];
            int[] opTypes = new int[this.taskCount];        // 0-read, 1-update

            //for (int i=0; i<this.taskCount; i++)
            //{
            //    intKeys[i] = rand.Next(0, indexBound);
            //}
            intKeys[0] = this.Partition;
            opTypes[0] = rand.Next(0, 100) < readRate ? 0 : 1;

            for (int i = 1; i < this.taskCount; i++)
            {
                if (intKeys[i - 1] + this.versionDb.PartitionCount >= indexBound)
                {
                    intKeys[i] = this.Partition;
                }
                else
                {
                    intKeys[i] = intKeys[i - 1] + this.versionDb.PartitionCount;
                }

                opTypes[i] = rand.Next(0, 100) < readRate ? 0 : 1;
            }

            //int ucnt = 0;
            //for (int i = 0; i < this.taskCount; i++)
            //{
            //    ucnt += opTypes[i];
            //}
            //Console.WriteLine("update count={0}/{1}", ucnt, this.taskCount);

            this.RunBeginTicks = DateTime.Now.Ticks;

            for (int i = 0; i < this.taskCount; i++)
            {
                if (!this.Active)
                {
                    break;
                }
                this.txExecution.Reset();

                int recordKey = intKeys[i];
                this.txExecution.Read("ycsb_table", recordKey, out received, out payload);

                // execute update
                if (opTypes[i] == 1)
                {
                    payload = this.txExecution.ReadPayload;
                    if (payload != null)
                    {
                        this.txExecution.Update("ycsb_table", recordKey, updatePayload);
                    }
                }

                this.txExecution.Commit();
                if (this.txExecution.TxStatus == TxStatus.Committed)
                {
                    this.CommittedTxs++;
                }
                this.FinishedTxs++;
            }

            this.RunEndTicks = DateTime.Now.Ticks;

            this.AllRequestsFinished = true;

        }

        private CassandraSessionManager SessionManager
        {
            get
            {
                return CassandraSessionManager.Instance;
            }
        }

        public void CassandraReadOnly()
        {
            int indexBound = 200000;    // setting

            ISession session = this.SessionManager.GetSession(PartitionedCassandraVersionDb.DEFAULT_KEYSPACE);

            string read_only_cql_raw = "SELECT * FROM ycsb_table WHERE recordKey='{0}' AND versionKey=1";

            string read_only_cql = "SELECT * FROM ycsb_table WHERE recordKey=? AND versionKey=1";
            var readonlyStmt = session.Prepare(read_only_cql);

            BoundStatement[] cqls = new BoundStatement[this.taskCount];
            string[] raw_cqls = new string[this.taskCount];
            int[] intKeys = new int[this.taskCount];
            intKeys[0] = this.Partition;
            for (int i = 1; i < this.taskCount; i++)
            {
                if (intKeys[i - 1] + this.versionDb.PartitionCount >= indexBound)
                {
                    intKeys[i] = this.Partition;
                }
                else
                {
                    intKeys[i] = intKeys[i - 1] + this.versionDb.PartitionCount;
                }
            }
            for (int i=0; i<this.taskCount; i++)
            {
                raw_cqls[i] = string.Format(read_only_cql_raw, intKeys[i]);
                cqls[i] = readonlyStmt.Bind(intKeys[i].ToString());
            }


            this.RunBeginTicks = DateTime.Now.Ticks;

            for (int i = 0; i < this.taskCount; i++)
            {
                if (!this.Active)
                {
                    break;
                }

                //session.Execute(cqls[i]);
                session.Execute(raw_cqls[i]);

                this.CommittedTxs++;
                this.FinishedTxs++;
            }

            this.RunEndTicks = DateTime.Now.Ticks;

            this.AllRequestsFinished = true;

        }

        public void CassandraUpdateOnly()
        {
            int indexBound = 200000;    // setting
            int batchSize = 1;

            ISession session = this.SessionManager.GetSession(PartitionedCassandraVersionDb.DEFAULT_KEYSPACE);

            string update_only_cql_raw = "UPDATE tx_table SET status=0, commitTime=-1, isCommitTsOrLB=0 WHERE txId={0}";
            string update_only_cql = "UPDATE tx_table SET status=0, commitTime=-1, isCommitTsOrLB=0 WHERE txId=?";            
            var updateonlyStmt = session.Prepare(update_only_cql);

            string[] raw_cqls = new string[this.taskCount];
            BoundStatement[] cqls = new BoundStatement[this.taskCount];
            int[] intKeys = new int[this.taskCount];
            intKeys[0] = this.Partition;
            for (int i = 1; i < this.taskCount; i++)
            {
                if (intKeys[i - 1] + this.versionDb.PartitionCount >= indexBound)
                {
                    intKeys[i] = this.Partition;
                }
                else
                {
                    intKeys[i] = intKeys[i - 1] + this.versionDb.PartitionCount;
                }
            }
            for (int i = 0; i < this.taskCount; i++)
            {
                raw_cqls[i] = string.Format(update_only_cql_raw, intKeys[i]);
                cqls[i] = updateonlyStmt.Bind((long)intKeys[i]);
            }

            //int batchTotal = this.taskCount / batchSize;
            //BatchStatement[] bss = new BatchStatement[batchTotal];
            //for (int i=0; i<batchTotal; i++)
            //{
            //    bss[i] = new BatchStatement();
            //    for (int j=batchSize*i; j<batchSize*(i+1); j++)
            //    {
            //        bss[i].Add(cqls[j]);
            //    }
            //}

            this.RunBeginTicks = DateTime.Now.Ticks;

            //for (int i = 0; i < batchTotal; i++)
            //{
            //    if (!this.Active)
            //    {
            //        break;
            //    }

            //    session.Execute(bss[i]);

            //    this.CommittedTxs += batchSize;
            //    this.FinishedTxs += batchSize;
            //}

            for (int i = 0; i < this.taskCount; i++)
            {
                if (!this.Active)
                {
                    break;
                }

                //session.Execute(cqls[i]);
                session.Execute(raw_cqls[i]);

                this.CommittedTxs += 1;
                this.FinishedTxs += 1;
            }

            this.RunEndTicks = DateTime.Now.Ticks;

            this.AllRequestsFinished = true;
        }

        public void Execute2()
        {
            // Only pin cores on server
            // TransactionExecutor.PinThreadOnCores(this.Partition);

            this.RunBeginTicks = DateTime.Now.Ticks;
            while (this.workingSet.Count > 0 || this.workloadQueue.Count > 0)
            {
                //if (DateTime.Now.Ticks - this.RunBeginTicks > 100000000)
                //{
                //    Console.WriteLine("Dead Loop");
                //}
                // TransactionRequest txReq = this.workload.Peek();
                // Dequeue incoming tx requests until the working set is full.
                while (this.activeTxs.Count < this.workingSetSize)
                {
                    if (this.workloadQueue.Count == 0)
                    {
                        break;  
                    }
                    TransactionRequest txReq = this.workloadQueue.Dequeue();

                    if (this.activeTxs.ContainsKey(txReq.SessionId))
                    {
                        Tuple<TransactionExecution, Queue<TransactionRequest>> execTuple =
                            this.activeTxs[txReq.SessionId];

                        Queue<TransactionRequest> queue = execTuple.Item2;
                        queue.Enqueue(txReq);
                    }
                    else
                    {
                        if (txReq.OperationType == OperationType.Open)
                        {
                            Tuple<TransactionExecution, Queue<TransactionRequest>> newExecTuple =
                                this.AllocateNewTxExecution();

                            TransactionExecution txExec = newExecTuple.Item1;
                            txExec.Reset();
                            if (txReq.IsStoredProcedure)
                            {
                                // TODO: handle variable procedures?
                                if (txExec.Procedure == null)
                                {
                                    txExec.Procedure = StoredProcedureFactory.CreateStoredProcedure(
                                           txReq.ProcedureType, this.ResourceManager);
                                    txExec.Procedure.RequestQueue = newExecTuple.Item2;
                                }
                                txExec.Procedure.Reset();
                                txExec.Procedure.Start(txReq.SessionId, txReq.Workload);
                            }

                            this.activeTxs.Add(txReq.SessionId, newExecTuple);
                            this.workingSet.Add(txReq.SessionId);
                        }
                        // Requests targeting unopen sessions are disgarded. 
                    }
                    //Console.WriteLine(this.workingSet.Count);
                }

                for (int sid = 0; sid < this.workingSet.Count;)
                {
                    Tuple<TransactionExecution, Queue<TransactionRequest>> execTuple =
                        this.activeTxs[this.workingSet[sid]];

                    TransactionExecution txExec = execTuple.Item1;
                    Queue<TransactionRequest> queue = execTuple.Item2;

                    // A tx runtime is blocked when CurrProc is not null. 
                    // Keep executing a tx until it's blocked.
                    do
                    {
                        if (txExec.CurrentProc != null)
                        {
                            txExec.CurrentProc();
                        }
                        
                        if (txExec.CurrentProc == null && queue.Count > 0)
                        {
                            TransactionRequest opReq = queue.Dequeue();

                            bool received = false;
                            object payload = null;

                            switch (opReq.OperationType)
                            {
                                case OperationType.Read:
                                    {
                                        txExec.Read(opReq.TableId, opReq.RecordKey, out received, out payload);
                                        break;
                                    }
                                case OperationType.InitiRead:
                                    {
                                        txExec.ReadAndInitialize(opReq.TableId, opReq.RecordKey, out received, out payload);
                                        //txExec.ReadAndInitialize(opReq.TableId, opReq.RecordKey, out received, out payload);
                                        break;
                                    }
                                case OperationType.Insert:
                                    {
                                        txExec.Insert(opReq.TableId, opReq.RecordKey, opReq.Payload);
                                        //txExec.Insert(opReq.TableId, opReq.RecordKey, opReq.Payload);
                                        break;
                                    }
                                case OperationType.Update:
                                    {
                                        txExec.Update(opReq.TableId, opReq.RecordKey, opReq.Payload);
                                        break;
                                    }
                                case OperationType.Delete:
                                    {
                                        txExec.Delete(opReq.TableId, opReq.RecordKey, out payload);
                                        break;
                                    }
                                case OperationType.Close:
                                    {
                                        txExec.Commit();
                                        break;
                                    }
                                default:
                                    break;
                            }
                        }
                    } while (txExec.CurrentProc == null && txExec.Progress == TxProgress.Open && queue.Count > 0);

                    if (txExec.Progress == TxProgress.Close)
                    {
                        Tuple<TransactionExecution, Queue<TransactionRequest>> runtimeTuple = this.activeTxs[this.workingSet[sid]];
                        this.activeTxs.Remove(this.workingSet[sid]);
                        
                        this.txRuntimePool.Enqueue(runtimeTuple);
                        this.workingSet.RemoveAt(sid);
                        if (txExec.TxStatus == TxStatus.Committed)
                        {
                            this.CommittedTxs += 1;
                        }
                        this.FinishedTxs += 1;
                        // Console.WriteLine("Finished: {0}", this.FinishedTxs);
                    }
                    else
                    {
                        sid++;
                    }
                }

                // The implementation of the flush logic needs to be refined.
                if (this.flushTables != null && this.flushTables.Length > 0)
                {
                    if (this.versionDb is RedisVersionDb)
                    {
                        this.FlushInstances();
                    } 
                }
            }

            this.RunEndTicks = DateTime.Now.Ticks;
            this.AllRequestsFinished = true;

            // unmount the current partition
            //this.versionDb.PartitionMounted[this.Partition] = false;
            //if (this.countdownEvent != null)
            //{
            //    this.countdownEvent.Signal();
            //}

            if (this.flushTables != null && this.flushTables.Length > 0)
            {
                if (this.versionDb is RedisVersionDb)
                {
                    while (true)
                    {
                        bool stop = this.versionDb.HasAllPartitionsUnmounted();
                        if (stop)
                        {
                            break;
                        }
                        this.FlushInstances();
                    }
                }
            }
            //while (this.Active)
            //{
            //    if (this.flushTables != null && this.flushTables.Length > 0)
            //    {
            //        this.FlushInstances();
            //    }
            //}
        }

        public void ExecuteWithoutWorkload()
        {
            // Only pin cores on server
            // TransactionExecutor.PinThreadOnCores(this.Partition);

            // PinThreadOnCores(this.Partition);
            this.RunBeginTicks = DateTime.Now.Ticks;
            int remainedWorkloads = this.taskCount;
            while (this.workingSet.Count > 0 || remainedWorkloads > 0)
            {
                // Dequeue incoming tx requests until the working set is full.
                while (this.activeTxs.Count < this.workingSetSize)
                {
                    if (remainedWorkloads == 0)
                    {
                        break;
                    }

                    string sessionId = remainedWorkloads.ToString();
                    remainedWorkloads--;

                    Tuple<TransactionExecution, Queue<TransactionRequest>> newExecTuple =
                            this.AllocateNewTxExecution();

                    TransactionExecution txExec = newExecTuple.Item1;
                    txExec.Reset();
                    if (txExec.Procedure == null)
                    {
                        txExec.Procedure = StoredProcedureFactory.CreateStoredProcedure(
                               this.storedProcedureType, this.ResourceManager);
                        txExec.Procedure.RequestQueue = newExecTuple.Item2;
                    }
                    txExec.Procedure.Reset();

                    StoredProcedureWorkload.Reload(this.workload);
                    txExec.Procedure.Start(sessionId, this.workload);

                    this.activeTxs.Add(sessionId, newExecTuple);
                    this.workingSet.Add(sessionId);
                }

                for (int sid = 0; sid < this.workingSet.Count;)
                {
                    Tuple<TransactionExecution, Queue<TransactionRequest>> execTuple =
                        this.activeTxs[this.workingSet[sid]];

                    TransactionExecution txExec = execTuple.Item1;
                    Queue<TransactionRequest> queue = execTuple.Item2; 
                    do
                    {
                        if (txExec.CurrentProc != null)
                        {
                            txExec.CurrentProc();
                        }

                        if (txExec.CurrentProc == null && queue.Count > 0)
                        {
                            TransactionRequest opReq = queue.Dequeue();

                            bool received = false;
                            object payload = null;

                            switch (opReq.OperationType)
                            {
                                case OperationType.Read:
                                    {
                                        txExec.Read(opReq.TableId, opReq.RecordKey, out received, out payload);
                                        break;
                                    }
                                case OperationType.InitiRead:
                                    {
                                        txExec.ReadAndInitialize(opReq.TableId, opReq.RecordKey, out received, out payload);
                                        //txExec.ReadAndInitialize(opReq.TableId, opReq.RecordKey, out received, out payload);
                                        break;
                                    }
                                case OperationType.Insert:
                                    {
                                        txExec.Insert(opReq.TableId, opReq.RecordKey, opReq.Payload);
                                        //txExec.Insert(opReq.TableId, opReq.RecordKey, opReq.Payload);
                                        break;
                                    }
                                case OperationType.Update:
                                    {
                                        txExec.Update(opReq.TableId, opReq.RecordKey, opReq.Payload);
                                        break;
                                    }
                                case OperationType.Delete:
                                    {
                                        txExec.Delete(opReq.TableId, opReq.RecordKey, out payload);
                                        break;
                                    }
                                case OperationType.Close:
                                    {
                                        txExec.Commit();
                                        break;
                                    }
                                default:
                                    break;
                            }
                        }
                    } while (txExec.CurrentProc == null && txExec.Progress == TxProgress.Open && queue.Count > 0);

                    if (txExec.Progress == TxProgress.Close)
                    {
                        Tuple<TransactionExecution, Queue<TransactionRequest>> runtimeTuple = this.activeTxs[this.workingSet[sid]];
                        this.activeTxs.Remove(this.workingSet[sid]);

                        this.txRuntimePool.Enqueue(runtimeTuple);
                        this.workingSet.RemoveAt(sid);
                        if (txExec.TxStatus == TxStatus.Committed)
                        {
                            this.CommittedTxs += 1;
                        }
                        this.FinishedTxs += 1;
                    }
                    else
                    {
                        sid++;
                    }
                }

                // The implementation of the flush logic needs to be refined.
                if (this.flushTables != null && this.flushTables.Length > 0)
                {
                    if (this.versionDb is RedisVersionDb)
                    {
                        this.FlushInstances();
                    }
                }
            }

            this.RunEndTicks = DateTime.Now.Ticks;
            this.AllRequestsFinished = true;

            // unmount the current partition
            this.versionDb.PartitionMounted[this.Partition] = false;

            if (this.flushTables != null && this.flushTables.Length > 0)
            {
                while (true)
                {
                    bool stop = this.versionDb.HasAllPartitionsUnmounted();
                    if (stop)
                    {
                        break;
                    }
                    this.FlushInstances();
                }
            }
        }

        public void Execute()
        {
            HashSet<string> toRemoveSessions = new HashSet<string>();
           
            while (this.activeTxs.Count > 0 || this.workloadQueue.Count > 0)
            {
                foreach (string sessionId in activeTxs.Keys)
                {
                    Tuple<TransactionExecution, Queue<TransactionRequest>> execTuple =
                        this.activeTxs[sessionId];

                    TransactionExecution txExec = execTuple.Item1;
                    Queue<TransactionRequest> queue = execTuple.Item2;

                    // check if the transaction execution has been time out
                    if (this.txTimeoutSeconds != 0 && txExec.ExecutionSeconds > this.txTimeoutSeconds)
                    {
                        // TODO: Timeout, the request stack has been cleared in the abort method
                        // txExec.TimeoutAbort();
                    }

                    // If the transaction is at the initi status, the CurrentProc will be not null
                    // It will be covered in this case
                    if (txExec.CurrentProc != null)
                    {
                        txExec.CurrentProc();
                    }
                    else if (txExec.Progress == TxProgress.Read)
                    {
                        TransactionRequest readReq = queue.Peek();
                        bool received = false;
                        object payload = null;

                        if (readReq.OperationType == OperationType.Read)
                        {
                            txExec.Read(readReq.TableId, readReq.RecordKey, out received, out payload);
                        }
                        else if (readReq.OperationType == OperationType.InitiRead)
                        {
                            txExec.ReadAndInitialize(readReq.TableId, readReq.RecordKey, out received, out payload);
                        }

                        if (received)
                        {
                            queue.Dequeue();
                            txExec.Procedure.ReadCallback(readReq.TableId, readReq.RecordKey, payload);
                        }
                    }
                    else if (txExec.Progress == TxProgress.Open)
                    {
                        if (queue.Count == 0)
                        {
                            // No pending work to do for this tx.
                            continue;
                        }

                        TransactionRequest opReq = queue.Peek();

                        // To support Net 4.0
                        bool received = false;
                        object payload = null;

                        switch (opReq.OperationType)
                        {
                            case OperationType.Read:
                                {
                                    txExec.Read(opReq.TableId, opReq.RecordKey, out received, out payload);
                                    if (received)
                                    {
                                        queue.Dequeue();
                                        txExec.Procedure?.ReadCallback(opReq.TableId, opReq.RecordKey, payload);
                                    }
                                    break;
                                }
                            case OperationType.InitiRead:
                                {
                                    txExec.ReadAndInitialize(opReq.TableId, opReq.RecordKey, out received, out payload);
                                    if (received)
                                    {
                                        queue.Dequeue();
                                        txExec.Procedure?.ReadCallback(opReq.TableId, opReq.RecordKey, payload);
                                    }
                                    break;
                                }
                            case OperationType.Insert:
                                {
                                    queue.Dequeue();
                                    txExec.Insert(opReq.TableId, opReq.RecordKey, opReq.Payload);
                                    txExec.Procedure?.InsertCallBack(opReq.TableId, opReq.RecordKey, opReq.Payload);
                                    break;
                                }
                            case OperationType.Update:
                                {
                                    queue.Dequeue();
                                    txExec.Update(opReq.TableId, opReq.RecordKey, opReq.Payload);
                                    txExec.Procedure?.UpdateCallBack(opReq.TableId, opReq.RecordKey, opReq.Payload);
                                    break;
                                }
                            case OperationType.Delete:
                                {
                                    queue.Dequeue();
                                    txExec.Delete(opReq.TableId, opReq.RecordKey, out payload);
                                    txExec.Procedure?.DeleteCallBack(opReq.TableId, opReq.RecordKey, payload);
                                    break;
                                }
                            case OperationType.Close:
                                {
                                    // unable to check whether the commit has been finished, so pop the request here
                                    queue.Dequeue();
                                    txExec.Commit();
                                    break;
                                }
                            default:
                                break;
                        }
                    }
                    else if (txExec.Progress == TxProgress.Close)
                    {
                        toRemoveSessions.Add(sessionId);
                        if (txExec.TxStatus == TxStatus.Committed)
                        {
                            this.CommittedTxs += 1;
                        }
                        this.FinishedTxs += 1;
                        //this.SetProgressBar();
                    }
                }

                if (this.flushTables != null)
                {
                    this.FlushInstances();
                }

                foreach (string sessionId in toRemoveSessions)
                {
                    Tuple<TransactionExecution, Queue<TransactionRequest>> runtimeTuple = this.activeTxs[sessionId];
                    this.activeTxs.Remove(sessionId);
                    this.txRuntimePool.Enqueue(runtimeTuple);
                }
                toRemoveSessions.Clear();

                while (this.workloadQueue.Count > 0 && this.activeTxs.Count < this.workingSetSize)
                {
                    TransactionRequest txReq = workloadQueue.Dequeue();

                    switch (txReq.OperationType)
                    {
                        case OperationType.Open:
                            {
                                if (this.activeTxs.ContainsKey(txReq.SessionId))
                                {
                                    continue;
                                }

                                Tuple<TransactionExecution, Queue<TransactionRequest>> newExecTuple = 
                                    this.AllocateNewTxExecution();

                                TransactionExecution txExec = newExecTuple.Item1;
                                txExec.Reset();
                                if (txReq.IsStoredProcedure)
                                {
                                    // TODO: handle variable procedures?
                                    if (txExec.Procedure == null)
                                    {
                                        txExec.Procedure = StoredProcedureFactory.CreateStoredProcedure(
                                            txReq.ProcedureType, this.ResourceManager);
                                        txExec.Procedure.RequestQueue = newExecTuple.Item2;
                                    }
                                    txExec.Procedure.Reset();
                                    txExec.Procedure.Start(txReq.SessionId, txReq.Workload);
                                }

                                this.activeTxs.Add(txReq.SessionId, newExecTuple);
                                this.workingSet.Add(txReq.SessionId);
                                break;
                            }

                        default:
                            {
                                if (!this.activeTxs.ContainsKey(txReq.SessionId))
                                {
                                    continue;
                                }

                                Queue<TransactionRequest> queue = this.activeTxs[txReq.SessionId].Item2;
                                queue.Enqueue(txReq);
                                break;
                            }
                    }
                }
            }

            // Set the finish flag as true
            this.AllRequestsFinished = true;
            if (this.flushTables != null)
            {
                while (this.Active)
                {
                    this.FlushInstances();
                }
            }
        }

        public void ExecuteNoFlush_1()
        {
            TransactionExecution exec = new TransactionExecution(
                this.logStore,
                this.versionDb,
                null,
                this.txRange,
                this.Partition);

            string priorSessionId = "";

            while (this.workloadQueue.Count > 0)
            {
                TransactionRequest req = this.workloadQueue.Peek();

                switch (req.OperationType)
                {
                    case OperationType.Close:
                        this.workloadQueue.Dequeue();
                        priorSessionId = req.SessionId;
                        exec.Commit();
                        if (exec.TxStatus == TxStatus.Committed)
                        {
                            this.CommittedTxs++;
                        }
                        this.FinishedTxs++;
                        if (this.workloadQueue.Count > 0)
                        {
                            exec.Reset();
                        }
                        break;
                    default:
                        throw new TransactionException("Should not go to this!");
                        break;
                }
            }
            this.AllRequestsFinished = true;
        }

        // TODO: it's supposed to have some issues here, it should be fixed if you want to run it
        public void ExecuteNoFlush()
        {
            TransactionExecution exec = new TransactionExecution(
                this.logStore,
                this.versionDb,
                null,
                this.txRange,
                this.Partition);

            string priorSessionId = "";

            while (this.workloadQueue.Count > 0)
            {
                TransactionRequest req = this.workloadQueue.Peek();

                switch (req.OperationType)
                {
                    case OperationType.Open:
                        if (req.SessionId != priorSessionId)
                        {
                            if (priorSessionId == "" || exec.Progress == TxProgress.Close)
                            {
                                this.workloadQueue.Dequeue();
                                priorSessionId = req.SessionId;
                                exec.Reset();
                                while (exec.CurrentProc != null)
                                {
                                    exec.CurrentProc();
                                }
                            }
                            else
                            {
                                while (exec.CurrentProc != null)
                                {
                                    exec.CurrentProc();
                                }
                            }
                        }
                        else
                        {
                            this.workloadQueue.Dequeue();
                            priorSessionId = req.SessionId;
                        }
                        break;
                    case OperationType.Close:
                        this.workloadQueue.Dequeue();
                        priorSessionId = req.SessionId;
                        exec.Commit();
                        while (exec.CurrentProc != null)
                        {
                            exec.CurrentProc();
                        }
                        break;
                    default:
                        break;
                }
            }
        }

        private int round = 0;

        bool empty = true;

        private void FlushInstances()
        {
            for (int i = 0; i < this.flushTables.Length; i++)
            {
                this.versionDb.Visit(this.flushTables[i], this.Partition);
            }
        }
    }
}
