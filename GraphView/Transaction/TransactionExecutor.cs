using TransactionBenchmarkTest.YCSB;

namespace GraphView.Transaction
{
    using Cassandra;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;

    internal class TransactionExecutor
    {
        private int executorId = 0;

        /// <summary>
        /// The size of current working transaction set
        /// </summary>
        private readonly int workingSetSize = 1;      // in terms of # of tx's

        /// <summary>
        /// A queue of workloads accepted from clients
        /// </summary>
        internal Queue<TransactionRequest> workload;

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

        /// <summary>
        /// A list of (table Id, partition key) pairs, each of which represents a key-value instance. 
        /// This worker is responsible for processing key-value ops directed to the designated instances.
        /// </summary>
        private List<Tuple<string, int>> partitionedInstances;

        private List<string> workingSet;

        private TxRange txRange;

        private string[] flushTables;

        /// <summary>
        /// The partition of current executor flushed
        /// </summary>
        internal int Partition { get; private set; }

        /// <summary>
        /// A queue of finished txs (committed or aborted) with their wall-clock time to be cleaned.
        /// A tx is cleaned after a certain period after it finishes post processing.
        /// </summary>
        //internal Queue<Tuple<long, long>> GarbageQueue { get; }
        internal Queue<long> GarbageQueueTxId { get; }
        internal Queue<long> GarbageQueueFinishTime { get; }

        internal TxRange TxRange { get; }

        internal TxResourceManager ResourceManager { get; }

        //internal static readonly long elapsed = 10000000L;      // 1 sec
        internal static readonly long elapsed = 0L;      // 1 sec

        private ManualResetEventSlim startEventSlim;

        private CountdownEvent countdownEvent;

        private TransactionExecution txExecution;

        private int taskCount;

        private string[] YCSBKeys;

        internal long RunBeginTicks { get; set; }
        internal long RunEndTicks { get; set; }

        public TransactionExecutor(
            VersionDb versionDb,
            ILogStore logStore,
            Queue<TransactionRequest> workload = null,
            int partition = 0,
            int startRange = -1,
            int txTimeoutSeconds = 0,
            TxResourceManager resourceManager = null,
            string[] flushTables = null,
            ManualResetEventSlim startEventSlim = null,
            CountdownEvent countdownEvent = null,
            string[] YCSBKeys = null,
            int taskCount = 0)
        {
            this.versionDb = versionDb;
            this.logStore = logStore;
            this.workload = workload ?? new Queue<TransactionRequest>();
            this.activeTxs = new Dictionary<string, Tuple<TransactionExecution, Queue<TransactionRequest>>>();

            this.txTimeoutSeconds = txTimeoutSeconds;
            this.GarbageQueueTxId = new Queue<long>();
            this.GarbageQueueFinishTime = new Queue<long>();
            this.txRange = startRange < 0 ? null : new TxRange(startRange);
            this.ResourceManager = resourceManager == null ? new TxResourceManager() : resourceManager;
            this.txRuntimePool = new Queue<Tuple<TransactionExecution, Queue<TransactionRequest>>>();
            this.workingSet = new List<string>(this.workingSetSize);

            this.Partition = partition;
            this.flushTables = flushTables;


            this.startEventSlim = startEventSlim;
            this.countdownEvent = countdownEvent;


            this.txExecution = new TransactionExecution(this.logStore, this.versionDb, null,
                this.GarbageQueueTxId,this.GarbageQueueFinishTime, this.txRange, this);

            this.YCSBKeys = YCSBKeys;
            this.taskCount = taskCount;
        }

        // add executor id
        //public TransactionExecutor(
        //    VersionDb versionDb,
        //    ILogStore logStore,
        //    int executorId,
        //    Queue<TransactionRequest> workload = null,
        //    List<Tuple<string, int>> instances = null,
        //    int txTimeoutSeconds = 0)
        //{
        //    this.versionDb = versionDb;
        //    this.logStore = logStore;
        //    this.executorId = executorId;
        //    this.workload = workload ?? new Queue<TransactionRequest>();
        //    this.activeTxs = new Dictionary<string, Tuple<TransactionExecution, Queue<TransactionRequest>>>();
        //    this.partitionedInstances = instances;
        //    this.txTimeoutSeconds = txTimeoutSeconds;
        //    this.workingSet = new List<string>(this.workingSetSize);

        //    if (instances != null)
        //    {
        //        this.Partition = instances[0].Item2;
        //    }
        //}

        public void SetProgressBar()
        {
            if (this.FinishedTxs % 1000 == 0)
            {
                Console.WriteLine("Executor {0}:\t Finished Txs: {1}", this.executorId, this.FinishedTxs);
            }
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

                if (reqQueue.Count > 0)
                {
                    reqQueue.Clear();
                }
                return runtimeTuple;
            }
            else
            {
                exec = new TransactionExecution(
                    this.logStore,
                    this.versionDb,
                    null,
                    this.GarbageQueueTxId,
                    this.GarbageQueueFinishTime,
                    this.txRange,
                    this);

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
            foreach (TransactionRequest req in this.workload)
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
            int indexBound = this.YCSBKeys.Length;

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
            int indexBound = this.YCSBKeys.Length;
            string updatePayload = new String('a', 100);
            for (int i = 0; i < this.taskCount; i++)
            {
                //string recordKey = YCSBKeys[rand.Next(0, indexBound)];
                // int recordKey = rand.Next(0, indexBound);
                int recordKey = this.GenerateYCSBKey(rand.Next(0, indexBound), indexBound);
                Console.WriteLine("RecordKey is {0}", recordKey);
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
            // PinThreadOnCores(this.Partition);
            this.RunBeginTicks = DateTime.Now.Ticks;

            Random rand = new Random();
            bool received = false;
            object payload = null;
            // int indexBound = this.YCSBKeys.Length;
            int indexBound = this.taskCount;
            string updatePayload = new String('a', 100);
            for (int i = 0; i < this.taskCount; i++)
            {
                //string recordKey = YCSBKeys[rand.Next(0, indexBound)];
                // int recordKey = rand.Next(0, indexBound);
                // int recordKey = this.GenerateYCSBKey(rand.Next(0, indexBound), indexBound);
                int recordKey = i;
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
            int indexBound = this.YCSBKeys.Length;

            for (int i = 0; i < this.taskCount; i++)
            {
                if (!this.Active)
                {
                    break;
                }
                this.txExecution.Reset();

                string recordKey = (this.workload.Dequeue().Workload as YCSBWorkload).Key;                
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

                session.Execute(cqls[i]);
                //session.Execute(raw_cqls[i]);

                this.CommittedTxs++;
                this.FinishedTxs++;
            }

            this.RunEndTicks = DateTime.Now.Ticks;

            this.AllRequestsFinished = true;

        }

        public void CassandraUpdateOnly()
        {
            int indexBound = 200000;    // setting
            int batchSize = 100;

            ISession session = this.SessionManager.GetSession(PartitionedCassandraVersionDb.DEFAULT_KEYSPACE);

            string update_only_cql = "UPDATE tx_table SET status=0, commitTime=-1, isCommitTsOrLB=0 WHERE txId=?";            
            var updateonlyStmt = session.Prepare(update_only_cql);
            
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
                cqls[i] = updateonlyStmt.Bind((long)intKeys[i]);
            }
            int batchTotal = this.taskCount / batchSize;
            BatchStatement[] bss = new BatchStatement[batchTotal];
            for (int i=0; i<batchTotal; i++)
            {
                bss[i] = new BatchStatement();
                for (int j=batchSize*i; j<batchSize*(i+1); j++)
                {
                    bss[i].Add(cqls[j]);
                }
            }

            this.RunBeginTicks = DateTime.Now.Ticks;

            for (int i = 0; i < batchTotal; i++)
            {
                if (!this.Active)
                {
                    break;
                }

                session.Execute(bss[i]);

                this.CommittedTxs += batchSize;
                this.FinishedTxs += batchSize;
            }

            this.RunEndTicks = DateTime.Now.Ticks;

            this.AllRequestsFinished = true;

        }

        public void Execute2()
        {
            // Only pin cores on server
            // TransactionExecutor.PinThreadOnCores(this.Partition);
            if (this.startEventSlim != null)
            {
                this.startEventSlim.Wait();
            }

            this.RunBeginTicks = DateTime.Now.Ticks;
            while (this.workingSet.Count > 0 || this.workload.Count > 0)
            {
                //if (DateTime.Now.Ticks - this.RunBeginTicks > 50000000)
                //{
                //    Console.WriteLine(123);
                //}
                // TransactionRequest txReq = this.workload.Peek();
                // Dequeue incoming tx requests until the working set is full.
                while (this.activeTxs.Count < this.workingSetSize)
                {
                    if (this.workload.Count == 0)
                    {
                        break;  
                    }
                    TransactionRequest txReq = this.workload.Dequeue();

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
                                        txExec.ReadAndInitialize(opReq.TableId, opReq.RecordIntKey, out received, out payload);
                                        //txExec.ReadAndInitialize(opReq.TableId, opReq.RecordKey, out received, out payload);
                                        break;
                                    }
                                case OperationType.Insert:
                                    {
                                        txExec.Insert(opReq.TableId, opReq.RecordIntKey, opReq.Payload);
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
                    this.FlushInstances();
                }
            }

            this.RunEndTicks = DateTime.Now.Ticks;
            this.AllRequestsFinished = true;

            //if (this.countdownEvent != null)
            //{
            //    this.countdownEvent.Signal();
            //}

            //while (this.Active)
            //{
            //    if (this.flushTables != null && this.flushTables.Length > 0)
            //    {
            //        this.FlushInstances();
            //    }
            //}
        }

        public void Execute()
        {
            HashSet<string> toRemoveSessions = new HashSet<string>();
           
            while (this.activeTxs.Count > 0 || this.workload.Count > 0)
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

                while (this.workload.Count > 0 && this.activeTxs.Count < this.workingSetSize)
                {
                    TransactionRequest txReq = workload.Dequeue();

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
                this.GarbageQueueTxId,
                this.GarbageQueueFinishTime,
                this.txRange,
                this);

            string priorSessionId = "";

            while (this.workload.Count > 0)
            {
                TransactionRequest req = this.workload.Peek();

                switch (req.OperationType)
                {
                    case OperationType.Close:
                        this.workload.Dequeue();
                        priorSessionId = req.SessionId;
                        exec.Commit();
                        if (exec.TxStatus == TxStatus.Committed)
                        {
                            this.CommittedTxs++;
                        }
                        this.FinishedTxs++;
                        if (this.workload.Count > 0)
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
                this.GarbageQueueTxId,
                this.GarbageQueueFinishTime,
                this.txRange,
                this);

            string priorSessionId = "";

            while (this.workload.Count > 0)
            {
                TransactionRequest req = this.workload.Peek();

                switch (req.OperationType)
                {
                    case OperationType.Open:
                        if (req.SessionId != priorSessionId)
                        {
                            if (priorSessionId == "" || exec.Progress == TxProgress.Close)
                            {
                                this.workload.Dequeue();
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
                            this.workload.Dequeue();
                            priorSessionId = req.SessionId;
                        }
                        break;
                    case OperationType.Close:
                        this.workload.Dequeue();
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

        private void FlushInstances()
        {
            for (int i = 0; i < this.flushTables.Length; i++)
            {
                this.versionDb.Visit(this.flushTables[i], this.Partition);
            }
        }

		internal int RecycleCount = 0;
        internal int InsertNewTxCount = 0;
        
        public long CreateTransaction()
        {
            long txId = -1;

            while (txId < 0)
            {
                if (this.GarbageQueueTxId != null && this.GarbageQueueTxId.Count > 0)
                {
                    long rtId = this.GarbageQueueTxId.Peek();
                    long finishTime = this.GarbageQueueFinishTime.Peek();

                    if (DateTime.Now.Ticks - finishTime >= TransactionExecutor.elapsed &&
                        this.versionDb.RecycleTx(rtId))
                    {
						this.RecycleCount++;
                        this.GarbageQueueTxId.Dequeue();
                        this.GarbageQueueFinishTime.Dequeue();
                        txId = rtId;
                        break;
                    }
                }

                long proposedTxId = this.txRange.NextTxCandidate();
                txId = this.versionDb.InsertNewTx(proposedTxId);
                this.InsertNewTxCount++;
            }

			//return new Transaction(this.logStore, this.versionDb, txId, this.GarbageQueue);
			return txId;
        }
    }
}
