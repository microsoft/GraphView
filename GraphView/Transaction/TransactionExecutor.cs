
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Threading;

    public class TransactionRequest
    {
        internal string SessionId { get; set; }
        internal string TableId { get; set; }
        internal object RecordKey { get; set; }
        internal object Payload { get; set; }
        internal OperationType OperationType;

        internal StoredProcedure Procedure { get; set; }

        public TransactionRequest(
            string sessionId, 
            string tableId, 
            string recordKey, 
            string payload, 
            OperationType operationType)
        {
            this.SessionId = sessionId;
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.Payload = payload;
            this.OperationType = operationType;
        }

        public TransactionRequest(string sessionId, StoredProcedure procedure)
        {
            this.SessionId = sessionId;
            this.Procedure = procedure;
            this.OperationType = OperationType.Open;
        }

        public TransactionRequest() { }
    }

    public enum OperationType
    {
        Open,
        Insert,
        Delete,
        Update,
        Read,
        InitiRead,
        Close,
    }

    internal class TxRange
    {
        /// <summary>
        /// A tx executor, run by a thread, is assigned to a range (1,000,000) for tx Ids.
        /// Txs initiated by one executor all fall in the range. 
        /// When a new tx is created and the tx Id leads to a collusion, 
        /// if the conflicting tx has long finished, the old tx Id is recycled to the new tx.
        /// </summary>
        internal long RangeStart { get; }

        internal static readonly long range = 100000;

        private int localTxIndex = 0;

        public TxRange(int start)
        {
            this.RangeStart = start * TxRange.range;
        }

        internal long NextTxCandidate()
        {
            long candidateId = this.RangeStart + this.localTxIndex++;
            if (this.localTxIndex >= TxRange.range)
            {
                this.localTxIndex = 0;
            }

            return candidateId;
        }

    }

    public class TransactionExecutor
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

        private TxRange txRange;

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

        public TransactionExecutor(
            VersionDb versionDb,
            ILogStore logStore,
            Queue<TransactionRequest> workload = null,
            List<Tuple<string, int>> instances = null,
            int startRange = -1,
            int txTimeoutSeconds = 0)
        {
            this.versionDb = versionDb;
            this.logStore = logStore;
            this.workload = workload ?? new Queue<TransactionRequest>();
            this.activeTxs = new Dictionary<string, Tuple<TransactionExecution, Queue<TransactionRequest>>>();
            this.partitionedInstances = instances;
            this.txTimeoutSeconds = txTimeoutSeconds;
            this.GarbageQueueTxId = new Queue<long>();
            this.GarbageQueueFinishTime = new Queue<long>();
            this.txRange = startRange < 0 ? null : new TxRange(startRange);
            this.ResourceManager = new TxResourceManager();
            this.txRuntimePool = new Queue<Tuple<TransactionExecution, Queue<TransactionRequest>>>();
        }

        // add executor id
        public TransactionExecutor(
            VersionDb versionDb,
            ILogStore logStore,
            int executorId,
            Queue<TransactionRequest> workload = null,
            List<Tuple<string, int>> instances = null,
            int txTimeoutSeconds = 0)
        {
            this.versionDb = versionDb;
            this.logStore = logStore;
            this.executorId = executorId;
            this.workload = workload ?? new Queue<TransactionRequest>();
            this.activeTxs = new Dictionary<string, Tuple<TransactionExecution, Queue<TransactionRequest>>>();
            this.partitionedInstances = instances;
            this.txTimeoutSeconds = txTimeoutSeconds;           
        }

        public void SetProgressBar()
        {
            if (this.FinishedTxs % 1000 == 0)
            {
                Console.WriteLine("Executor {0}:\t Finished Txs: {1}", this.executorId, this.FinishedTxs);
            }
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
                        if (txExec.isCommitted)
                        {
                            this.CommittedTxs += 1;
                        }
                        this.FinishedTxs += 1;
                        //this.SetProgressBar();
                    }
                }

                if (this.partitionedInstances != null)
                {
                    this.FlushInstances();
                }

                foreach (string sessionId in toRemoveSessions)
                {
                    Tuple<TransactionExecution, Queue<TransactionRequest>> runtime = this.activeTxs[sessionId];
                    this.activeTxs.Remove(sessionId);
                    this.txRuntimePool.Enqueue(runtime);
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

                                TransactionExecution exec = null;
                                if (this.txRuntimePool.Count > 0)
                                {
                                    Tuple<TransactionExecution, Queue<TransactionRequest>> runtime = 
                                        this.txRuntimePool.Dequeue();

                                    exec = runtime.Item1;
                                    Queue<TransactionRequest> reqQueue = runtime.Item2;

                                    reqQueue.Clear();
                                    if (txReq.Procedure != null)
                                    {
                                        txReq.Procedure.RequestQueue = reqQueue;
                                    }

                                    exec.Reset(txReq.Procedure);
                                    this.activeTxs[txReq.SessionId] = Tuple.Create(exec, reqQueue);
                                }
                                else
                                {
                                    exec = new TransactionExecution(
                                        this.logStore, 
                                        this.versionDb, 
                                        txReq.Procedure, 
                                        this.GarbageQueueTxId, 
                                        this.GarbageQueueFinishTime, 
                                        this.txRange,
                                        this);

                                    Queue<TransactionRequest> reqQueue = new Queue<TransactionRequest>();
                                    if (txReq.Procedure != null)
                                    {
                                        txReq.Procedure.RequestQueue = reqQueue;
                                    }

                                    this.activeTxs[txReq.SessionId] = Tuple.Create(exec, reqQueue);
                                }

                                exec.Procedure?.Start();
                                break;
                            }
                        default:
                            {
                                if (this.activeTxs.ContainsKey(txReq.SessionId))
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
            if (this.partitionedInstances != null)
            {
                while (this.Active)
                {
                    this.FlushInstances();
                }
            }
        }

        private void FlushInstances()
        {
            foreach (Tuple<string, int> tuple in this.partitionedInstances)
            {
                string tableId = tuple.Item1;
                int partition = tuple.Item2;
                this.versionDb.Visit(tableId, partition);
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
