
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;

    internal class TransactionRequest
    {
        internal string SessionId { get; set; }
        internal string TableId { get; set; }
        internal object RecordKey { get; set; }
        internal object Payload { get; set; }
        internal OperationType OperationType;

        internal TxCallBack Callback { get; set; }
    }

    internal enum OperationType
    {
        Open,
        Insert,
        Delete,
        Update,
        Read,
        InitiRead,
        Close,
    }

    internal delegate void TxCallBack(string tableId, object recordKey, object payload);

    internal class TxFunction
    {
        internal virtual void ReadCallback(string tableId, object recordKey, object payload) { }
        internal virtual void InsertCallBack(string tableId, object recordKey, object payload) { }
        internal virtual void DeleteCallBack(string tableId, object recordKey, object payload) { }
        internal virtual void UpdateCallBack(string tableId, object recordKey, object newPayload) { }
    }

    internal class TransactionExecutor
    {
        /// <summary>
        /// The size of current working transaction set
        /// </summary>
        private readonly int workingSetSize = 100;      // in terms of # of tx's

        /// <summary>
        /// A queue of workloads accepted from clients
        /// </summary>
        internal Queue<TransactionRequest> workload;

        private readonly VersionDb versionDb;
        private readonly ILogStore logStore;

        /// <summary>
        /// A list of (table Id, partition key) pairs, each of which represents a key-value instance. 
        /// This worker is responsible for processing key-value ops directed to the designated instances.
        /// </summary>
        private List<Tuple<string, int>> partitionedInstances;

        private Dictionary<string, Tuple<TransactionExecution, Queue<TransactionRequest>>> activeTxs;

        public TransactionExecutor(
            VersionDb versionDb, 
            ILogStore logStore, 
            Queue<TransactionRequest> workload = null, 
            List<Tuple<string, int>> instances = null)
        {
            this.versionDb = versionDb;
            this.logStore = logStore;
            this.workload = workload ?? new Queue<TransactionRequest>();
            this.partitionedInstances = instances;
        }

        internal void Execute()
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

                    if (queue.Count == 0)
                    {
                        // No pending work to do for this tx.
                        continue;
                    }

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
                            readReq.Callback(readReq.TableId, readReq.RecordKey, payload);
                        }
                    }
                    else if (txExec.Progress == TxProgress.Open)
                    {
                        TransactionRequest opReq = queue.Peek();

                        switch(opReq.OperationType)
                        {
                            case OperationType.Read:
                                {
                                    txExec.Read(opReq.TableId, opReq.RecordKey, out bool received, out object payload);
                                    if (received)
                                    {
                                        queue.Dequeue();
                                        opReq.Callback(opReq.TableId, opReq.RecordKey, payload);
                                    }
                                    break;
                                }
                            case OperationType.InitiRead:
                                {
                                    txExec.ReadAndInitialize(opReq.TableId, opReq.RecordKey, out bool received, out object payload);
                                    if (received)
                                    {
                                        queue.Dequeue();
                                        opReq.Callback(opReq.TableId, opReq.RecordKey, payload);
                                    }
                                    break;
                                }
                            case OperationType.Insert:
                                {
                                    queue.Dequeue();
                                    txExec.Insert(opReq.TableId, opReq.RecordKey, opReq.Payload);
                                    opReq.Callback(opReq.TableId, opReq.RecordKey, opReq.Payload);
                                    break;
                                }
                            case OperationType.Update:
                                {
                                    queue.Dequeue();
                                    txExec.Update(opReq.TableId, opReq.RecordKey, opReq.Payload);
                                    opReq.Callback(opReq.TableId, opReq.RecordKey, opReq.Payload);
                                    break;
                                }
                            case OperationType.Delete:
                                {
                                    queue.Dequeue();
                                    txExec.Delete(opReq.TableId, opReq.RecordKey, out object payload);
                                    opReq.Callback(opReq.TableId, opReq.RecordKey, payload);
                                    break;
                                }
                            case OperationType.Close:
                                {
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
                    }
                }

                if (this.partitionedInstances != null)
                {
                    foreach (Tuple<string, int> kvIns in this.partitionedInstances)
                    {
                        string tableId = kvIns.Item1;
                        int partitionKey = kvIns.Item2;

                        this.versionDb.Visit(tableId, partitionKey);
                    }
                }

                foreach (string sessionId in toRemoveSessions)
                {
                    this.activeTxs.Remove(sessionId);
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

                                TransactionExecution exec = new TransactionExecution(this.logStore, this.versionDb);
                                this.activeTxs[txReq.SessionId] = 
                                    Tuple.Create(exec, new Queue<TransactionRequest>());
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
        }

        internal void Read(string sessionId, string tableId, object recordKey)
        {
            TxFunction func = new TxFunction();

            this.workload.Enqueue(new TransactionRequest()
            {
                SessionId = sessionId,
                OperationType = OperationType.Read,
                TableId = tableId,
                RecordKey = recordKey,
                Callback = new TxCallBack(func.ReadCallback)
            });
        }

        internal void Insert(string sessionId, string tableId, object recordKey, object payload)
        {
            TxFunction func = new TxFunction();

            this.workload.Enqueue(new TransactionRequest()
            {
                SessionId = sessionId,
                OperationType = OperationType.Insert,
                TableId = tableId,
                RecordKey = recordKey,
                Payload = payload,
                Callback = new TxCallBack(func.InsertCallBack)
            });
        }

        internal void Delete(string sessionId, string tableId, object recordKey)
        {
            TxFunction func = new TxFunction();

            this.workload.Enqueue(new TransactionRequest()
            {
                SessionId = sessionId,
                OperationType = OperationType.Delete,
                TableId = tableId,
                RecordKey = recordKey,
                Callback = new TxCallBack(func.DeleteCallBack)
            });
        }

        internal void Update(string sessionId, string tableId, object recordKey, object payload)
        {
            TxFunction func = new TxFunction();

            this.workload.Enqueue(new TransactionRequest()
            {
                SessionId = sessionId,
                OperationType = OperationType.Update,
                TableId = tableId,
                RecordKey = recordKey,
                Payload = payload,
                Callback = new TxCallBack(func.UpdateCallBack)
            });
        }
    }
}
