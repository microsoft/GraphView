using GraphView.Transaction;
using System.Collections.Generic;

namespace TransactionBenchmarkTest.YCSB
{
    class YCSBWorkload : StoredProcedureWorkload
    {
        internal string TableId;
        internal string Key;
        internal string Value;
        internal string Type;

        public YCSBWorkload(
            string type, 
            string tableId, 
            string key, 
            string value)
        {
            this.TableId = tableId;
            this.Key = key;
            this.Value = value;
            this.Type = type;
        }

        public override string ToString()
        {
            return string.Format("key={0},value={1},type={2},tableId={3}",
                this.Key, this.Value, this.Type, this.TableId);
        }
    }

    class YCSBStoredProcedure : StoredProcedure
    {
        internal static bool ONLY_CLOSE = false;

        private string sessionId;

        private YCSBWorkload workload;

        private TxResourceManager resourceManager;

        private Queue<TransactionRequest> txRequestGCQueue;

        public YCSBStoredProcedure(TxResourceManager resourceManager = null)
        {
            this.txRequestGCQueue = new Queue<TransactionRequest>();
            this.resourceManager = resourceManager;
        }

        public override void Start(
            string sessionId, 
            StoredProcedureWorkload workload)
        {
            this.sessionId = sessionId;
            this.workload = workload as YCSBWorkload;
            this.Start();
        }

        public override void Start()
        {
            if (!YCSBStoredProcedure.ONLY_CLOSE)
            {
                if (workload.Type == "INSERT")
                {
                    TransactionRequest initiReadReq = this.resourceManager.TransactionRequest(
                        this.sessionId,
                        workload.TableId, 
                        workload.Key, 
                        workload.Value, 
                        OperationType.InitiRead);
                    this.txRequestGCQueue.Enqueue(initiReadReq);
                    this.RequestQueue.Enqueue(initiReadReq);
                }
                else if (workload.Type == "READ")
                {
                    TransactionRequest readReq = this.resourceManager.TransactionRequest(
                        this.sessionId,
                        workload.TableId,
                        workload.Key,
                        workload.Value,
                        OperationType.Read);
                    this.txRequestGCQueue.Enqueue(readReq);
                    this.RequestQueue.Enqueue(readReq);
                }
                else if (workload.Type == "UPDATE" || workload.Type == "DELETE")
                {
                    TransactionRequest updateReq = this.resourceManager.TransactionRequest(
                        this.sessionId,
                        workload.TableId,
                        workload.Key,
                        workload.Value,
                        OperationType.Read);
                    this.txRequestGCQueue.Enqueue(updateReq);
                    this.RequestQueue.Enqueue(updateReq);
                }
                else
                {
                    this.Close();
                }
            }
            else
            {
                this.Close();
            }
        }

        public override void ReadCallback(string tableId, object recordKey, object payload)
        {
            switch (workload.Type)
            {
                case "READ":
                    // close the transaction
                    this.Close();
                    break;
                case "UPDATE":
                    if (payload != null)
                    {
                        TransactionRequest updateReq = this.resourceManager.TransactionRequest(
                            this.sessionId,
                            workload.TableId,
                            workload.Key,
                            workload.Value,
                            OperationType.Update);
                        this.txRequestGCQueue.Enqueue(updateReq);
                        this.RequestQueue.Enqueue(updateReq);
                    }
                    else
                    {
                        this.Close();
                    }
                    break;
                case "DELETE":
                    if (payload != null)
                    {
                        TransactionRequest deleteReq = this.resourceManager.TransactionRequest(
                            this.sessionId,
                            workload.TableId,
                            workload.Key,
                            workload.Value,
                            OperationType.Delete);
                        this.txRequestGCQueue.Enqueue(deleteReq);
                        this.RequestQueue.Enqueue(deleteReq);
                    }
                    else
                    {
                        this.Close();
                    }
                    break;
                case "INSERT":
                    if (payload == null)
                    {
                        TransactionRequest insertReq = this.resourceManager.TransactionRequest(
                            this.sessionId,
                            workload.TableId,
                            workload.Key,
                            workload.Value,
                            OperationType.Insert);
                        this.txRequestGCQueue.Enqueue(insertReq);
                        this.RequestQueue.Enqueue(insertReq);
                    }
                    else
                    {
                        this.Close();
                    }
                    break;
                default:
                    this.Close();
                    break;
            }
        }

        public override void UpdateCallBack(string tableId, object recordKey, object newPayload)
        {
            this.Close();
        }

        public override void DeleteCallBack(string tableId, object recordKey, object payload)
        {
            this.Close();
        }

        public override void InsertCallBack(string tableId, object recordKey, object payload)
        {
            this.Close();
        }

        private void Close()
        {
            TransactionRequest closeReq = this.resourceManager.TransactionRequest(
                            this.sessionId,
                            workload.TableId,
                            workload.Key,
                            workload.Value,
                            OperationType.Close);
            this.txRequestGCQueue.Enqueue(closeReq);
            this.RequestQueue.Enqueue(closeReq);
        }

        public override void Reset()
        {
            this.sessionId = "";
            this.workload = null;

            TransactionRequest transReq = null;
            while (this.txRequestGCQueue.Count > 0)
            {
                transReq = this.txRequestGCQueue.Dequeue();
                this.resourceManager.RecycleTransRequest(ref transReq);
            }

            base.Reset();
        }
    }
}
