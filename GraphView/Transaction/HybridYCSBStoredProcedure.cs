using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.Transaction
{
    class HybridYCSBWorkload : StoredProcedureWorkload
    {
        internal string TableId;

        internal object[] Keys;

        internal object[] Values;

        internal string[] Queries;

        internal int QueryCount;
        
        public HybridYCSBWorkload()
        {

        }

        public HybridYCSBWorkload(
            string tableId,
            object[] keys,
            object[] values,
            string[] queries)
        {
            this.TableId = tableId;
            this.Keys = keys;
            this.Values = values;
            this.Queries = queries;

            this.QueryCount = keys == null ? 0 : keys.Length;
        }

        public override void Set(StoredProcedureWorkload baseWorkload)
        {
            HybridYCSBWorkload workload = baseWorkload as HybridYCSBWorkload;
            int queryCount = workload.QueryCount;
            if (this.Keys == null)
            {
                this.Keys = new object[queryCount];
                this.Values = new object[queryCount];
                this.Queries = new string[queryCount];
            }

            this.TableId = workload.TableId;
            Array.Copy(workload.Keys, this.Keys, queryCount);
            Array.Copy(workload.Values, this.Values, queryCount);
            Array.Copy(workload.Queries, this.Queries, queryCount);
            this.QueryCount = workload.QueryCount;
        }
    }

    class HybridYCSBStoredProcedure : StoredProcedure
    {
        private string sessionId;

        private HybridYCSBWorkload workload;

        private TxResourceManager resourceManager;

        private Queue<TransactionRequest> txRequestGCQueue;

        private int localIndex;

        private string currentOper;

        public HybridYCSBStoredProcedure(TxResourceManager resourceManager = null)
        {
            this.txRequestGCQueue = new Queue<TransactionRequest>();
            this.workload = new HybridYCSBWorkload(null, null, null, null);
            this.resourceManager = resourceManager;
        }

        public override void Start(
            string sessionId,
            StoredProcedureWorkload workload)
        {
            this.sessionId = sessionId;
            HybridYCSBWorkload hybridWorkload = workload as HybridYCSBWorkload;
            this.workload.Set(hybridWorkload);

            this.Start();
        }

        public override void Start()
        {
            this.NextOperation();
        }

        public override void ReadCallback(string tableId, object recordKey, object payload)
        {
            switch (this.currentOper)
            {
                case "READ":
                    this.localIndex++;
                    if (this.localIndex >= this.workload.QueryCount)
                    {
                        this.Close();
                    }
                    else
                    {
                        this.NextOperation();
                    }
                    break;

                case "UPDATE":
                    TransactionRequest updateReq = this.resourceManager.TransactionRequest(
                            this.sessionId,
                            workload.TableId,
                            workload.Keys[localIndex],
                            workload.Values[localIndex],
                            OperationType.Update);
                    this.txRequestGCQueue.Enqueue(updateReq);
                    this.RequestQueue.Enqueue(updateReq);
                    break;
            }
        }

        public override void UpdateCallBack(string tableId, object recordKey, object newPayload)
        {
            this.localIndex++;
            if (this.localIndex >= this.workload.QueryCount)
            {
                this.Close();
            }
            else
            {
                this.NextOperation();
            }
        }

        private void Close()
        {
            TransactionRequest closeReq = this.resourceManager.TransactionRequest(
                            this.sessionId,
                            workload.TableId,
                            null,
                            null,
                            OperationType.Close);
            this.txRequestGCQueue.Enqueue(closeReq);
            this.RequestQueue.Enqueue(closeReq);
        }

        private void NextOperation()
        {
            this.currentOper = this.workload.Queries[this.localIndex];
            switch (this.currentOper)
            {
                case "READ":
                case "UPDATE":
                    TransactionRequest readReq = this.resourceManager.TransactionRequest(
                        this.sessionId,
                        workload.TableId,
                        workload.Keys[this.localIndex],
                        workload.Values[this.localIndex],
                        OperationType.Read);
                    this.txRequestGCQueue.Enqueue(readReq);
                    this.RequestQueue.Enqueue(readReq);

                    break;

                default:
                    throw new ArgumentException("wrong operations");
            }
        }

        public override void Reset()
        {
            this.sessionId = "";
            this.localIndex = 0;

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
