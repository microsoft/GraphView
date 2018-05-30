using GraphView.Transaction;

namespace TransactionBenchmarkTest.YCSB
{
    class YCSBStoredProcedure : StoredProcedure
    {
        internal static bool ONLY_CLOSE = false;

        private string sessionId;

        private TxWorkload workload;

        public YCSBStoredProcedure(string sessionId, TxWorkload workload)
        {
            this.workload = workload;
        }

        public override void Start()
        {
            if (!YCSBStoredProcedure.ONLY_CLOSE)
            {
                if (workload.Type == "INSERT")
                {
                    TransactionRequest initiReadReq = new TransactionRequest(this.sessionId,
                        workload.TableId, workload.Key, workload.Value, OperationType.InitiRead);
                    this.RequestQueue.Enqueue(initiReadReq);
                }
                else if (workload.Type == "READ")
                {
                    TransactionRequest readReq = new TransactionRequest(this.sessionId,
                        workload.TableId, workload.Key, workload.Value, OperationType.Read);
                    this.RequestQueue.Enqueue(readReq);
                }
                else if (workload.Type == "UPDATE")
                {
                    TransactionRequest readReq = new TransactionRequest(this.sessionId,
                        workload.TableId, workload.Key, workload.Value, OperationType.Read);
                    this.RequestQueue.Enqueue(readReq);
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
                        TransactionRequest updateReq = new TransactionRequest(this.sessionId,
                            workload.TableId, workload.Key, workload.Value, OperationType.Update);
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
                        TransactionRequest deleteReq = new TransactionRequest(this.sessionId,
                            workload.TableId, workload.Key, workload.Value, OperationType.Delete);
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
                        TransactionRequest insertReq = new TransactionRequest(this.sessionId,
                            workload.TableId, workload.Key, workload.Value, OperationType.Insert);
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
            TransactionRequest closeReq = new TransactionRequest(this.sessionId,
                            workload.TableId, workload.Key, workload.Value, OperationType.Close);
            this.RequestQueue.Enqueue(closeReq);
        }
    }
}
