namespace GraphView.Transaction
{

    /// <summary>
    /// There will be two types TransactionRequest:
    /// (1) command request, with the sessionId, tableId, recordKey and payload
    /// (2) stored procedure request, with the sessionId, workload
    /// </summary>
    public class TransactionRequest : IResource
    {
        private bool inUse;
        internal string SessionId { get; set; }

        internal string TableId { get; set; }
        internal object RecordKey { get; set; }
        internal object Payload { get; set; }
        internal OperationType OperationType;

        /// <summary>
        /// The workload for StoredProcedure. StoredProcedure may have different 
        /// parameters, so here define a workload to store variables
        /// </summary>
        internal StoredProcedureWorkload Workload { get; set; }
        internal bool IsStoredProcedure { get; set; }

        internal StoredProcedureType ProcedureType { get; set; }

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
            this.IsStoredProcedure = false;
        }

        public TransactionRequest(
            string sessionId,
            StoredProcedureWorkload workload,
            StoredProcedureType type)
        {
            this.SessionId = sessionId;
            this.Workload = workload;
            this.OperationType = OperationType.Open;
            this.IsStoredProcedure = true;
            this.ProcedureType = type;
        }

        public TransactionRequest() { }

        public void Use()
        {
            this.inUse = true;
        }

        public bool IsActive()
        {
            return this.inUse;
        }

        public void Free()
        {
            this.inUse = false;
        }
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

        internal static readonly long range = 10000;

        private int localTxIndex = 0;

        internal static int GetRange(object txId)
        {
            return (int)((long)txId / range);
        }

        public TxRange(int start)
        {
            this.RangeStart = start * TxRange.range;
        }

        public void Reset()
        {
            this.localTxIndex = 0;
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
}
