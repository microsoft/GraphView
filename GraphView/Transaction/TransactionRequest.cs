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

        /// <summary>
        /// The RangeOffset unit in different processes
        /// Like Process 0, offset = 0; Process 1, offset = RangeOffsetUnit; Process 2, offset = RangeOffsetUnit * 2
        /// </summary>
        internal static int RangeOffsetUnit = 8;

        internal static int RangeOffsetIndex = 0;

        private int localTxIndex = 0;

        internal static int GetRange(object txId)
        {
            int rangeWithOffset = (int)((long)txId / range);
            // In multiple processes, here the computed range is a logical offset within all processes.
            // Specific to a process, we should convert it to the local range to put the request into 
            // right visitor.
            // In the case, 6 workers runs in 2 processes(1-3 in process 1, 4-6 in process 2), RangeOffsetUnit=3. 
            // The TxId offset in worker 5 will be 4, which is a logical range considering all workers. But each process has 
            // only 3 workers, that is 3 visitors. Thus we convert it to the local offset as 4 - 3 = 1.
            // If all workers run in the same process, RangeOffsetIndex will be 0.
            return rangeWithOffset - RangeOffsetIndex * RangeOffsetUnit;
        }

        public TxRange(int start)
        {
            // If workers runs in server processes, we should also control their txId intervals to avoid repeated txIds.
            // For example, 6 workers runs in 2 processes(1-3 in process 1, 4-6 in process 2). Those workers belong to the 
            // same process will have different txId intervals by the parameter RangeStart(always starts from 0), like 
            // worker 1 starts from 0, worker 2 starts from 10000 etc. However, we should also control those workers' intervals
            // in various processes by parameter RangeOffsetIndex, otherwise, worker 1 and worker 4 will have the same RangeStart
            // in this case.Thus, here the RangeOffsetIndex of process 1 is 0, and it's 1 for process 2 to make intervals of workers
            // 1-6 as 0, 10000, ..., 40000, 50000.
            // If all workers are running in the same process, the RangeOffsetIndex will be the 
            // default value 0.
            int startWithOffset = start + RangeOffsetUnit * RangeOffsetIndex;
            this.RangeStart = startWithOffset * TxRange.range;
            //this.RangeStart = start * TxRange.range;
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
