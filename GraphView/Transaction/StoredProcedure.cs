
namespace GraphView.Transaction
{
    using System.Collections.Generic;

    /// <summary>
    /// A stored procedure is a piece of code executing tx computation logic. 
    /// Reads/writes happended inside the procedure are translated into queued tx requests,
    /// which are processed by a tx executor. 
    /// User-defined procedures should inherit this class and overrides virtual methods, 
    /// to implement user-defined computation logic. 
    /// </summary>
    internal class StoredProcedure
    {
        internal Queue<TransactionRequest> RequestQueue { get; set; }
        /// <summary>
        /// stored procedure id
        /// </summary> 
        public int pid = 0;

        public StoredProcedure()
        {

        }

        public virtual void Start() { }

        public virtual void Start(
            string sessionId, 
            StoredProcedureWorkload workload)
        {
            
        }

        public virtual void ReadCallback(
            string tableId,
            object recordKey,
            object payload)
        { }

        public virtual void InsertCallBack(
            string tableId,
            object recordKey,
            object payload)
        { }

        public virtual void DeleteCallBack(
            string tableId,
            object recordKey,
            object payload)
        { }

        public virtual void UpdateCallBack(
            string tableId,
            object recordKey,
            object newPayload)
        { }

        internal virtual void InitializeCallBack(
            string tableId,
            string recordKey)
        { }

        public virtual void Reset()
        {
            while (this.RequestQueue.Count > 0)
            {
                this.RequestQueue.Dequeue();
            }
        }
    }


}
