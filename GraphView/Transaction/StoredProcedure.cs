
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
    public class StoredProcedure
    {
        internal Queue<TransactionRequest> RequestQueue { get; set; }

        public StoredProcedure()
        {
        }

        public virtual void Start() { }

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
    }
}
