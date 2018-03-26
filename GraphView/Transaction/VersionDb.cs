
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using Newtonsoft.Json.Linq;
    using RecordRuntime;

    /// <summary>
    /// A version Db for concurrency control.
    /// </summary>
    public abstract partial class VersionDb
    {

    }

    public abstract partial class VersionDb : ITransactionStore
    {
        public abstract long GetCommitTime(long txId);
        public abstract TxStatus GetTxStatusByTxId(long txId);
        public abstract void InsertNewTx(long txId);
        public abstract long SetCommitTime(long txId, long lowerBound, long upperBound);
        public abstract bool SetMaxCommitLowerBound(long txId, long lowerBound);
        public abstract void UpdateTxStatus(long txId);
    }
}
