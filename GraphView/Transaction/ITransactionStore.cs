namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    interface ITransactionStore
    {
        void InsertNewTx(long txId);

        TxStatus GetTxStatusByTxId(long txId);

        void UpdateTxStatus(long txId);

        long SetCommitTime(long txId, long lowerBound, long upperBound);

        long GetCommitTime(long txId);

        bool SetMaxCommitLowerBound(long txId, long lowerBound);
    }
}
