namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    interface ITransactionStore
    {
        Transaction ReadTransaction(long txId);

        bool InsertTransaction(Transaction tx);

        bool UpdateTransaction(long txId, Transaction newTx, Transaction oldTx);

        bool DeleteTransaction(long txId);

        long SetCommitTime(long txId, long lowerBound, long upperBound);

        bool SetMaxCommitLowerBound(long txId, long lowerBound);
    }
}
