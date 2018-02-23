
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Data.Linq;

    /// <summary>
    /// An interface for the transaction table.
    /// </summary>
    public abstract class TransactionTable
    {
        internal virtual TxStatus GetTxStatusByTxId(long txId)
        {
            throw new NotImplementedException();
        }

        internal virtual void UpdateTxStatusByTxId(long txId, TxStatus txStatus)
        {
            throw new NotImplementedException();
        }

        internal virtual void InsertNewTx(long txId, long beginTimestamp)
        {
            throw new NotImplementedException();
        }

        internal virtual void UpdateTxEndTimestampByTxId(long txId, long endTimestamp)
        {
            throw new NotImplementedException();
        }
    }

    

}
