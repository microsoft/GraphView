namespace GraphView.Transaction
{
    using System;
    //using System.Collections.Concurrent;
    using System.Threading;
    using NonBlocking;

    internal class SingletonVersionDbVisitor : VersionDbVisitor
    {
        private readonly ConcurrentDictionary<long, TxTableEntry> txTable;

        public SingletonVersionDbVisitor(ConcurrentDictionary<long, TxTableEntry> txTable)
        {
            this.txTable = txTable;
        }

        internal override void Visit(GetTxEntryRequest req)
        {
            TxTableEntry txEntry = req.RemoteTxEntry;
            if (txEntry == null || req.TxId != txEntry.TxId)
            {
                if (!this.txTable.TryGetValue(req.TxId, out txEntry))
                {
                    throw new TransactionException("The specified tx does not exist.");
                }
                // return back the txEntry
                req.RemoteTxEntry = txEntry;
            }

            while (Interlocked.CompareExchange(ref txEntry.latch, 1, 0) != 0) ;
            TxTableEntry.CopyValue(txEntry, req.LocalTxEntry);
            Interlocked.Exchange(ref txEntry.latch, 0);

            req.Result = req.LocalTxEntry;
            req.Finished = true;
        }

        internal override void Visit(InsertTxIdRequest req)
        {
            TxTableEntry txEntry = req.RemoteTxEntry;
            if (txEntry == null)
            {
                if (!this.txTable.TryGetValue(req.TxId, out txEntry))
                {
                    throw new TransactionException("The specified txId does not exist.");
                }
            }

            while (Interlocked.CompareExchange(ref txEntry.latch, 1, 0) != 0) ;
            txEntry.Reset(req.TxId);
            Interlocked.Exchange(ref txEntry.latch, 0);

            req.Finished = true;
        }

        internal override void Visit(NewTxIdRequest req)
        {
            TxTableEntry txEntry = new TxTableEntry();
            req.Result = this.txTable.TryAdd(req.TxId, txEntry) ? true : false;
            req.RemoteTxEntry = txEntry;
            req.Finished = true;
        }

        internal override void Visit(RecycleTxRequest req)
        {
            // RecycleTxRequest will be called at the begining of tx, the remoteEntry is supposed to be null
            TxTableEntry txEntry = null;
            if (!this.txTable.TryGetValue(req.TxId, out txEntry))
            {
                txEntry = new TxTableEntry(req.TxId);
                if (!this.txTable.TryAdd(req.TxId, txEntry))
                {
                    req.Result = false;
                    req.Finished = true;
                    return;
                }
            }
     
            while (Interlocked.CompareExchange(ref txEntry.latch, 1, 0) != 0) ;
            txEntry.Reset();
            Interlocked.Exchange(ref txEntry.latch, 0);

            req.RemoteTxEntry = txEntry;
            req.Result = true;
            req.Finished = true;
        }

        internal override void Visit(RemoveTxRequest req)
        {
            TxTableEntry te = null;
            this.txTable.TryRemove(req.TxId, out te);
            if (te != null)
            {
                req.Result = true;
            }
            else
            {
                req.Result = false;
            }
            req.Finished = true;
        }

        internal override void Visit(SetCommitTsRequest req)
        {
            TxTableEntry txEntry = req.RemoteTxEntry;
            if (txEntry == null)
            {
                if (!this.txTable.TryGetValue(req.TxId, out txEntry))
                {
                    throw new TransactionException("The specified tx does not exist.");
                }
            }

            while (Interlocked.CompareExchange(ref txEntry.latch, 1, 0) != 0) ;
            // read and write
            long commitTime = Math.Max(req.ProposedCommitTs, txEntry.CommitLowerBound);
            txEntry.CommitTime = commitTime;

            Interlocked.Exchange(ref txEntry.latch, 0);

            req.Result = commitTime;
            req.Finished = true;
        }

        internal override void Visit(UpdateCommitLowerBoundRequest req)
        {
            TxTableEntry txEntry = req.RemoteTxEntry;
            if (txEntry == null)
            {
                if (!this.txTable.TryGetValue(req.TxId, out txEntry))
                {
                    throw new TransactionException("The specified tx does not exist.");
                }
            }

            while (Interlocked.CompareExchange(ref txEntry.latch, 1, 0) != 0) ;
            long commitTime = txEntry.CommitTime;
            if (commitTime == TxTableEntry.DEFAULT_COMMIT_TIME &&
                txEntry.CommitLowerBound < req.CommitTsLowerBound)
            {
                txEntry.CommitLowerBound = req.CommitTsLowerBound;
            }
            Interlocked.Exchange(ref txEntry.latch, 0);

            req.Result = commitTime;
            req.Finished = true;
        }

        internal override void Visit(UpdateTxStatusRequest req)
        {
            TxTableEntry txEntry = req.RemoteTxEntry;
            if (txEntry == null)
            {
                if (!this.txTable.TryGetValue(req.TxId, out txEntry))
                {
                    throw new TransactionException("The specified tx does not exist.");
                }
            }

            txEntry.Status = req.TxStatus;

            req.Result = null;
            req.Finished = true;
        }
    }
}
