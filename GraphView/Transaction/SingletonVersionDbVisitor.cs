using System;
using NonBlocking;
using System.Threading;
//using System.Collections.Concurrent;

namespace GraphView.Transaction
{
    internal class SingletonVersionDbVisitor : VersionDbVisitor
    {
        private readonly ConcurrentDictionary<long, TxTableEntry> txTable;
        private readonly TxResourceManager resourceManager;

        public SingletonVersionDbVisitor(ConcurrentDictionary<long, TxTableEntry> txTable,
            TxResourceManager resourceManager)
        {
            this.txTable = txTable;
            this.resourceManager = resourceManager;
        }

        internal override void Visit(GetTxEntryRequest req)
        {
            TxTableEntry txEntry = null;
            if (!this.txTable.TryGetValue(req.TxId, out txEntry))
            {
                throw new TransactionException("The specified tx does not exist.");
            }

            while (Interlocked.CompareExchange(ref txEntry.latch, 1, 0) != 0) ;
            TxTableEntry.CopyValue(txEntry, req.TxEntry);
            Interlocked.Exchange(ref txEntry.latch, 0);

            req.Result = req.TxEntry;
            req.Finished = true;
        }

        internal override void Visit(InsertTxIdRequest req)
        {
            TxTableEntry txEntry = null;
            if (!this.txTable.TryGetValue(req.TxId, out txEntry))
            {
                throw new TransactionException("The specified txId does not exist.");
            }

            while (Interlocked.CompareExchange(ref txEntry.latch, 1, 0) != 0) ;

            txEntry.TxId = req.TxId;
            txEntry.Status = TxStatus.Ongoing;
            txEntry.CommitTime = TxTableEntry.DEFAULT_COMMIT_TIME;
            txEntry.CommitLowerBound = TxTableEntry.DEFAULT_LOWER_BOUND;

            Interlocked.Exchange(ref txEntry.latch, 0);

            req.Finished = true;
        }

        internal override void Visit(NewTxIdRequest req)
        {
            TxTableEntry txEntry = new TxTableEntry();
            req.Result = this.txTable.TryAdd(req.TxId, txEntry) ? 1L : 0L;
            req.Finished = true;
        }

        internal override void Visit(RecycleTxRequest req)
        {
            TxTableEntry txEntry = null;
            if (this.txTable.TryGetValue(req.TxId, out txEntry))
            {
                while (Interlocked.CompareExchange(ref txEntry.latch, 1, 0) != 0) ;
                // Reset includes multiple commands
                txEntry.Reset();
                Interlocked.Exchange(ref txEntry.latch, 0);

                req.Result = 1L;
            }
            else
            {
                req.Result = 0L;
            }
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
            TxTableEntry txEntry = null;
            if (!this.txTable.TryGetValue(req.TxId, out txEntry))
            {
                throw new TransactionException("The specified tx does not exist.");
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
            TxTableEntry txEntry = null;
            if (!this.txTable.TryGetValue(req.TxId, out txEntry))
            {
                throw new TransactionException("The specified tx does not exist.");
            }

            // read and write, the latch is necessnary
            // TODO: is it required?
            while (Interlocked.CompareExchange(ref txEntry.latch, 1, 0) != 0) ;

            txEntry.CommitLowerBound = req.CommitTsLowerBound;
            long commitTime = txEntry.CommitTime;

            Interlocked.Exchange(ref txEntry.latch, 0);

            req.Result = commitTime;
            req.Finished = true;
        }

        internal override void Visit(UpdateTxStatusRequest req)
        {
            TxTableEntry txEntry = null;
            if (!this.txTable.TryGetValue(req.TxId, out txEntry))
            {
                throw new TransactionException("The specified tx does not exist.");
            }

            txEntry.Status = req.TxStatus;

            req.Result = null;
            req.Finished = true;
        }
    }
}
