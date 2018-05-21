using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.Transaction
{
    internal class SingletonVersionDbVisitor : VersionDbVisitor
    {
        private readonly NonBlocking.ConcurrentDictionary<long, TxTableEntry> txTable;
        private readonly TxResourceManager resourceManager;

        public SingletonVersionDbVisitor(NonBlocking.ConcurrentDictionary<long, TxTableEntry> txTable, 
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

            req.Result = txEntry;
            req.Finished = true;
        }

        internal override void Visit(InsertTxIdRequest req)
        {
            TxTableEntry txEntry = null;
            if (!this.txTable.TryGetValue(req.TxId, out txEntry))
            {
                throw new TransactionException("The specified txId does not exist.");
            }

            TxTableEntry newTxEntry = this.resourceManager.GetTxTableEntry();
            newTxEntry.TxId = req.TxId;
            newTxEntry.Status = TxStatus.Ongoing;
            newTxEntry.CommitTime = TxTableEntry.DEFAULT_COMMIT_TIME;
            newTxEntry.CommitLowerBound = TxTableEntry.DEFAULT_LOWER_BOUND;

            this.txTable.TryUpdate(req.TxId, newTxEntry, txEntry);
            req.Finished = true;
        }

        internal override void Visit(NewTxIdRequest req)
        {
            req.Result = this.txTable.TryAdd(req.TxId, null) ? 1L : 0L;
            req.Finished = true;
        }

        internal override void Visit(RecycleTxRequest req)
        {
            TxTableEntry txEntry = null;
            if (this.txTable.TryGetValue(req.TxId, out txEntry))
            {
                txEntry.Reset();
                req.Result = true;
            }
            else
            {
                req.Result = false;
            }
            req.Finished = true;
        }

        internal override void Visit(RemoveTxRequest req)
        {
            TxTableEntry te = null;
            this.txTable.TryRemove(req.TxId, out te);
            if (te != null)
            {
                this.resourceManager.RecycleTxTableEntry(ref te);
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

            TxTableEntry newTxEntry = null;
            while (txEntry.CommitTime < 0)
            {
                if (newTxEntry == null)
                {
                    newTxEntry = this.resourceManager.GetTxTableEntry();
                }

                newTxEntry.Status = txEntry.Status;
                newTxEntry.CommitTime = Math.Max(req.ProposedCommitTs, txEntry.CommitLowerBound);
                newTxEntry.CommitLowerBound = txEntry.CommitLowerBound;

                if (this.txTable.TryUpdate(req.TxId, newTxEntry, txEntry))
                {
                    this.resourceManager.RecycleTxTableEntry(ref txEntry);
                    txEntry = newTxEntry;
                }
                else
                {
                    this.txTable.TryGetValue(req.TxId, out txEntry);
                }
            }

            req.Result = txEntry.CommitTime;
            req.Finished = true;
        }

        internal override void Visit(UpdateCommitLowerBoundRequest req)
        {
            TxTableEntry txEntry = null;
            if (!this.txTable.TryGetValue(req.TxId, out txEntry))
            {
                throw new TransactionException("The specified tx does not exist.");
            }

            TxTableEntry newTxEntry = null;
            while (txEntry.CommitTime < 0 && txEntry.CommitLowerBound < req.CommitTsLowerBound)
            {
                if (newTxEntry == null)
                {
                    newTxEntry = this.resourceManager.GetTxTableEntry();
                }

                newTxEntry.Status = txEntry.Status;
                newTxEntry.CommitTime = txEntry.CommitTime;
                newTxEntry.CommitLowerBound = req.CommitTsLowerBound;

                if (this.txTable.TryUpdate(req.TxId, newTxEntry, txEntry))
                {
                    this.resourceManager.RecycleTxTableEntry(ref txEntry);
                    txEntry = newTxEntry;
                    break;
                }
                else
                {
                    this.txTable.TryGetValue(req.TxId, out txEntry);
                }
            }

            req.Result = txEntry.CommitTime >= 0 ? txEntry.CommitTime : -1;
            req.Finished = true;
        }

        internal override void Visit(UpdateTxStatusRequest req)
        {
            TxTableEntry txEntry = null;
            if (!this.txTable.TryGetValue(req.TxId, out txEntry))
            {
                throw new TransactionException("The specified tx does not exist.");
            }

            TxTableEntry txNewEntry = this.resourceManager.GetTxTableEntry();
            txNewEntry.TxId = req.TxId;
            txNewEntry.Status = req.TxStatus;
            txNewEntry.CommitTime = txEntry.CommitTime;
            txNewEntry.CommitLowerBound = txEntry.CommitLowerBound;

            if (!this.txTable.TryUpdate(req.TxId, txNewEntry, txEntry))
            {
                throw new TransactionException("A tx's status has been updated by another tx concurrently.");
            }

            req.Result = null;
            req.Finished = true;
        }
    }
}
