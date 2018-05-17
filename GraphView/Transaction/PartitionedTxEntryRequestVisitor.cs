
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;

    internal class PartitionedVersionDbVisitor : VersionDbVisitor
    {
        // A reference of dict in the version db
        private readonly Dictionary<long, TxTableEntry> txTable;

        public PartitionedVersionDbVisitor(Dictionary<long, TxTableEntry> txTable)
        {
            this.txTable = txTable;
        }

        internal override void Visit(GetTxEntryRequest req)
        {
            req.Result = txTable[req.TxId];
            req.Finished = true;
        }

        internal override void Visit(InsertTxIdRequest req)
        {
            this.txTable[req.TxId] = new TxTableEntry(req.TxId);
            req.Finished = true;
        }

        internal override void Visit(RemoveTxRequest req)
        {
            this.txTable.Remove(req.TxId);
            req.Result = true;
            req.Finished = true;
        }

        internal override void Visit(NewTxIdRequest req)
        {
            if (this.txTable.ContainsKey(req.TxId))
            {
                req.Result = 0L;
            }
            else
            {
                this.txTable.Add(req.TxId, null);
                req.Result = 1L;
            }

            req.Finished = true;
        }

        internal override void Visit(RecycleTxRequest req)
        {
            TxTableEntry txEntry = this.txTable[req.TxId];
            txEntry.Reset();
            req.Result = 1L;
            req.Finished = true;
        }

        internal override void Visit(SetCommitTsRequest req)
        {
            TxTableEntry txEntry = this.txTable[req.TxId];
            // already has a commit time
            if (txEntry.CommitTime != TxTableEntry.DEFAULT_COMMIT_TIME)
            {
                req.Result = -1L;
            }
            else
            {
                txEntry.CommitTime = Math.Max(txEntry.CommitLowerBound, req.ProposedCommitTs);
                req.Result = txEntry.CommitTime;
            }
            req.Finished = true;
        }

        internal override void Visit(UpdateCommitLowerBoundRequest req)
        {
            TxTableEntry txEntry = this.txTable[req.TxId];

            if (txEntry.CommitTime < 0)
            {
                txEntry.CommitLowerBound = Math.Max(txEntry.CommitLowerBound, req.CommitTsLowerBound);
                req.Result = -1L;
            }
            else
            {
                req.Result = txEntry.CommitTime;
            }

            req.Finished = true;
        }

        internal override void Visit(UpdateTxStatusRequest req)
        {
            TxTableEntry txEntry = this.txTable[req.TxId];
            txEntry.Status = req.TxStatus;
            req.Result = null;
            req.Finished = true;
        }
    }
}
