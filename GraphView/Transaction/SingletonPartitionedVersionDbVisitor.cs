
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Threading;

    internal class SingletonPartitionedVersionDbVisitor : VersionDbVisitor
    {
        // A reference of dict in the version db
        private readonly Dictionary<long, TxTableEntry> txTable;

        public SingletonPartitionedVersionDbVisitor(Dictionary<long, TxTableEntry> txTable)
        {
            this.txTable = txTable;
        }

        internal override void Visit(GetTxEntryRequest req)
        {
            TxTableEntry txEntry = req.RemoteTxEntry;
            if (txEntry == null)
            {
                txEntry = txTable[req.TxId];
                req.RemoteTxEntry = txEntry;
            }

            TxTableEntry.CopyValue(txEntry, req.LocalTxEntry);

            req.Result = req.LocalTxEntry;
            req.Finished = true;
        }

        internal override void Visit(InsertTxIdRequest req)
        {
            TxTableEntry txEntry = req.RemoteTxEntry;
            if (txEntry == null)
            {
                txEntry = this.txTable[req.TxId];
            }

            txEntry.Reset(req.TxId);
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
                req.Result = false;
            }
            else
            {
                TxTableEntry txEntry = new TxTableEntry();
                this.txTable.Add(req.TxId, txEntry);
                req.RemoteTxEntry = txEntry;
                req.Result = true;
            }

            req.Finished = true;
        }

        internal override void Visit(RecycleTxRequest req)
        {
            // RecycleTxRequest is supposed to has no remoteTxEntry reference
            TxTableEntry txEntry = null;
            this.txTable.TryGetValue(req.TxId, out txEntry);
            
            if (txEntry == null)
            {
                txEntry = new TxTableEntry(req.TxId);
                this.txTable.Add(req.TxId, txEntry);
            }
            else
            {
                txEntry.Reset();
                req.RemoteTxEntry = txEntry;
                req.Result = true;
                req.Finished = true;
            }
        }

        internal override void Visit(SetCommitTsRequest req)
        {
            TxTableEntry txEntry = req.RemoteTxEntry;
            if (txEntry == null)
            {
                txEntry = this.txTable[req.TxId];
            }
                
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
            TxTableEntry txEntry = req.RemoteTxEntry;
            if (txEntry == null)
            {
                txEntry = this.txTable[req.TxId];
            }

            if (txEntry.CommitTime == TxTableEntry.DEFAULT_COMMIT_TIME)
            {
                txEntry.CommitLowerBound = Math.Max(txEntry.CommitLowerBound, req.CommitTsLowerBound);
                req.Result = txEntry.CommitTime;
            }
            else
            {
                req.Result = txEntry.CommitTime;
            }

            req.Finished = true;
        }

        internal override void Visit(UpdateTxStatusRequest req)
        {
            TxTableEntry txEntry = req.RemoteTxEntry;
            if (txEntry == null)
            {
                txEntry = this.txTable[req.TxId];
            }
            
            txEntry.Status = req.TxStatus;
            req.Result = null;
            req.Finished = true;
        }
    }
}
