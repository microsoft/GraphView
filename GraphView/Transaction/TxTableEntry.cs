using System;
using System.Runtime.Serialization;

namespace GraphView.Transaction
{
    public class TxTableEntry
    {
        public static readonly string TXID_STRING = "tx_id";
        public static readonly string STATUS_STRING = "status";
        public static readonly string COMMIT_TIME_STRING = "commit_time";
        public static readonly string COMMIT_LOWER_BOUND_STRING = "commit_lower_bound";

        public static readonly long DEFAULT_COMMIT_TIME = -1L;
        public static readonly long DEFAULT_LOWER_BOUND = 0L;

        internal long TxId { get; set; }
        internal TxStatus Status { get; set; }
        internal long CommitTime { get; set; }
        internal long CommitLowerBound { get; set; }

        internal int latch = 0;

        public TxTableEntry()
        {

        }

        public TxTableEntry(long txId)
        {
            this.TxId = txId;
            this.Status = TxStatus.Ongoing;
            this.CommitTime = TxTableEntry.DEFAULT_COMMIT_TIME;
            this.CommitLowerBound = TxTableEntry.DEFAULT_LOWER_BOUND;
        }

        public TxTableEntry(long txId, TxStatus status, long commitTime, long commitLowerBound)
        {
            this.TxId = txId;
            this.Status = status;
            this.CommitTime = commitTime;
            this.CommitLowerBound = commitLowerBound;
        }

        public static void CopyValue(TxTableEntry src, TxTableEntry dst)
        {
            dst.TxId = src.TxId;
            dst.Status = src.Status;
            dst.CommitTime = src.CommitTime;
            dst.CommitLowerBound = src.CommitLowerBound;
        }

        public void Set(long txId, TxStatus status, long commitTime, long commitLowerBound)
        {
            this.TxId = txId;
            this.Status = status;
            this.CommitTime = commitTime;
            this.CommitLowerBound = commitLowerBound;
        }

        public override int GetHashCode()
        {
            return this.TxId.GetHashCode();   
        }

        public override bool Equals(object obj)
        {
            TxTableEntry entry = obj as TxTableEntry;
            if (entry == null)
            {
                return false;
            }

            return this.TxId == entry.TxId && this.Status == entry.Status &&
                this.CommitTime == entry.CommitTime && this.CommitLowerBound == entry.CommitLowerBound;
        }

        internal void Reset(long txId = -1)
        {
            if (txId != -1)
            {
                this.TxId = txId;
            }

            this.Status = TxStatus.Ongoing;
            this.CommitTime = TxTableEntry.DEFAULT_COMMIT_TIME;
            this.CommitLowerBound = TxTableEntry.DEFAULT_LOWER_BOUND;
        }
    }
}
