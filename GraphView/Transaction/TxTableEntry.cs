using System;
using System.Runtime.Serialization;

namespace GraphView.Transaction
{
    [Serializable]
    internal class TxTableEntry : ISerializable
    {
        private readonly long txId;
        private TxStatus status;
        private long commitTime;
        private long commitLowerBound;

        public long TxId
        {
            get
            {
                return this.txId;
            }
        }

        public TxStatus Status
        {
            get
            {
                return this.status;
            }
            set
            {
                this.status = value;
            }
        }

        public long CommitTime
        {
            get
            {
                return this.commitTime;
            }
            set
            {
                this.commitTime = value;

            }
        }

        public long CommitLowerBound
        {
            get
            {
                return this.commitLowerBound;

            }
            set
            {
                this.commitLowerBound = value;

            }
        }

        public TxTableEntry(long txId)
        {
            this.txId = txId;
            this.status = TxStatus.Ongoing;
            this.commitTime = -1;
            this.commitLowerBound = 0;
        }

        public TxTableEntry(long txId, TxStatus status, long commitTime, long commitLowerBound)
        {
            this.txId = txId;
            this.status = status;
            this.commitTime = commitTime;
            this.commitLowerBound = commitLowerBound;
        }

        public override int GetHashCode()
        {
            return this.txId.GetHashCode();   
        }

        public override bool Equals(object obj)
        {
            TxTableEntry entry = obj as TxTableEntry;
            if (entry == null)
            {
                return false;
            }

            return this.TxId == entry.TxId && this.status == entry.Status &&
                this.commitTime == entry.CommitTime && this.commitLowerBound == entry.CommitLowerBound;
        }

        public TxTableEntry(SerializationInfo info, StreamingContext contex)
        {
            this.txId = (long) info.GetValue("txId", typeof(long));
            this.status = (TxStatus) info.GetValue("status", typeof(TxStatus));
            this.commitTime = (long) info.GetValue("commitTime", typeof(long));
            this.commitLowerBound = (long)info.GetValue("commitLowerBound", typeof(long));
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("txId", this.txId, typeof(long));
            info.AddValue("status", this.status, typeof(TxStatus));
            info.AddValue("commitTime", this.commitTime, typeof(long));
            info.AddValue("commitLowerBound", this.commitLowerBound, typeof(long));
        }
    }
}
