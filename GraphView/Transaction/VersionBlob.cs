
namespace GraphView.Transaction
{
    internal class VersionBlob
    {
        internal long beginTimestamp;
        internal long endTimestamp;
        internal object payload;
        internal long txId;
        internal long maxCommitTs;

        public VersionBlob(long beginTs, long endTs, object payload, long txId, long maxCommitTs)
        {
            this.beginTimestamp = beginTs;
            this.endTimestamp = endTs;
            this.payload = payload;
            this.txId = txId;
            this.maxCommitTs = maxCommitTs;
        }

        public override bool Equals(object obj)
        {
            VersionBlob blob = obj as VersionBlob;
            if (blob == null)
            {
                return false;
            }

            return beginTimestamp == blob.beginTimestamp &&
                endTimestamp == blob.endTimestamp &&
                txId == blob.txId &&
                maxCommitTs == blob.maxCommitTs;
        }

        public override int GetHashCode()
        {
            return this.beginTimestamp.GetHashCode();
        }
    }
}
