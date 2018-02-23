
namespace GraphView.Transaction
{
    internal class TxTableEntry
    {
        public TxStatus Status;
        public readonly long BeginTimestamp;
        public long EndTimestamp;

        public TxTableEntry(TxStatus txStatus, long beginTimestamp, long endTimestamp)
        {
            this.Status = txStatus;
            this.BeginTimestamp = beginTimestamp;
            this.EndTimestamp = endTimestamp;
        }
    }
}
