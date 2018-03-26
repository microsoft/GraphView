
namespace GraphView.Transaction
{
    internal class TxTableEntry
    {
        private long txId;

        private TxStatus status;

        private long commitTime;

        private long commitLowerBound;
    }
}
