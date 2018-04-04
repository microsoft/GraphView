namespace GraphView.Transaction
{
    internal class ReadSetEntry
    {
        internal long VersionKey { get; private set; }
        internal long BeginTimestamp { get; private set; }
        internal long EndTimestamp { get; private set; }
        internal long TxId { get; private set; }
        internal object Record { get; private set; }

        public ReadSetEntry(long versionKey, long beginTimestamp, long endTimestamp, long txId, object record)
        {
            this.VersionKey = versionKey;
            this.BeginTimestamp = beginTimestamp;
            this.EndTimestamp = endTimestamp;
            this.TxId = txId;
            this.Record = record;
        }
    }
}
