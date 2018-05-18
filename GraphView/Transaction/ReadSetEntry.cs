namespace GraphView.Transaction
{
    internal class ReadSetEntry
    {
        internal string TableId { get; set; }
        internal object RecordKey { get; set; }
        internal long VersionKey { get; set; }
        internal long BeginTimestamp { get; set; }
        internal long EndTimestamp { get; set; }
        internal long TxId { get; set; }
        internal object Record { get; set; }

        public ReadSetEntry()
        {

        }

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
