namespace GraphView.Transaction
{
    internal class VersionPrimaryKey
    {
        internal object RecordKey { get; private set; }
        internal long VersionKey { get; private set; }

        public VersionPrimaryKey(object recordKey, long versionKey)
        {
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
        }
    }
}

