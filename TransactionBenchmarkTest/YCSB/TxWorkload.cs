namespace TransactionBenchmarkTest.YCSB
{
   class TxWorkload
    {
        internal string TableId;
        internal string Key;
        internal string Value;
        internal string Type;

        public TxWorkload(string type, string tableId, string key, string value)
        {
            this.TableId = tableId;
            this.Key = key;
            this.Value = value;
            this.Type = type;
        }

        public override string ToString()
        {
            return string.Format("key={0},value={1},type={2},tableId={3}", this.Key, this.Value, this.Type, this.TableId);
        }
    }
}
