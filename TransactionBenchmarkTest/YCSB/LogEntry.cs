namespace TransactionBenchmarkTest.YCSB
{
    using GraphView.Transaction;
    using Newtonsoft.Json;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;

    class LogEntry
    {
        public long TxId { get; set; }
        public string Operator { get; set; }
        public string Key { get; set; }
        public string Value { get; set; }
        public long CommitTime { get; set; }
        public TxStatus Status { get; set; }
        public string ReadValue { get; set; }

        public LogEntry(long txId, string optr, string key, string value, long commitTime, 
            TxStatus status, string readValue)
        {
            this.TxId = txId;
            this.Operator = optr;
            this.Key = Key;
            this.Value = value;
            this.CommitTime = commitTime;
            this.Status = status;
            this.ReadValue = readValue;
        }

        public override int GetHashCode()
        {
            int hash = 17;
            hash = hash * 23 + this.TxId.GetHashCode();
            hash = hash * 23 + this.Operator.GetHashCode();
            hash = hash * 23 + this.Key.GetHashCode();
            if (this.Value != null)
            {
                hash = hash * 23 + this.Value.GetHashCode();
            }
            hash = hash * 23 + this.CommitTime.GetHashCode();
            hash = hash * 23 + this.Status.GetHashCode();
            if (this.ReadValue != null)
            {
                hash = hash * 23 + this.ReadValue.GetHashCode();
            }

            return hash;
        }

        public override bool Equals(object obj)
        {
            LogEntry other = obj as LogEntry;
            if (other == null)
            {
                return false;
            }

            return this.TxId == other.TxId && this.Operator == other.Operator &&
                this.Key == other.Key && this.Value == other.Value &&
                this.CommitTime == other.CommitTime && this.Status == other.Status &&
                this.ReadValue == other.ReadValue;
        }

        public static string Serialize(LogEntry entry)
        {
            return JsonConvert.SerializeObject(entry);
        }

        public static LogEntry Deserialize(string json)
        {
            return JsonConvert.DeserializeObject<LogEntry>(json);
        }

        public static List<LogEntry> LoadCommitedLogEntries(string logFile)
        {
            List<LogEntry> entryList = new List<LogEntry>();
            using (StreamReader reader = new StreamReader(logFile))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    LogEntry entry = LogEntry.Deserialize(line);
                    if (entry.Status == TxStatus.Committed)
                    {
                        entryList.Add(entry);
                    }
                }
            }

            return entryList.OrderBy(x => x.CommitTime).ToList();
        }
    }
}
