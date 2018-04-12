namespace TransactionBenchmarkTest.YCSB
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using GraphView.Transaction;

    class SequenceRunner : Runner
    {
        public static readonly String TABLE_ID = "usertable_sequence";

        public static readonly long REDIS_DB_INDEX = 5L;

        public SequenceRunner(string logFile)
            : base(logFile)
        {

        }

        internal override void Run()
        {
            foreach (Operation optr in this.operations)
            {
                this.ExecuteOperation(optr);
            }
        }

        internal override void LoadOperations(string operationsFile)
        {
            List<LogEntry> entryList = LogEntry.LoadCommitedLogEntries(operationsFile);
            this.operations = entryList.Select(entry =>
                new Operation(entry.Operator, SequenceRunner.TABLE_ID, entry.Key, entry.Value)).ToList();
        }
    }
}
