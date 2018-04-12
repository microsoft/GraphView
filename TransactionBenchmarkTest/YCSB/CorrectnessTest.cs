using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;

namespace TransactionBenchmarkTest.YCSB
{
    [TestClass]
    public class CorrectnessTest : YCSBTestBase
    {
        [TestMethod]
        public void TestCorrectness()
        {
            string concurrencyInput = "concurrency.in";
            string concurrencyOutput = "concurrency.out";
            string sequenceInput = "sequence.in";
            string sequenceOutput = "sequence.out";

            ConcurrencyRunner conRunner = new ConcurrencyRunner(concurrencyOutput);
            conRunner.LoadOperations(concurrencyInput);
            conRunner.Run();

            SequenceRunner seqRunner = new SequenceRunner(sequenceOutput);
            seqRunner.LoadOperations(sequenceInput);
            seqRunner.Run();

            List<LogEntry> conLogEntryList = LogEntry.LoadCommitedLogEntries(concurrencyOutput);
            List<LogEntry> seqLogEntryList = LogEntry.LoadCommitedLogEntries(sequenceOutput);
            Assert.AreEqual(conLogEntryList.Count, seqLogEntryList.Count);

            for (int i = 0; i < conLogEntryList.Count; i++)
            {
                Assert.AreEqual(conLogEntryList[i], seqLogEntryList[i]);
            }
        }
    }
}
