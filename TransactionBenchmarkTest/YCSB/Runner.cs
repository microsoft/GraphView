
namespace TransactionBenchmarkTest.YCSB
{
    using GraphView.Transaction;
    using System;
    using System.Collections.Generic;
    using System.IO;

    enum TestType
    {
        CorrectnessTest,
        ThroughputTest,
    }

    class Runner : IDisposable
    {
        private RedisVersionDb versionDb = RedisVersionDb.Instance;

        private string logFilePath;

        protected StreamWriter writer;

        protected List<Operation> operations;

        internal TestType TestMode { get; set; } = TestType.CorrectnessTest;

        public Runner(string logFilePath)
        {
            this.logFilePath = logFilePath;
            this.operations = new List<Operation>();
            if (this.TestMode == TestType.CorrectnessTest)
            {
                this.writer = new StreamWriter(logFilePath);
            }
        }

        internal virtual void Run()
        {
            throw new NotImplementedException();
        }

        internal virtual void LoadOperations(string operationFile)
        {
            throw new NotImplementedException();
        }

        internal void ExecuteOperation(Operation oper)
        {
            Transaction tx = new Transaction(null, this.versionDb);

            string readValue = null;
            try
            {
                switch (oper.Operator)
                {
                    case "READ":
                        readValue = (string)tx.Read(oper.TableId, oper.Key);
                        break;

                    case "UPDATE":
                        readValue = (string)tx.Read(oper.TableId, oper.Key);
                        if (readValue != null)
                        {
                            tx.Update(oper.TableId, oper.Key, oper.Value);
                        }
                        break;

                    case "DELETE":
                        readValue = (string)tx.Read(oper.TableId, oper.Key);
                        if (readValue != null)
                        {
                            tx.Delete(oper.TableId, oper.Key);
                        }
                        break;

                    case "INSERT":
                        readValue = (string)tx.ReadAndInitialize(oper.TableId, oper.Key);
                        if (readValue == null)
                        {
                            tx.Insert(oper.TableId, oper.Key, oper.Value);
                        }
                        break;

                    default:
                        break;
                }
                tx.Commit();
            }
            catch (TransactionException e)
            {

            }

            if (this.TestMode == TestType.CorrectnessTest)
            {
                LogEntry entry = new LogEntry(tx.TxId, oper.Operator, 
                    oper.Key, oper.Value, tx.CommitTs, tx.Status, readValue);

                this.writer.WriteLineAsync(LogEntry.Serialize(entry));
            }
        }

        public void Dispose()
        {
            if (this.TestMode == TestType.CorrectnessTest)
            {
                if (this.writer != null)
                {
                    try
                    {
                        this.writer.Close();
                    }
                    finally
                    {

                    }
                    
                }
            }
        }
    }
}
