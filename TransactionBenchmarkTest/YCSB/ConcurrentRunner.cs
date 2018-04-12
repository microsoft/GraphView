using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TransactionBenchmarkTest.YCSB
{
    class ConcurrencyRunner : Runner
    {
        public static readonly String TABLE_ID = "usertable_concurrent";

        public static readonly long REDIS_DB_INDEX = 7L;

        public ConcurrencyRunner(string logFile) : base(logFile)
        {

        }

        internal override void Run()
        {
            int size = this.operations.Count;
            using (CountdownEvent countdownEvent = new CountdownEvent(size))
            {
                foreach(Operation optr in this.operations)
                {
                    ThreadPool.QueueUserWorkItem(x =>
                    {
                        this.ExecuteOperation(optr);
                        countdownEvent.Signal();
                    }, optr);
                }
                countdownEvent.Wait();
            }
        }

        internal override void LoadOperations(string operationFile)
        {
            string resourceName = String.Format(operationFile);
            using (StreamReader reader = new StreamReader(resourceName))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    string[] fields = this.ParseCommandFormat(line);
                    this.operations.Add(new Operation(fields[0], ConcurrencyRunner.TABLE_ID, fields[2], fields[3]));
                }
            }
        }

        private string[] ParseCommandFormat(string line)
        {
            string[] fields = line.Split(' ');
            string value = null;
            if (fields[4].Length > 6)
            {
                value = fields[4].Substring(7, fields[4].Length - 7);
            }

            return new string[] {
                fields[0], fields[1], fields[2], value
            };
        }
    }
}
