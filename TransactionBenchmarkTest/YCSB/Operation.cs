using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TransactionBenchmarkTest.YCSB
{
    class Operation
    {
        public string Operator { get; set; }
        public string TableId { get; set; }
        public string Key { get; set; }
        public string Value { get; set; }

        public Operation(string operation, string tableId, string key, string value)
        {
            this.Operator = operation;
            this.TableId = tableId;
            this.Key = key;
            this.Value = value;
        }

        public override string ToString()
        {
            return String.Format("operation = {0}, taleId = {1}, key = {2}, value = {3}",
                this.Operator, this.TableId, this.Key, this.Value);
        }
    }
}
