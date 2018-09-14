using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;

using GraphView.Transaction;

namespace TransactionBenchmarkTest.TPCC
{
    public class CsvUtil
    {
        static internal IEnumerable<string[]> LoadPyTpccCsv(string csvPath)
        {
            using (var streamReader = new System.IO.StreamReader(csvPath))
            {
                for (string line; (line = streamReader.ReadLine()) != null;)
                {
                    yield return SplitQuotedCsvLine(line);
                }
            }
        }
        static private string[] SplitQuotedCsvLine(string line)
        {
            return line.Split(',')
                // remove double quotes
                .Select(s => s.Substring(1, s.Length - 2)).ToArray();
        }
    }
    public class TPCCTableLoader
    {
        static private IEnumerable<Tuple<object, object>>
        LoadKvsFromDir(string dir, TpccTable table)
        {
            string csvPath = table.Type().ToFilename(dir);
            foreach (string[] columns in CsvUtil.LoadPyTpccCsv(csvPath))
            {
                yield return table.ParseColumns(columns);
            }
        }
        static private void CreateTable(VersionDb versionDb, TpccTable table)
        {
            versionDb.CreateVersionTable(table.Name());
        }
        static public
        int Load(string dir, TpccTable table, VersionDb versionDb)
        {
            CreateTable(versionDb, table);
            int recordCount = 0;
            var txExec = new TransactionExecution(
                null, versionDb, null, new TxRange((int)table.Type()), 0);
            var auxIndexLoader = AuxIndexLoader.FromTableType(table.Type());
            foreach (var kv in LoadKvsFromDir(dir, table))
            {
                object k = kv.Item1, v = kv.Item2;
                txExec.Reset();
                try
                {
                    txExec.InitAndInsert(table.Name(), k, v);
                    txExec.Commit();
                    auxIndexLoader.BuildAuxIndex(k, v);
                    ++recordCount;
                }
                catch (AbortException) { }
            }
            recordCount += auxIndexLoader.SaveTo(versionDb);
            return recordCount;
        }
        /// <summary>
        /// Auxiliary index loader
        /// </summary>
        private class AuxIndexLoader
        {
            public static AuxIndexLoader FromTableType(TableType t)
            {
                switch (t)
                {
                    case TableType.CUSTOMER:
                        return new CustomerLastNameIndexLoader();
                    default:
                        return new AuxIndexLoader();
                }
            }
            public virtual void BuildAuxIndex(object k, object v) { }

            public virtual int SaveTo(VersionDb versionDb)
            {
                return 0;
            }
        }
        private class CustomerLastNameIndexLoader : AuxIndexLoader
        {
            public override void BuildAuxIndex(object k, object v)
            {
                var cpk = k as CustomerPkey;
                var cpl = v as CustomerPayload;
                Debug.Assert(k != null && v != null);
                var lastNameKey =
                    CustomerLastNameIndexKey.FromPKeyAndPayload(cpk, cpl);
                uint cid = cpk.C_ID;
                AddToStore(lastNameKey, cid);
            }
            private void AddToStore(CustomerLastNameIndexKey k, uint cid)
            {
                if (!this.tempStore.ContainsKey(k))
                {
                    this.tempStore[k] = new List<uint>();
                }
                this.tempStore[k].Add(cid);
            }

            public override int SaveTo(VersionDb versionDb)
            {
                int recordCount = 0;
                var txExec = new TransactionExecution(
                    null, versionDb, null, new TxRange(0));
                foreach (var kv in this.tempStore)
                {
                    CustomerLastNameIndexKey lastNameKey = kv.Key;
                    List<uint> cids = kv.Value;
                    cids.Sort();
                    txExec.Reset();
                    txExec.InitAndInsert(
                        TableType.CUSTOMER.Name(), lastNameKey, cids.ToArray());
                    txExec.Commit();
                    ++recordCount;
                }
                return recordCount;
            }
            Dictionary<CustomerLastNameIndexKey, List<uint>> tempStore =
                new Dictionary<CustomerLastNameIndexKey, List<uint>>();
        }
    }
}
