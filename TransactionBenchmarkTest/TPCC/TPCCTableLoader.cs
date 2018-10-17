using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;

using GraphView.Transaction;

namespace TransactionBenchmarkTest.TPCC
{
    public class TPCCTableLoader
    {
        static private IEnumerable<Tuple<TpccTableKey, TpccTablePayload>>
        LoadKvsFromDir(string dir, TpccTable table)
        {
            string csvPath = FileHelper.CSVPath(dir, table.Type().Name());
            foreach (string[] columns in FileHelper.LoadCsv(csvPath))
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
            var txExec = new SyncExecution(versionDb, (int)table.Type());
            var auxIndexLoader = AuxIndexLoader.FromTableType(table.Type());
            foreach (var kv in LoadKvsFromDir(dir, table))
            {
                txExec.Start();
                if (txExec.Insert(kv.Item1, kv.Item2).IsAborted())
                {
                    continue;
                }
                if (txExec.Commit().IsAborted())
                {
                    continue;
                }
                auxIndexLoader.BuildAuxIndex(kv.Item1, kv.Item2);
                ++recordCount;
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
            public virtual
            void BuildAuxIndex(TpccTableKey k, TpccTablePayload v) { }

            public virtual int SaveTo(VersionDb versionDb)
            {
                return 0;
            }
        }
        private class CustomerLastNameIndexLoader : AuxIndexLoader
        {
            public override
            void BuildAuxIndex(TpccTableKey k, TpccTablePayload v)
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
                CreateTable(versionDb, TpccTable.Instance(TableType.CUSTOMER_INDEX));
                int recordCount = 0;
                SyncExecution txExec = new SyncExecution(versionDb);
                foreach (var kv in this.tempStore)
                {
                    CustomerLastNameIndexKey lastNameKey = kv.Key;
                    List<uint> cids = kv.Value;
                    txExec.Start();
                    txExec.Insert(
                        lastNameKey, CustomerLastNamePayloads.FromList(cids));
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
