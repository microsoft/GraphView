using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
        static public
        int Load(string dir, TpccTable table, VersionDb versionDb)
        {
            int c = 0;
            var txExec = new TransactionExecution(
                null, versionDb, null, new TxRange((int)table.Type()), 0);
            foreach (var kv in LoadKvsFromDir(dir, table))
            {
                txExec.Reset();
                txExec.InitAndInsert(Constants.DefaultTbl, kv.Item1, kv.Item2);
                txExec.Commit();
                ++c;
            }
            return c;
        }
    }
}
