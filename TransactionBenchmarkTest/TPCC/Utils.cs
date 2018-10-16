using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using GraphView.Transaction;

namespace TransactionBenchmarkTest.TPCC
{

    // Table Code
    enum TableCode { W, D, C, I, S, O, OL, NO, H };      // table code

    // Tx FINAL STATUS
    enum TxFinalStatus { ABORTED, COMMITTED, UNKNOWN };

    static class Constants
    {
        // fixed
        public const uint NullCarrierID = 0;
        public const char Delimiter = ',';
        public const string WorkloadDelimiter = "\",";
        public const char PlaceHolder = '*';
        public const string PlaceHolders = "**";
        public const string BadCredit = "BC";
        public const int Max_C_DATA = 500;   // max C_DATA length

        // redis
        public const string RedisHost = "localhost";
        public const int RedisPort = 6379;
        public const int RedisDbN = 6;
        public const int RedisIndexDbN = 7;      // c_last name index db
        public const string DefaultTbl = "test";

        public static string[] TableNames = { "WAREHOUSE.csv", "DISTRICT.csv", "CUSTOMER.csv", "ITEM.csv", "STOCK.csv", "ORDERS.csv", "ORDER_LINE.csv", "NEW_ORDER.csv", "HISTORY.csv" };
        public static TableCode[] TableCodes = { TableCode.W, TableCode.D, TableCode.C, TableCode.I, TableCode.S, TableCode.O, TableCode.OL, TableCode.NO, TableCode.H };

        public const string TpccFileDir = @"D:\Elastas\benchmark\tpcc\data";
        public const string DataSetDir = TpccFileDir + @"\tpcc-tables";
        public const string WorkloadDir = TpccFileDir + @"\tpcc-txns";
    }

    static public class FileHelper
    {
        static public string CSVPath(string dir, string fileName)
        {
            return $"{dir}\\{fileName}.csv";
        }
        static public string DataSetDir(string tpccFileDir)
        {
            return $"{tpccFileDir}\\tpcc-tables";
        }
        static public string WorkloadDir(string tpccFileDir)
        {
            return $"{tpccFileDir}\\tpcc-txns";
        }
        static public
        IEnumerable<string[]> LoadCsv(string path, bool ignoreFirstLine = false)
        {
            using (var streamReader = new System.IO.StreamReader(path))
            {
                if (ignoreFirstLine)
                {
                    streamReader.ReadLine();
                }
                for (string line; (line = streamReader.ReadLine()) != null;)
                {
                    yield return SplitQuotedLine(line);
                }
            }
        }
        static private string[] SplitQuotedLine(string line)
        {
            string[] result = line.Split(new string[]{"\",\""}, StringSplitOptions.None);
            result[0] = result[0].Substring(1);
            result[result.Length - 1] = result.Last().Substring(0, result.Last().Length - 1);
            return result;
            // return line.Split(',')
            //     // remove double quotes
            //     .Select(s => s.Substring(1, s.Length - 2)).ToArray();
        }
    }

    public class BenchmarkConfig
    {
        static public BenchmarkConfig globalConfig = new BenchmarkConfig();

        public enum TransactionType
        {
            PAYMENT, NEW_ORDER
        }
        public BenchmarkConfig()
        {
            this.Concurrency = 2;
            this.WorkloadPerWorker = 200000;
            this.PaymentRatio = 1; // currently not used
            this.TpccFileDir = Constants.TpccFileDir;
            this.Warehouses = 4;
        }

        public int Concurrency;
        public int WorkloadPerWorker;
        public double PaymentRatio; // currently not used
        public string TpccFileDir;
        public int Warehouses;

        public void Print()
        {
            Console.WriteLine($"Concurrency: {this.Concurrency}");
            Console.WriteLine($"Workload per worker: {this.WorkloadPerWorker}");
            Console.WriteLine($"Payment Ratio: {this.PaymentRatio}");
            Console.WriteLine();
        }

        static public TransactionType StringToTxType(string t)
        {
            switch (t.ToLower())
            {
                case "payment":
                    return TransactionType.PAYMENT;
                case "new_order":
                    return TransactionType.NEW_ORDER;
                default:
                    throw new Exception($"unknown transaction type: {t}");
            }
        }
    }
}
