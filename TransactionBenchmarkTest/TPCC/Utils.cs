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

        // data population
        public const string BaseDirOfDatasets =
            @"D:\Elastas\benchmark\tpcc\data\tpcc-tables";
        public static string[] TableNames = { "WAREHOUSE.csv", "DISTRICT.csv", "CUSTOMER.csv", "ITEM.csv", "STOCK.csv", "ORDERS.csv", "ORDER_LINE.csv", "NEW_ORDER.csv", "HISTORY.csv" };
        public static TableCode[] TableCodes = { TableCode.W, TableCode.D, TableCode.C, TableCode.I, TableCode.S, TableCode.O, TableCode.OL, TableCode.NO, TableCode.H };

        // workload path
        public const string NewOrderWorkloadPath =
            @"D:\Elastas\benchmark\tpcc\data\tpcc-txns\NEW_ORDER.csv";
        public const string PaymentWorkloadPath =
            @"D:\Elastas\benchmark\tpcc\data\tpcc-txns\PAYMENT.csv";

        static public class Singleton
        {
            public const int Concurrency = 1;
        }
    }
}
