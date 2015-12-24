using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewUnitTest
{
    public static class TestInitialization
    {
        public static string ConnectionString { get { return ConnStr; } }

        private static readonly string ConnStr =
            System.Configuration.ConfigurationManager
                .ConnectionStrings["GraphViewDbConnectionString"].ConnectionString;

        public static void ClearDatabase()
        {
            using (var conn = new SqlConnection(ConnStr))
            {
                var sr = new StreamReader("ClearDatabase.sql");

                conn.Open();
                var command = conn.CreateCommand();
                var transaction = conn.BeginTransaction("ClearDB");
                var clearQuery = sr.ReadToEnd().Split(new string[] { "GO" }, StringSplitOptions.None);

                command.Connection = conn;
                command.Transaction = transaction;

                foreach (var query in clearQuery)
                {
                    if (query == "") continue;
                    command.CommandText = query;
                    command.ExecuteNonQuery();
                }
                transaction.Commit();
            }
        }

        [TestMethod]
        public static void CreateGraphTable()
        {
            using (var graph = new GraphViewConnection(ConnStr))
            {
                graph.Open();

                const string createEmployeeStr = @"
                CREATE TABLE [EmployeeNode] (
                    [ColumnRole: ""NodeId""]
                    [WorkId] [varchar](32),
                    [ColumnRole: ""Property""]
                    [name] [varchar](32),
                    [ColumnRole: ""Edge"", Reference: ""EmployeeNode""]
                    [Colleagues] [varchar](max),
                    [ColumnRole: ""Edge"", Reference: ""ClientNode"", Attributes: {credit: ""int"", aaa: ""double"", hhh: ""string""} ]
                    --[ColumnRole: ""Edge"", Reference: ""ClientNode""]
                    [Clients] [varchar](max),
                    [ColumnRole: ""Edge"", Reference: ""EmployeeNode""]
                    [Manager] [varchar](max),
                )";
                graph.CreateNodeTable(createEmployeeStr);

                const string createClientStr = @"
                CREATE TABLE [ClientNode] (
                    [ColumnRole: ""NodeId""]
                    [ClientId] [varchar](32),
                    [ColumnRole: ""Property""]
                    [name] [varchar](32),
                    --[ColumnRole: ""Edge"", Reference: ""ClientNode"", Attributes: {credit: ""int""} ]
                    [ColumnRole: ""Edge"", Reference: ""ClientNode""]
                    [Colleagues] [varchar](max)
                )";
                graph.CreateNodeTable(createClientStr);
                const string createUserStr = @"
                CREATE TABLE [UserNode] (
                    [ColumnRole: ""Property""]
                    [name] [varchar](32),
                    [ColumnRole: ""Property""]
                    [income] [int],
                )";
                graph.CreateNodeTable(createUserStr);
            }
        }

        public static void GenerateRandomData()
        {
            using (var graph = new GraphViewConnection(ConnStr))
            {
                graph.Open();
                DataGenerator.InsertDataEmployNode(graph.Conn);
                DataGenerator.InsertDataClientNode(graph.Conn);
                foreach (var table in graph.GetNodeTables())
                    graph.UpdateTableStatistics(table.Item1, table.Item2);
            }
        }

        
        /// <summary>
        /// Clear database, create table and generate random data
        /// </summary>
        public static void Init()
        {
            ClearDatabase();
            CreateGraphTable();
            GenerateRandomData();
        }
    }
}
