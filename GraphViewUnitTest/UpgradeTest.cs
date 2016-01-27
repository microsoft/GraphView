using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewUnitTest
{
    [TestClass]
    public class UpgradeTest
    {

        private static string yourSQLServerName = @"(local)";
        private static string yourDBName = "graphViewV1-00";

        public static string getConnectionString()
        {
            return "Data Source=" + yourSQLServerName
                   + ";Initial Catalog=" + yourDBName
                   +
                   ";Integrated Security=True;Connect Timeout=3000;Encrypt=False;TrustServerCertificate=False;ApplicationIntent=ReadWrite;MultiSubnetFailover=False;Max Pool Size=300";
        }

        [TestMethod]
        public void UpgradeFunction()
        {
            using (var conn = new GraphViewConnection(getConnectionString()))
            {
                conn.Open();
                conn.UpgradeGraphViewFunction();
                conn.updateGlobalNodeView();
                conn.UpdateTableStatistics("dbo","ClientNode");
                conn.UpdateTableStatistics("dbo", "EmployeeNode");

            }
        }

        [TestMethod]
        public void UpgradeSelectTest()
        {
            using (var conn = new GraphViewConnection(getConnectionString()))
            {
                conn.Open();
                var command = conn.CreateCommand();
                command.CommandText = @"Select a.workid from employeeNode a, employeeNode b, ClientNode c,ClientNode d match a-[colleagues]->b, a-[Clients]->c, c-[colleagues as x1]->d, c-[colleagues as x2]->d";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Trace.WriteLine(reader[0]);
                    }
                }
                command.CommandText = @"Select a.workid from employeeNode a, employeeNode b,employeeNode c match a-[colleagues]->b,a-[colleagues]->c";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Trace.WriteLine(reader[0]);
                    }
                }
                command.CommandText = @"Select * from employeeNode a, employeeNode b match a-[colleagues*1..3 as x]->b";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Trace.WriteLine(reader[0]);
                    }
                }
                command.CommandText = @"Select x.* from employeeNode a, employeeNode b match a-[colleagues*1..3 as x]->b";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Trace.WriteLine(reader[0]);
                    }
                }

                command.CommandText = @"Select x.* from globalnodeview a, globalnodeview b match a-[colleagues*1..3 as x]->b";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Trace.WriteLine(reader[0]);
                    }
                }
            }
        }
    }
}
