using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.IO;
using System.Linq;
using GraphView;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewUnitTest
{
    [TestClass]
    public class GraphDbTest
    {
        private readonly string _connStr =
            System.Configuration.ConfigurationManager
                  .ConnectionStrings["GraphViewDbConnectionString"].ConnectionString;

        public void TestEstimate()
        {
            TestInitialization.ClearDatabase();
            TestInitialization.CreateGraphTable();
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                graph.UpdateTableStatistics("dbo", "EmployeeNode");
            }
            using (var conn = new SqlConnection(_connStr))
            {
                conn.Open();
                var command = conn.CreateCommand();
                command.CommandText = string.Format(
                    "SELECT AverageDegree FROM {0} WHERE TableName = 'EmployeeNode'",
                    GraphViewConnection.MetadataTables[3]);
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var averageDegree = Convert.ToInt32(reader["AverageDegree"]);
                        Assert.AreEqual(averageDegree, 0);
                    }
                }
            }
        }

        [TestMethod]
        public void ClearDatabaseTest()
        {
            TestInitialization.ClearDatabase();
            TestInitialization.CreateGraphTable();
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                var edges = new List<Tuple<string, string>>();
                edges.Add(Tuple.Create("EmployeeNode", "Colleagues"));
                edges.Add(Tuple.Create("EmployeeNode", "Manager"));
                var edgeAttribute = new List<string>();
                var attributeMapping = new List<Tuple<string, List<Tuple<string, string, string>>>>();
                graph.CreateEdgeView("dbo", "EmployeeNode", "edgeView", edges, edgeAttribute, null, attributeMapping);

                const string q2 = @"
                    CREATE PROCEDURE SingleEdgeInsert
                        @src varchar(32)
                     AS
                     BEGIN
                        BEGIN TRAN;
                        INSERT NODE INTO ClientNode (ClientId) VALUES (@src)
                        COMMIT TRAN;
                     END";
                graph.CreateProcedure(q2);

                const string q3 = @"
                    CREATE PROCEDURE SingleEdge
                        @src varchar(32)
                     AS
                     BEGIN
                        BEGIN TRAN;
                        INSERT NODE INTO ClientNode (ClientId) VALUES (@src)
                        COMMIT TRAN;
                     END";
                graph.CreateProcedure(q3);

                var propertymapping = new List<Tuple<string, List<Tuple<string, string>>>>()
                {
                    Tuple.Create("ClientId",
                        new List<Tuple<string, string>>()
                        {
                            Tuple.Create("ClientNode", "ClientId"),
                            Tuple.Create("EmployeeNode", "WorkId")
                        })
                };
                graph.CreateNodeView("dbo", "suppernodetest", new List<string>()
                {
                    "ClientNode",
                    "EmployeeNode"
                },
                    propertymapping);
            }
        }


        [TestMethod]
        public void BitmapTest()
        {
            TestInitialization.ClearDatabase();
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();

                const string createEmployeeStr = @"
                CREATE TABLE [EmployeeNode] (
                    [ColumnRole: ""NodeId""]
                    [WorkId] [varchar](32),
                    [ColumnRole: ""Property""]
                    [name] [varchar](32),
                    [ColumnRole: ""Edge"", Reference: ""ClientNode"", Attributes: {a: ""int"", b: ""double"", c: ""string"", d:""int"", 
                    e:""int"", f:""int"", g:""int"", h:""int"", i:""Int""}]
                    [Clients] [varchar](max),
                )";
                graph.CreateNodeTable(createEmployeeStr);

                const string createClientStr = @"
                CREATE TABLE [ClientNode] (
                    [ColumnRole: ""NodeId""]
                    [ClientId] [varchar](32),
                    [ColumnRole: ""Edge"", Reference: ""ClientNode"", Attributes: {a:""int""}]
                    [Colleagues] [varchar](max)
                )";
                graph.CreateNodeTable(createClientStr);

                const string sqlStr = @"
                INSERT NODE INTO EmployeeNode (WorkId) VALUES ('apple');
                INSERT NODE INTO ClientNode (ClientId) VALUES ('banana');";
                var command = new GraphViewCommand(sqlStr,graph);
                command.ExecuteNonQuery();

                const string sqlStr2 = @"
                INSERT EDGE INTO EmployeeNode.Clients
                SELECT En, Cn, 1, 0, 'test', null, 123, null, null, null, 100
                FROM EmployeeNode En, ClientNode Cn
                WHERE En.Workid = 'apple' AND Cn.ClientId = 'banana';";
                command.CommandText = sqlStr2;
                command.ExecuteNonQuery();
                //check result by:
                //select *
                //from EmployeeNode cross apply dboEmployeeNodeClientsDecoder(EmployeeNode.Clients, EmployeeNode.ClientsDeleteCol)
                //Bitmap is 0x1701
                const string sqlStr3 = @"
                DELETE EDGE [EmployeeNode]-[Clients]->[ClientNode]
                FROM EmployeeNode, ClientNode
                WHERE EmployeeNode.Workid  = 'apple' AND ClientNode.ClientId = 'banana';";
                command.CommandText = sqlStr3;
                command.ExecuteNonQuery();
            }
        }
        [TestMethod]
        public void BulkInsertTest()
        {
            TestInitialization.ClearDatabase();
            TestInitialization.CreateGraphTable();
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string filedterminator = @",";
                const string rowterminator = "\r\n";
                var columnList1 = new List<string> {"WorkId"};
                var columnList2 = new List<string> {"ClientId"};
                var propetyList = new List<string> {"aaa", "credit", "hhh"};
                graph.BulkInsertNode(@"d:\data\nodedata2.txt", "EmployeeNode", "dbo", columnList1, filedterminator,
                    rowterminator);
                graph.BulkInsertNode(@"d:\data\nodedata2.txt", "ClientNode", "dbo", columnList2, filedterminator,
                    rowterminator);
                var sw = new Stopwatch();
                sw.Start();
                graph.BulkInsertEdge(@"d:\data\edgedata2.txt", "dbo", "EmployeeNode", "workid", "ClientNode", "clientid",
                    "clients",
                   propetyList, filedterminator, "\r\n");
                sw.Stop();
                Trace.WriteLine(sw.ElapsedMilliseconds.ToString());
            }
        }

        [TestMethod]
        public void TestStoreProcedure()
        {
            TestInitialization.Init();
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
//                const string q2 = @"
//                CREATE PROCEDURE testSP
//                    @input1 int,
//                    @input2 int
//                AS
//                    SELECT * 
//                    FROM EmployeeNode as e1, ClientNode as c1
//                    MATCH [e1]-[Clients as edge]->[c1]
//                    WHERE edge.credit = @input1 and edge.aaa<@input2";
//                graph.CreateProcedure(q2);
                
//                const string q3 = @"
//                CREATE PROCEDURE testSP;2
//                    @input1 int,
//                    @input2 int
//                AS
//                    SELECT * 
//                    FROM EmployeeNode as e1, EmployeeNode as e2
//                    MATCH [e1]-[Colleagues as edge]->[e2]
//                    WHERE e1.name like 'n%'";
//                graph.CreateProcedure(q3);
                
//                const string q4 = @"
//                CREATE PROCEDURE testSP2
//                    @input1 int,
//                    @input2 int
//                AS
//                    SELECT * 
//                    FROM EmployeeNode as e1
//                    WHERE e1.name like 'n%'";
//                graph.CreateProcedure(q4);

//                const string q5 = @"
//                DROP PROCEDURE testSP, testSP2";
//                graph.DropProcedure(q5);
                const string qq = @"CREATE PROCEDURE SingleEdgeInsert
                        @src varchar(32)
                     AS
                     BEGIN
                        BEGIN TRAN;
                        INSERT NODE INTO ClientNode (ClientId) VALUES (@src)
                        COMMIT TRAN;
                     END";
                graph.CreateProcedure(qq);
                using (var command = graph.CreateCommand())
                {
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = "SingleEdgeInsert";
                    command.Parameters.Add("@src", SqlDbType.VarChar, 32);
                    command.Parameters["@src"].Value = "B";
                    command.ExecuteNonQuery();
                    //command.ExecuteReader();
                }
            }
        }

//        [TestMethod]
//        public void TestPR()
//        {
//            using (var graph = new GraphViewConnection(_connStr))
//            {
//                try
//                {
//                    graph.Open();
//                    const string createUserStr = @"
//                    CREATE TABLE [NodePR] (
//                        [ColumnRole: ""Property""]
//                        [score] [float],
//                        [ColumnRole: ""Property""]
//                        [converge] [int],
//                    )";
//                    graph.CreateNodeTable(createUserStr);

//                    const string q2 = @"
//                        DECLARE @N int = 100;
//
//                        UPDATE NodePR SET score = 1/@N
//
//                        DECLARE @convNum int = 0;
//                        WHILE @convNum <@N
//                        BEGIN
//	                        UPDATE NodePR
//	                        SET score = newScore,
//		                        converge = CASE WHEN ABS(oldScore-newScore)<0.1
//		                        THEN 1 ELSE 0 END
//	                        FROM
//	                        (
//		                        SELECT n1.GlobalNodeId, n1.WorkId as oldScore, n1.WorkId as newScore
//		                        FROM EmployeeNode n1,EmployeeNode n2
//		                        MATCH n1-[Colleagues]->n2
//		                        group by n1.GlobalNodeId,n1.WorkId
//	                        ) iterR
//	                        where iterR.GlobalNodeId = NodePR.GlobalNodeId;
//
//	                        SELECT @convNum = COUNT(*)
//	                        FROM　NodePR
//	                        WHERE converge>0;
//                        END";
//                    using (var command = new GraphViewCommand(q2,graph))
//                    {
//                        using (var reader = command.ExecuteReader())
//                        {
//                            Assert.Fail();
//                            var count = 0;
//                            while (reader.Read())
//                            {
//                                count++;
//                            }
//                            Trace.WriteLine(count);
//                            //reader.Close();
//                        }
//                    }
//                }
//                catch (SqlException e)
//                {
//                }
//                finally
//                {
//                    graph.DropNodeTable("DROP TABLE NodePR");
//                }
//            }

//        }


        [TestMethod]
        public void TestDropNodeTable()
        {
            TestInitialization.Init();
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string dropClientNodeTable = @"DROP TABLE ClientNode;";
                graph.DropNodeTable(dropClientNodeTable);
                const string dropEmployeeNodeeTable = @"DROP TABLE EmployeeNode;";
                graph.DropNodeTable(dropEmployeeNodeeTable);
                const string dropUserNodeTable = @"DROP TABLE UserNode;";
                graph.DropNodeTable(dropUserNodeTable);
            }
        }

        [TestMethod]
        public void EdgeViewTest()
        {
            TestInitialization.ClearDatabase();
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();

                const string createEmployeeStr = @"
                CREATE TABLE [EmployeeNode] (
                    [ColumnRole: ""NodeId""]
                    [WorkId] [varchar](32),
                    [ColumnRole: ""Property""]
                    [name] [varchar](32),
                    [ColumnRole: ""Edge"", Reference: ""ClientNode"", Attributes: {a: ""int"", b: ""double"", d:""int""}]
                    [Clients] [varchar](max),
                )";
                graph.CreateNodeTable(createEmployeeStr);
                const string createEmployeeStr2 = @"
                CREATE TABLE [ClientNode] (
                    [ColumnRole: ""NodeId""]
                    [ClientId] [varchar](32),
                    [ColumnRole: ""Edge"", Reference: ""ClientNode"", Attributes: {a:""int"", c:""string"", d:""int"", e:""double""}]
                    [Colleagues] [varchar](max)
                )";
                graph.CreateNodeTable(createEmployeeStr2);

                List<Tuple<string, string>> Edges = new List<Tuple<string, string>>();
                Edges.Add(Tuple.Create("employeenode", "clients"));
                Edges.Add(Tuple.Create("ClientNode", "Colleagues"));
                List<string> edgeAttribute = new List<string>() {"a", "b", "c_new", "d"};
                List<Tuple<string, List<Tuple<string, string, string>>>> mapping =
                    new List<Tuple<string, List<Tuple<string, string, string>>>>();
                mapping.Add(Tuple.Create("a",
                    new List<Tuple<string, string, string>>()
                    {
                        Tuple.Create("employeenode", "clients", "a"),
                        Tuple.Create("ClientNode", "Colleagues", "a")
                    }));
                mapping.Add(Tuple.Create("b",
                    new List<Tuple<string, string, string>>() {Tuple.Create("employeenode", "clients", "b")}));
                mapping.Add(Tuple.Create("c_new",
                    new List<Tuple<string, string, string>>() {Tuple.Create("ClientNode", "Colleagues", "c")}));
                mapping.Add(Tuple.Create("d", new List<Tuple<string, string, string>>()));
                graph.CreateEdgeView("dbo", "NodeView", "EdgeView", Edges, edgeAttribute, null, mapping);


                //const string createSupperNode = @"
                //create view suppernodetest as
                //(
                //    select GlobalNodeId, InDegree, ClientId, Colleagues, ColleaguesDeleteCol, 0x as clients, 0x as  clientsDeleteCol
                //    from ClientNode
                //    union all
                //    select GlobalNodeId, Indegree, WorkId, 0x, 0x, clients,  clientsDeleteCol
                //    from EmployeeNode
                //)";
                //command.CommandText = createSupperNode;
                //command.ExecuteNonQuery();

                var propertymapping = new List<Tuple<string, List<Tuple<string, string>>>>()
                {
                    Tuple.Create("ClientId",
                        new List<Tuple<string, string>>()
                        {
                            Tuple.Create("ClientNode", "ClientId"),
                            Tuple.Create("EmployeeNode", "WorkId")
                        }),
                    Tuple.Create("Id",
                        new List<Tuple<string, string>>()
                        {
                            Tuple.Create("ClientNode", "ClientId")
                        })
                };
                graph.CreateNodeView("dbo", "suppernodetest", new List<string>() {
                "ClientNode",
                "EmployeeNode"
                },
                propertymapping);

                var command = new GraphViewCommand(null, graph);
                const string insertNode = @"
                INSERT NODE INTO ClientNode (ClientId) VALUES ('apple');
                INSERT NODE INTO EmployeeNode (WorkId, name) VALUES ('banana', 'banana');";
                command.CommandText = insertNode;
                command.ExecuteNonQuery();

                const string insertEdge = @"
                INSERT EDGE INTO EmployeeNode.Clients
                SELECT En, Cn, 1, 0.1, 1000
                FROM EmployeeNode En, ClientNode Cn
                WHERE En.Workid = 'banana' AND Cn.ClientId = 'apple';
                INSERT EDGE INTO ClientNode.Colleagues
                SELECT Cn1, Cn2, 2, 'test', 10000, 0.2
                FROM ClientNode  Cn1, ClientNode Cn2
                WHERE Cn1.ClientId  = 'apple' AND Cn2.ClientId = 'apple'";
                command.CommandText = insertEdge;
                command.ExecuteNonQuery();

                const string deleEdge = @"
                DELETE EDGE [En]-[Clients]->[Cn] 
                FROM EmployeeNode En, ClientNode Cn
                WHERE En.Workid = 'banana' AND Cn.ClientId = 'apple';";
                command.CommandText = deleEdge;
                command.ExecuteNonQuery();

                //const string querrySupperEdge = @"
                //    select *
                //    from suppernodetest cross apply dboNodeViewEdgeViewDecoder(clients, clientsDeleteCol,  colleagues, colleaguesDeleteCol) as a;";
                //using (var reader = graph.ExecuteReader(querrySupperEdge))
                //{
                //    while (reader.Read())
                //    {
                //    }
                //}

                //graph.DropNodeView("dbo", "suppernodetest");
                //graph.DropEdgeView("dbo", "NodeView", "EdgeView");
            }
        }

        [TestMethod]
        public void PathFunctionTest()
        {
            TestInitialization.ClearDatabase();
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string createClientStr = @"
                    CREATE TABLE [ClientNode] (
                    [ColumnRole: ""NodeId""]
                    [ClientId] [int],
                    [ColumnRole: ""Edge"", Reference: ""ClientNode"", Attributes: {year:""int""}]
                    [Colleagues] [varchar](max),
                    [ColumnRole: ""Edge"", Reference: ""ClientNode""]
                    [Colleagues2] [varchar](max)
                )";
                graph.CreateNodeTable(createClientStr);
                
                var command = new GraphViewCommand(null,graph);

                const string insertNode = @"
                INSERT NODE INTO ClientNode (ClientId) VALUES (0);
                INSERT NODE INTO ClientNode (ClientId) VALUES (1);
                INSERT NODE INTO ClientNode (ClientId) VALUES (2);
                INSERT NODE INTO ClientNode (ClientId) VALUES (3);
                INSERT NODE INTO ClientNode (ClientId) VALUES (4);
                INSERT NODE INTO ClientNode (ClientId) VALUES (5);
                ";
                command.CommandText = insertNode;
                command.ExecuteNonQuery();

                const string insertEdge = @"
                INSERT EDGE INTO ClientNode.Colleagues
                SELECT Cn1, Cn2, 1
                FROM ClientNode  Cn1, ClientNode Cn2
                WHERE Cn1.ClientId  = 0 AND Cn2.ClientId = 1;
                INSERT EDGE INTO ClientNode.Colleagues
                SELECT Cn1, Cn2, 2 
                FROM ClientNode  Cn1, ClientNode Cn2
                WHERE Cn1.ClientId  = 1 AND Cn2.ClientId = 2;
                INSERT EDGE INTO ClientNode.Colleagues
                SELECT Cn1, Cn2, 3
                FROM ClientNode  Cn1, ClientNode Cn2
                WHERE Cn1.ClientId  = 2 AND Cn2.ClientId = 3;
                INSERT EDGE INTO ClientNode.Colleagues
                SELECT Cn1, Cn2, 4 
                FROM ClientNode  Cn1, ClientNode Cn2
                WHERE Cn1.ClientId  = 3 AND Cn2.ClientId = 1;
                INSERT EDGE INTO ClientNode.Colleagues
                SELECT Cn1, Cn2, 5 
                FROM ClientNode  Cn1, ClientNode Cn2
                WHERE Cn1.ClientId  = 1 AND Cn2.ClientId = 4;
                INSERT EDGE INTO ClientNode.Colleagues
                SELECT Cn1, Cn2, 6 
                FROM ClientNode  Cn1, ClientNode Cn2
                WHERE Cn1.ClientId  = 2 AND Cn2.ClientId = 5;";
                command.CommandText = insertEdge;
                command.ExecuteNonQuery();

                //Run following SQL query can get 8 paths:
                //string query = @"
                //    select *
                //    from dbo.dboClientNodeColleaguesbfs(0);";
                //graph.ExecuteNonQuery(query);

                const string deleteEdge = @"
                    DELETE EDGE [Cn1]-[Colleagues]->[Cn2]
                    FROM ClientNode  Cn1, ClientNode Cn2
                    WHERE Cn1.ClientId  = 2 AND Cn2.ClientId = 5;";
                command.CommandText = deleteEdge;
                command.ExecuteNonQuery();

                //Run following SQL query can get 6 paths:
                //graph.ExecuteNonQuery(query);

                graph.DropNodeTable(@"drop table clientnode");
            }
        }

        [TestMethod]
        public void TestPartialUpdate()
        {
            TestInitialization.ClearDatabase();
            TestInitialization.CreateGraphTable();
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                UPDATE EmployeeNode
                SET Clients .write(0x,datalength(Clients),0) WHERE 1!=1";
                using (var command = new GraphViewCommand(q2, graph))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        var count = 0;
                        while (reader.Read())
                        {
                            count++;
                        }
                        Trace.WriteLine(count);
                        //reader.Close();
                    }
                }
            }
        }

        [TestMethod]
        public void TestGraphViewCommand()
        {
            TestInitialization.Init();
            using (var conn = new GraphViewConnection(_connStr))
            {
                conn.Open();
                using (var command = conn.CreateCommand())
                {
                    command.CommandText = @"
                SELECT e1.WorkId, e2.WorkId
                FROM 
                 EmployeeNode AS e1, EmployeeNode AS e2
                MATCH [e1]-[Colleagues]->[e2]";
                    using (var reader = command.ExecuteReader())
                    {
                        var count = 0;
                        while (reader.Read())
                        {
                            count++;
                        }
                        Trace.WriteLine(count);
                    }
                }
            }
        }
    }

}
