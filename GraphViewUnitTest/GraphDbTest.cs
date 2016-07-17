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
                graph.ClearGraphDatabase();
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

        //[TestMethod]
        //public void TestUpdateTableStatistics()
        //{
        //    //TestInitialization.Init();
        //    using (var graph = new GraphViewConnection(_connStr))
        //    {
        //        graph.Open();
        //        graph.UpdateTableStatistics("dbo", "EmployeeNode", "Clients");
        //        graph.UpdateTableStatistics("dbo", "EmployeeNode", "Colleagues");
        //        graph.UpdateTableStatistics("dbo", "GlobalNodeView");
        //    }
        //}


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
        public void EmptySubViewTest()
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
                var edges = new List<Tuple<string, string>>() {Tuple.Create("employeeNode", "CLients")};
                graph.CreateEdgeView("dbo", "Employeenode", "edgeview", edges);
                graph.DropEdgeView("dbo", "Employeenode", "edgeview", false);
            }
        }

        [TestMethod]
        public void ViewTest()
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
                    [ColumnRole: ""Property""]
                    [name] [varchar](32),
                    [ColumnRole: ""Edge"", Reference: ""ClientNode"", Attributes: {a:""int"", c:""string"", d:""int"", e:""double""}]
                    [Colleagues] [varchar](max)
                )";
                graph.CreateNodeTable(createEmployeeStr2);

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
                graph.CreateNodeView("dbo", "NodeView", new List<string>() {
                "ClientNode",
                "EmployeeNode"
                },
                propertymapping);


                List<Tuple<string, string>> Edges = new List<Tuple<string, string>>();
                Edges.Add(Tuple.Create("ClientNode", "Colleagues"));
                Edges.Add(Tuple.Create("employeenode", "clients"));
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
                //    from nodeview cross apply dbo_NodeView_EdgeView_Decoder(clients, clientsDeleteCol,  colleagues, colleaguesDeleteCol) as a;";
                //using (var reader = graph.ExecuteReader(querrySupperEdge))
                //{
                //    while (reader.Read())
                //    {
                //    }
                //}

                //graph.DropEdgeView("dbo", "NodeView", "EdgeView");
                //graph.DropNodeView("dbo", "NodeView");
            }
        }

        [TestMethod]
        public void ViewByDefaultTest()
        {
            TestInitialization.ClearDatabase();
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string createEmployeeStr2 = @"
                CREATE TABLE [ClientNode] (
                    [ColumnRole: ""NodeId""]
                    [ClientId] [varchar](32),
                    [ColumnRole: ""Property""]
                    [number] [bigint],
                    [ColumnRole: ""Property""]
                    [name] [varchar](32),
                    [ColumnRole: ""Edge"", Reference: ""ClientNode"", Attributes: {a:""int"", c:""string"", d:""string""}]
                    [Clients] [varchar](max)
                )";
                graph.CreateNodeTable(createEmployeeStr2);
                const string createEmployeeStr = @"
                CREATE TABLE [EmployeeNode] (
                    [ColumnRole: ""NodeId""]
                    [WorkId] [varchar](32),
                    [ColumnRole: ""Property""]
                    [number] [varchar](32),
                    [ColumnRole: ""Property""]
                    [name] [varchar](32),
                    [ColumnRole: ""Edge"", Reference: ""ClientNode"", Attributes: {a: ""int"", b: ""double"", d:""Int""}]
                    [Clients] [varchar](max),
                    [ColumnRole: ""Edge"", Reference: ""ClientNode"", Attributes: {a: ""int"", b: ""double"", d:""Int""}]
                    [Colleagues] [varchar](max),
                )";
                graph.CreateNodeTable(createEmployeeStr);
                //graph.CreateNodeView("dbo", "NodeView", new List<string>() {
                //"ClientNode",
                //"EmployeeNode"});
                //graph.ClearGraphDatabase();
                //graph.DropNodeView("dbo", "NodeView");
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
                    [ColumnRole: ""Edge"", Reference: ""ClientNode"", Attributes: {year:""int"", ""mouth"":""string"",
                        ""time"":""double"", ""together"":""bool""}]
                    [Colleagues] [varchar](max),
                    [ColumnRole: ""Edge"", Reference: ""ClientNode""]
                    [Colleagues2] [varchar](max)
                )";
                graph.CreateNodeTable(createClientStr);
                
                const string createEmployeeStr = @"
                    CREATE TABLE [EmployeeNode] (
                    [ColumnRole: ""NodeId""]
                    [ClientId] [int],
                    [ColumnRole: ""Edge"", Reference: ""ClientNode"", Attributes: {year:""int"", ""mouth"":""string"",
                        ""time"":""double"", ""together"":""bool""}]
                    [Colleagues] [varchar](max)
                )";
                graph.CreateNodeTable(createEmployeeStr);

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
                SELECT Cn1, Cn2, 1, 'Jan', 1.1, 'true'
                FROM ClientNode  Cn1, ClientNode Cn2
                WHERE Cn1.ClientId  = 0 AND Cn2.ClientId = 1;
                INSERT EDGE INTO ClientNode.Colleagues
                SELECT Cn1, Cn2, 2, 'Feb', 2.2, null 
                FROM ClientNode  Cn1, ClientNode Cn2
                WHERE Cn1.ClientId  = 1 AND Cn2.ClientId = 2;
                INSERT EDGE INTO ClientNode.Colleagues
                SELECT Cn1, Cn2, 3, 'Mar', 3.3, 'true'
                FROM ClientNode  Cn1, ClientNode Cn2
                WHERE Cn1.ClientId  = 2 AND Cn2.ClientId = 3;
                INSERT EDGE INTO ClientNode.Colleagues
                SELECT Cn1, Cn2, 4, 'Apr', 4.4, 'false'
                FROM ClientNode  Cn1, ClientNode Cn2
                WHERE Cn1.ClientId  = 3 AND Cn2.ClientId = 1;
                INSERT EDGE INTO ClientNode.Colleagues
                SELECT Cn1, Cn2, 5, 'May', 5.5, 'true'
                FROM ClientNode  Cn1, ClientNode Cn2
                WHERE Cn1.ClientId  = 1 AND Cn2.ClientId = 4;
                INSERT EDGE INTO ClientNode.Colleagues
                SELECT Cn1, Cn2, 6, 'June', 6.6, 'false'
                FROM ClientNode  Cn1, ClientNode Cn2
                WHERE Cn1.ClientId  = 2 AND Cn2.ClientId = 5;";
                command.CommandText = insertEdge;
                command.ExecuteNonQuery();

                //Run following SQL query can get 8 paths:
                int cnt = 0;
                string query = @"
                select *
				from ClientNode cross apply dbo_ClientNode_Colleagues_bfsPath(ClientNode.GlobalNodeId,0,-1,
				 ClientNode.colleagues, ClientNode.ColleaguesDeleteCol, null, null, null, null)
				where ClientNode.ClientId = 0";
                command.CommandText = query;
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        cnt++;
                    }
                }
                if (cnt!=8) Assert.Fail(cnt.ToString());

                //Show Path on Base node table and ordinary edge.
                cnt = 0;
                query = @"
                select dbo.dbo_ClientNode_Colleagues_PathMessageDecoder(PathMessage, 'ClientNode', c.ClientId)
                from ClientNode cross apply dbo_ClientNode_Colleagues_bfsPathWithMessage(ClientNode.GlobalNodeId,0,-1,'ClientNode', ClientNode.ClientId,
                    ClientNode.colleagues, ClientNode.ColleaguesDeleteCol, null, null, null, null) as pathInfo
                    join ClientNode as c
                    on c.GlobalNodeId = pathInfo.sink
				where ClientNode.ClientId = 0";
                command.CommandText = query;
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        cnt++;
                    }
                }
                if (cnt!=8) Assert.Fail(cnt.ToString());

                //Show Path on Global Node view and Edge view.
                command.CommandText = @"
                INSERT NODE INTO EmployeeNode(ClientId) VALUES (10);";
                command.ExecuteNonQuery();
                command.CommandText = @"
                INSERT EDGE INTO EmployeeNode.Colleagues
                SELECT Cn1, Cn2, 2, null, null, 'true' 
                FROM  EmployeeNode Cn1, ClientNode Cn2
                WHERE Cn1.ClientId = 10 AND Cn2.ClientId = 1;";
                command.ExecuteNonQuery();

                //Run following SQL query can get 8 paths:
                const string edgeViewShowPath = @"
                select dbo.dbo_GlobalNodeView_colleagues_PathMessageDecoder(PathMessage, c._NodeType, c._NodeId)
                from GlobalNodeView cross apply 
                    dbo_GlobalNodeView_colleagues_bfsPathWithMessage(GlobalNodeView.GlobalNodeId,0,-1,
                    GlobalNodeView._NodeType, GlobalNodeView._NodeId,
                    GlobalNodeView.clientnode_colleagues, GlobalNodeView.clientnode_colleaguesDeleteCol, 
                    GlobalNodeView.employeenode_colleagues, GlobalNodeView.employeenode_colleaguesDeleteCol,
                    null, null, null, null) as pathInfo
                    join GlobalNodeView as c
                    on c.GlobalNodeId = pathInfo.sink
                where GlobalNodeView._NodeId = 10";
                command.CommandText = edgeViewShowPath;
                cnt = 0;
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        cnt++;
                    }
                }
                if (cnt != 8) Assert.Fail(cnt.ToString());

                //Show Path in GV
                string gvQuery = @"
                select path.*
				from ClientNode as c1, ClientNode as c2
				match c1-[colleagues* as path]->c2
                where c1.ClientId = 0";
                command.CommandText = gvQuery;
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Trace.WriteLine(reader[0]);
                    }
                }

                // Run following GraphView query can get 8 paths:
                cnt = 0;
                gvQuery = @"
                select *
				from ClientNode as c1, ClientNode as c2
				match c1-[colleagues*]->c2
                where c1.ClientId = 0";
                command.CommandText = gvQuery;
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        cnt++;
                    }
                }
                if (cnt != 8) Assert.Fail(cnt.ToString());


                const string deleteEdge = @"
                    DELETE EDGE [Cn1]-[Colleagues]->[Cn2]
                    FROM ClientNode  Cn1, ClientNode Cn2
                    WHERE Cn1.ClientId  = 1 AND Cn2.ClientId = 2;";
                command.CommandText = deleteEdge;
                command.ExecuteNonQuery();

                cnt = 0;
                //Run following SQL query can get 2 paths:
                command.CommandText = gvQuery;
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        cnt++;
                    }
                }
                if (cnt != 3) Assert.Fail(cnt.ToString());

                //graph.DropNodeTable(@"drop table clientnode");
            }
        }

        [TestMethod]
        public void PathWithoutAtttributeTest()
        {
            TestInitialization.ClearDatabase();
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string createClientStr = @"
                    CREATE TABLE [ClientNode] (
                    [ColumnRole: ""NodeId""]
                    [ClientId] [int],
                    [ColumnRole: ""Edge"", Reference: ""ClientNode""]
                    [Colleagues] [varchar](max),
                    [ColumnRole: ""Edge"", Reference: ""ClientNode""]
                    [Colleagues2] [varchar](max)
                )";
                graph.CreateNodeTable(createClientStr);

                const string createEmployeeStr = @"
                    CREATE TABLE [EmployeeNode] (
                    [ColumnRole: ""NodeId""]
                    [ClientId] [int],
                    [ColumnRole: ""Edge"", Reference: ""ClientNode""]
                    [Colleagues] [varchar](max)
                )";
                graph.CreateNodeTable(createEmployeeStr);

                var command = new GraphViewCommand(null, graph);

                const string insertNode = @"
                INSERT NODE INTO EmployeeNode (ClientId) VALUES (0);
                INSERT NODE INTO ClientNode (ClientId) VALUES (1);
                INSERT NODE INTO ClientNode (ClientId) VALUES (2);
                INSERT NODE INTO ClientNode (ClientId) VALUES (3);
                INSERT NODE INTO ClientNode (ClientId) VALUES (4);
                INSERT NODE INTO ClientNode (ClientId) VALUES (5);";
                command.CommandText = insertNode;
                command.ExecuteNonQuery();
                const string insertEdge = @"
                INSERT EDGE INTO EmployeeNode.Colleagues
                SELECT Cn1, Cn2
                FROM EmployeeNode Cn1, ClientNode Cn2
                WHERE Cn1.ClientId  = 0 AND Cn2.ClientId = 1;
                INSERT EDGE INTO ClientNode.Colleagues
                SELECT Cn1, Cn2
                FROM ClientNode  Cn1, ClientNode Cn2
                WHERE Cn1.ClientId  = 1 AND Cn2.ClientId = 2;
                INSERT EDGE INTO ClientNode.Colleagues
                SELECT Cn1, Cn2
                FROM ClientNode  Cn1, ClientNode Cn2
                WHERE Cn1.ClientId  = 2 AND Cn2.ClientId = 3;
                INSERT EDGE INTO ClientNode.Colleagues
                SELECT Cn1, Cn2
                FROM ClientNode  Cn1, ClientNode Cn2
                WHERE Cn1.ClientId  = 3 AND Cn2.ClientId = 1;
                INSERT EDGE INTO ClientNode.Colleagues
                SELECT Cn1, Cn2
                FROM ClientNode  Cn1, ClientNode Cn2
                WHERE Cn1.ClientId  = 1 AND Cn2.ClientId = 4;
                INSERT EDGE INTO ClientNode.Colleagues
                SELECT Cn1, Cn2
                FROM ClientNode  Cn1, ClientNode Cn2
                WHERE Cn1.ClientId  = 2 AND Cn2.ClientId = 5;";
                command.CommandText = insertEdge;
                command.ExecuteNonQuery();
                //Run following SQL query can get 8 paths:
                const string edgeViewShowPath = @"
                select dbo.dbo_GlobalNodeView_colleagues_PathMessageDecoder(PathMessage, c._NodeType, c._NodeId)
                from GlobalNodeView cross apply 
                    dbo_GlobalNodeView_colleagues_bfsPathWithMessage(GlobalNodeView.GlobalNodeId,0,-1,
                    GlobalNodeView._NodeType, GlobalNodeView._NodeId,
                    GlobalNodeView.clientnode_colleagues, GlobalNodeView.clientnode_colleaguesDeleteCol, 
                    GlobalNodeView.employeenode_colleagues, GlobalNodeView.employeenode_colleaguesDeleteCol) as pathInfo
                    join GlobalNodeView as c
                    on c.GlobalNodeId = pathInfo.sink
                where GlobalNodeView._NodeId = 0";
                command.CommandText = edgeViewShowPath;
                var cnt = 0;
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        cnt++;
                    }
                }
                if (cnt != 8) Assert.Fail(cnt.ToString());
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

        [TestMethod]
        public void TestSelectStatementHints()
        {
            const string query = @"
                SELECT e1.WorkId FROM EmployeeNode e1
                OPTION(MAXDOP 1,FORCE ORDER, merge join, optimize for unknown)";
            var sr = new StringReader(query);
            var parser = new GraphViewParser();
            TestInitialization.Init();
            IList<ParseError> errors;
            var script = parser.Parse(sr, out errors) as WSqlScript;
            Assert.IsNotNull(script);
            var stat = script.Batches[0].Statements[0] as WStatementWithCtesAndXmlNamespaces;
            Assert.IsNotNull(stat);
            Assert.IsNotNull(stat.OptimizerHints);
            using (var conn = new GraphViewConnection(_connStr))
            {
                conn.Open();
                using (var command = conn.CreateCommand())
                {
                //    command.CommandText = @"
                //DECLARE @a varchar(32);
                //SELECT e1.WorkId FROM EmployeeNode e1 where workid = @a
                //OPTION(optimize for (@a = '1'))";
                    command.CommandText = query;
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

        //#region Work Load Test
        //public void CreateGraphTable()
        //{
        //    using (var graph = new GraphViewConnection(_connStr))
        //    {
        //        graph.Open();

        //        const string createPatentStr = @"
        //        CREATE TABLE [Patent_NT] (
        //            [ColumnRole: ""NodeId""]
        //            patentId INT NOT NULL,
        //            [ColumnRole: ""Property""]
        //            gyear INT,
        //            [ColumnRole: ""Property""]
        //            gdate INT,
        //            [ColumnRole: ""Property""]
        //            ayear INT,
        //            [ColumnRole: ""Property""]
        //            country VARCHAR(10),
        //            [ColumnRole: ""Property""]
        //            postate VARCHAR(10),
        //            [ColumnRole: ""Property""]
        //            assignee INT,
        //            [ColumnRole: ""Property""]
        //            asscode INT,
        //            [ColumnRole: ""Property""]
        //            claims INT,
        //            [ColumnRole: ""Property""]
        //            nclass INT,
        //            [ColumnRole: ""Property""]
        //            cat INT,
        //            [ColumnRole: ""Property""]
        //            subcat INT,
        //            [ColumnRole: ""Property""]
        //            cmade INT,
        //            [ColumnRole: ""Property""]
        //            creceive INT,
        //            [ColumnRole: ""Property""]
        //            ratiocit DECIMAL(12,5),
        //            [ColumnRole: ""Property""]
        //            general DECIMAL(12,5),
        //            [ColumnRole: ""Property""]
        //            original DECIMAL(12,5),
        //            [ColumnRole: ""Property""]
        //            fwdaplag DECIMAL(12,5),
        //            [ColumnRole: ""Property""]
        //            bckgtlag DECIMAL(12,5),
        //            [ColumnRole: ""Property""]
        //            selfctub DECIMAL(12,5),
        //            [ColumnRole: ""Property""]
        //            selfctlb DECIMAL(12,5),
        //            [ColumnRole: ""Property""]
        //            secdupbd DECIMAL(12,5),
        //            [ColumnRole: ""Property""]
        //            secdlwbd DECIMAL(12,5),
        //            [ColumnRole: ""Edge"", Reference: ""Patent_NT""]
        //            adjacencyList varbinary(8000)
        //        )";
        //        graph.CreateNodeTable(createPatentStr);
        //    }
        //}

        //public void BulkInsertNode()
        //{
        //    using (var graph = new GraphViewConnection(_connStr))
        //    {
        //        graph.Open();
        //        const string filedterminator = @",";
        //        const string rowterminator = "\n";
        //        graph.BulkInsertNode(@"D:\data2\apat63_99.txt", "Patent_NT", "dbo", null, filedterminator, rowterminator);
        //    }
        //}

        //public void BulkInsertEdge()
        //{
        //    using (var graph = new GraphViewConnection(_connStr))
        //    {
        //        graph.Open();
        //        const string filedterminator = @",";
        //        const string rowterminator = "\n";
        //        graph.BulkInsertEdge(@"D:\data2\cite75_99.txt", "dbo", "Patent_NT", "patentid", "Patent_NT", "patentid",
        //            "adjacencyList",
        //            null, filedterminator, rowterminator);
        //    graph.UpdateTableStatistics("dbo", "Patent_NT");
        //    }
        //}

        //[TestMethod]
        //public void BuildIndex()
        //{

        //    using (var graph = new GraphViewConnection(_connStr))
        //    {
        //        graph.Open();
        //        string query = @"
        //        CREATE NONCLUSTERED COLUMNSTORE INDEX cli
        //            ON dbo.Patent_NT(gyear,gdate,GlobalNodeId);
        //        CREATE NONCLUSTERED INDEX igear on dbo.Patent_NT(gyear);
        //        CREATE NONCLUSTERED INDEX igdate on dbo.Patent_NT(gdate);";
        //        graph.ExecuteNonQuery(query);
        //    }
        //}

        //[TestMethod]
        //public void SetUpWorkLoadTest()
        //{
        //    TestInitialization.ClearDatabase();
        //    CreateGraphTable();
        //    BulkInsertNode();
        //    BulkInsertEdge();
        //    BuildIndex();
        //}

        //[TestMethod]
        //public void ExecuteQuery()
        //{
        //    using (var graph = new GraphViewConnection(_connStr))
        //    {
        //        graph.Open();
        //        string query = @"
		//	//SELECT count(*)
        //        FROM 
        //            Patent_NT as A, 
        //            Patent_NT as B
        //        MATCH
        //           	A-[adjacencyList*0..5]->B
        //        Where A.GYEAR = 1984";
        //        using (var reader = graph.ExecuteReader(query))
        //        {
        //            while (reader.Read())
        //            {
        //                
        //            }
        //        }
        //    }
        //}

        //[TestMethod]
        //public void AllExecuteQuery()
        //{
        //    using (var graph = new GraphViewConnection(_connStr))
        //    {
        //        graph.Open();
        //        var sr = new StreamReader("WorkLoadTest.sql");

        //        var clearQuery = sr.ReadToEnd().Split(new string[] { "GO" }, StringSplitOptions.None);

        //        string query = @"";
        //        foreach (var it in clearQuery)
        //        {
        //            if (it == "") continue;
        //            query = it;
        //            using (var reader = graph.ExecuteReader(query))
        //            {
        //                //while (reader.Read())
        //                //{
        //                //    
        //                //}
        //            }
        //        }
        //    }
        //}

        //#endregion

        [TestMethod]
        public void CreateNodeTable()
        {
            TestInitialization.ClearDatabase();
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();

                const string createPatentStr = @"
                CREATE TABLE [Friends] (
                    [ColumnRole: ""Edge"", Reference: ""Friends""]
                    adjacencyList varbinary(8000)
                )";
                graph.CreateNodeTable(createPatentStr);
            }

        }

        [TestMethod]
        public void AddNodeTableProperty()
        {
            TestInitialization.ClearDatabase();
            TestInitialization.CreateGraphTable();
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();

                const string addPropertyStr = @"
                    ALTER TABLE [dbo].[EmployeeNode]
                    ADD [ColumnRole: ""Property""]
                        phone varchar(32),
                        [ColumnRole: ""Edge"", Reference: ""EmployeeNode""]
                        EtoE varchar(max),
                        [ColumnRole: ""Edge"", Reference: ""ClientNode"", Attributes: {int32: ""int"", str: ""string""}]
                        EtoC varchar(max),
                        [ColumnRole: ""Edge"", Reference: ""Nonexist""]
                        EtoNon int
                ";

                graph.AddNodeTableColumn(addPropertyStr);
            }
        }

        [TestMethod]
        public void DropNodeTableColumn()
        {
            AddNodeTableProperty();

            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();

                //graph.CreateNodeView(@"
                //    CREATE NODE VIEW NV1 AS
                //    SELECT Workid as id, name, phone
                //    FROM EmployeeNode
                //    UNION ALL
                //    SELECT Clientid as id, null, null
                //    FROM ClientNode");
                
                //graph.CreateEdgeView(@"
                //    CREATE EDGE VIEW NV1.EV1 AS
                //    SELECT *
                //    FROM EmployeeNode.EtoC
                //    UNION ALL
                //    SELECT *
                //    FROM ClientNode.Colleagues
                //    ");
                
                //graph.CreateEdgeView(@"
                //    CREATE EDGE VIEW EmployeeNode.EV2 AS
                //    SELECT *
                //    FROM EmployeeNode.Clients
                //    UNION ALL
                //    SELECT *
                //    FROM EmployeeNode.EtoE
                //    ");

                const string dropPropertyStr = @"
                    ALTER TABLE [dbo].[EmployeeNode]
                    DROP COLUMN [phone], EtoE, [EtoC], EtoNon
                ";

                graph.DropNodeTableColumn(dropPropertyStr);
            }
        }
    }

}
