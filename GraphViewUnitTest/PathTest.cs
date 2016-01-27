using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using GraphView;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewUnitTest
{
    [TestClass]
    public class PathTest
    {

        private void Init()
        {
            TestInitialization.ClearDatabase();
            TestInitialization.CreateTableAndProc();
            TestInitialization.InsertDataByProc(10);
        }

        private void CreateView()
        {
            using (var conn = new GraphViewConnection(TestInitialization.ConnectionString))
            {
                conn.Open();
                conn.CreateNodeView(@"
                    CREATE NODE VIEW NV1 AS
                    SELECT Workid as id, name
                    FROM EmployeeNode
                    UNION ALL
                    SELECT Clientid as id, null
                    FROM ClientNode
                    ");
                conn.CreateNodeView(@"
                    CREATE NODE VIEW NV2 AS
                    SELECT Workid as id, name
                    FROM EmployeeNode
                    WHERE Workid = 'A'");
                conn.CreateEdgeView(@"
                    CREATE EDGE VIEW NV1.EV1 AS
                    SELECT a, b, null as c_new, d as d
                    FROM EmployeeNode.Clients
                    UNION ALL
                    SELECT a as a, null, c, d
                    FROM ClientNode.Colleagues
                    ");

                conn.CreateEdgeView(@"
                    CREATE EDGE VIEW EmployeeNode.EV2 AS
                    SELECT a, b, null as c_new, d as d
                    FROM EmployeeNode.Clients
                    UNION ALL
                    SELECT a as a, null, c, d
                    FROM EmployeeNode.Colleagues
                    ");
                conn.UpdateTableStatistics("dbo", "NV1");
                conn.UpdateTableStatistics("dbo", "EmployeeNode");
                conn.UpdateTableStatistics("dbo", "GlobalNodeView");
            }
        }

        [TestMethod]
        public void ParsePathTest()
        {
            string query = @"
                    SELECT *
                    FROM EmployeeNode as E1,EmployeeNode as E2
                    MATCH [E1]-[Colleagues* as a]->[E2];

                    SELECT *
                    FROM EmployeeNode as E1,EmployeeNode as E2
                    MATCH [E1]-[Colleagues*0..5 AS a]->[E2];

                    SELECT *
                    FROM EmployeeNode as E1,EmployeeNode as E2
                    MATCH [E1]-[Colleagues*0 .. 5 as a]->[E2];
                    
                    SELECT *
                    FROM EmployeeNode as E1,EmployeeNode as E2
                    MATCH [E1]-[Colleagues * 0 .. 5 as a]->[E2];

                    SELECT *
                    FROM EmployeeNode as E1,EmployeeNode as E2
                    MATCH [E1]-[Colleagues * 0 .. 5]->[E2]
                    
                    SELECT e1.WorkId, e2.WorkId
                    FROM 
                     EmployeeNode AS e1, EmployeeNode AS e2
                    MATCH [e1]-[Colleagues as c {a:1, c:""str""}]->[e2]

                    SELECT e1.WorkId, e2.WorkId
                    FROM 
                     EmployeeNode AS e1, EmployeeNode AS e2
                    MATCH [e1]-[Colleagues*1..5 as c {a:1, c:""str""}]->[e2]";

            var parser = new GraphViewParser();
            IList<ParseError> errors;
            var stat = parser.Parse(new StringReader(query), out errors);
        }


        [TestMethod]
        public void PathTranslationTest()
        {
            Init();
            CreateView();
            using (var conn = new GraphViewConnection(TestInitialization.ConnectionString))
            {
                conn.Open();
                var command = conn.CreateCommand();
                command.CommandText = @" 
                    SELECT e1.WorkId, e2.WorkId
                    FROM 
                     EmployeeNode AS e1, EmployeeNode AS e2
                    MATCH [e1]-[Colleagues* as c]->[e2]
                    
                    SELECT e1.WorkId, e2.WorkId
                    FROM 
                     EmployeeNode AS e1, EmployeeNode AS e2
                    MATCH [e1]-[Colleagues*1..5 as c {a:1, c:""str""}]->[e2];

                    SELECT e1.WorkId, e2.WorkId
                    FROM 
                     EmployeeNode AS e1, EmployeeNode AS e2
                    MATCH [e1]-[Colleagues*1..5 as c]->[e2]

                    SELECT e1.WorkId, e2.WorkId
                    FROM 
                     EmployeeNode AS e1, EmployeeNode AS e2
                    MATCH [e1]-[EV2* as c {a:1, c_new:'str'}]->[e2]

                    SELECT e1.WorkId, e2.WorkId
                    FROM 
                     EmployeeNode AS e1, EmployeeNode AS e2
                    MATCH [e1]-[EV2* as c]->[e2]
                    
                    SELECT e1.WorkId, e2.WorkId
                    FROM 
                     EmployeeNode AS e1, EmployeeNode AS e2
                    MATCH [e1]-[Colleagues * 1 .. 1 as c]->[e2]
                    
                    SELECT e1.name, e2.name
                    FROM 
                     GlobalNodeView AS e1, GlobalNodeView AS e2
                    MATCH [e1]-[Colleagues* as c]->[e2];
                    
                    SELECT e1.name, e2.name
                    FROM 
                     GlobalNodeView AS e1, GlobalNodeView AS e2
                    MATCH [e1]-[Colleagues*1..1 as c]->[e2]
                    ";
                command.ExecuteNonQuery();
                //Trace.WriteLine(command.GetTsqlQuery());
            }
        }

        [TestMethod]
        public void RegularEdgePathDisplayTest()
        {
            Init();
            using (var conn = new GraphViewConnection(TestInitialization.ConnectionString))
            {
                conn.Open();
                var command = conn.CreateCommand();
                //Show Path in GV
                string gvQuery = @"
                select path.*
				from ClientNode as c1, ClientNode as c2
				match c1-[colleagues*1 .. 3 as path]->c2
                where c1.ClientId = 0";
                command.CommandText = gvQuery;
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Trace.WriteLine(reader[0]);
                    }
                }
                //Trace.WriteLine(command.GetTsqlQuery());
            }
        }

        [TestMethod]
        public void EdgeViewPathDisplayTest()
        {
            Init();
            CreateView();
            using (var conn = new GraphViewConnection(TestInitialization.ConnectionString))
            {
                conn.Open();
                var command = conn.CreateCommand();
                //Show Path in GV
                string gvQuery = @"
                select path.*
				from NV1 as n1, NV1 as n2
				match n1-[EV1*1 .. 3 as path]->n2
                where n1.id = 0";
                command.CommandText = gvQuery;
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Trace.WriteLine(reader[0]);
                    }
                }
                //Trace.WriteLine(command.GetTsqlQuery());
            }
        }

        [TestMethod]
        public void GlobalViewPathDisplayTest()
        {
            Init();
            //CreateView();
            using (var conn = new GraphViewConnection(TestInitialization.ConnectionString))
            {
                conn.Open();
                var command = conn.CreateCommand();
                //Show Path in GV
                string gvQuery = @"
                select path.*
				from GlobalNodeView  as n1, GlobalNodeView  as n2
				match n1-[Colleagues*1 .. 3 as path]->n2
                where n1._NodeId = 0";
                command.CommandText = gvQuery;
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Trace.WriteLine(reader[0]);
                    }
                }
                //Trace.WriteLine(command.GetTsqlQuery());
            }
        }

        [TestMethod]
        public void bfsPathFunctionTest()
        {
            TestInitialization.InitPathTest();
            using (var graph = new GraphViewConnection(TestInitialization.ConnectionString))
            {
                graph.Open();
                var command = new GraphViewCommand(null,graph);

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

                //Show Path on Edge view of employeenode.colleagues and clientnode.colleagues by global node view.
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
                //graph.DropNodeTable(@"drop table clientnode");

                TestInitialization.AddNewTableForPathTest();
                //Show Path on Edge view of employeenode.colleagues,clientnode.colleagues and usernode.colleagues by global node view.
                //Run following SQL query can get 8 paths:
                const string edgeViewShowPath2 = @"
                select dbo.dbo_GlobalNodeView_colleagues_PathMessageDecoder(PathMessage, c._NodeType, c._NodeId)
                from GlobalNodeView cross apply 
                    dbo_GlobalNodeView_colleagues_bfsPathWithMessage(GlobalNodeView.GlobalNodeId,0,-1,
                    GlobalNodeView._NodeType, GlobalNodeView._NodeId,
                    GlobalNodeView.clientnode_colleagues, GlobalNodeView.clientnode_colleaguesDeleteCol, 
                    GlobalNodeView.employeenode_colleagues, GlobalNodeView.employeenode_colleaguesDeleteCol,
					GlobalNodeView.usernode_colleagues, GlobalNodeView.usernode_colleaguesDeleteCol,
                    null, null, null, null, null, null) as pathInfo
                    join GlobalNodeView as c
                    on c.GlobalNodeId = pathInfo.sink
                where GlobalNodeView._NodeId = 20";
                command.CommandText = edgeViewShowPath2;
                cnt = 0;
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        cnt++;
                    }
                }
                if (cnt != 9) Assert.Fail(cnt.ToString());
            }
        }

        [TestMethod]
        public void bfsPathParseTest()
        {
            TestInitialization.InitPathTest();
            using (var graph = new GraphViewConnection(TestInitialization.ConnectionString))
            {
                graph.Open();
                var command = new GraphViewCommand(null,graph);
                int cnt;
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

                //test global node view
                cnt = 0;
                var globalnodeviewQuery = @"
                select *
				from globalnodeview as c1, globalnodeview as c2
				match c1-[colleagues*]->c2
                where c1.ClientId = 10";
                command.CommandText = globalnodeviewQuery;
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        cnt++;
                    }
                }
                if (cnt != 8) Assert.Fail(cnt.ToString());

                //test global node view
                TestInitialization.AddNewTableForPathTest();
                cnt = 0;
                globalnodeviewQuery = @"
                select a.*
				from globalnodeview as c1, globalnodeview as c2
				match c1-[colleagues* as a]->c2
                where c1.ClientId = 20";
                command.CommandText = globalnodeviewQuery;
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        cnt++;
                    }
                }
                if (cnt != 9) Assert.Fail(cnt.ToString());

                ////
                //graph.CreateNodeView(@"
                //    CREATE NODE VIEW NodeView AS
                //    SELECT null
                //    FROM EmployeeNode
                //    UNION ALL
                //    SELECT null
                //    FROM ClientNode");
                ////test Empty-attribute edge view path
                //graph.CreateEdgeView(@"
                //CREATE EDGE VIEW NV1.EV1 AS
                //SELECT null
                //FROM EmployeeNode.Colleagues
                //UNION ALL
                //SELECT null 
                //FROM ClientNode.Colleagues");

                const string deleteEdge = @"
                    DELETE EDGE [Cn1]-[Colleagues]->[Cn2]
                    FROM ClientNode  Cn1, ClientNode Cn2
                    WHERE Cn1.ClientId  = 1 AND Cn2.ClientId = 2;";
                command.CommandText = deleteEdge;
                command.ExecuteNonQuery();

                cnt = 0;
                //Run following SQL query can get 3 paths:
                command.CommandText = gvQuery;
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        cnt++;
                    }
                }
                if (cnt != 3) Assert.Fail(cnt.ToString());
            }
        }

        [TestMethod]
        public void PathWithoutAtttributeTest()
        {
            TestInitialization.ClearDatabase();
            using (var graph = new GraphViewConnection(TestInitialization.ConnectionString))
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

    }
}
