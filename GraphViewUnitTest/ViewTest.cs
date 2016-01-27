using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using GraphView;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewUnitTest
{
    [TestClass]
    public class ViewTest
    {
        private const int NodeNum = 50;
        private const int NodeDegree = 20;
        private void Init()
        {
            TestInitialization.ClearDatabase();
            TestInitialization.CreateTableAndProc();
            TestInitialization.InsertDataByProc(NodeNum,NodeDegree);
        }
        [TestMethod]
        public void ValidateTest()
        {
            TestInitialization.InitAndInsHandmadeData();
            using (var con = new GraphView.GraphViewConnection(TestInitialization.ConnectionString))
            {
                con.Open();
                con.CreateEdgeView(@"
                    CREATE EDGE VIEW Device.Links AS
                    SELECT *
                    FROM Device.STARTDEVICE
                    UNION ALL
                    SELECT *
                    FROM Device.ENDDEVICE
                    ");

                for (int i = 0; i < handmadeData.DeviceNum; ++i)
                {
                    var res = con.ExecuteReader(string.Format(@"SELECT Link.id from Link, Device
                                            MATCH Device-[Links]->Link WHERE Device.id = {0}", i));
                    try
                    {
                        int cnt = 0;
                        while (res.Read())
                        {
                            ++cnt;
                            int x = Convert.ToInt32(res["id"]);
                            if (2*x % handmadeData.DeviceNum != i && 3*x % handmadeData.DeviceNum != i )   //  Any Link #x could be linked to #2*x or #3*x device
                                throw new Exception(string.Format("The Link {0} have wrong device Linked!", i));
                        }
                        if (cnt != 2)
                            throw new Exception(string.Format("The Link {0} doesn't have 2 device Linked!", i));
                    }
                    finally
                    {
                        res.Close();
                    }
                }
            }
        }

        [TestMethod]
        public void NodeViewTest()
        {
            Init();
            using (var conn = new GraphViewConnection(TestInitialization.ConnectionString))
            {
                conn.Open();
                conn.CreateNodeView(@"
                    CREATE NODE VIEW NV1 AS
                    SELECT Workid as id, name
                    FROM EmployeeNode
                    UNION ALL
                    SELECT Clientid as id, null
                    FROM ClientNode");
                conn.CreateNodeView(@"
                    CREATE NODE VIEW NV2 AS
                    SELECT Workid as id, name
                    FROM EmployeeNode
                    WHERE Workid = 'A'");
                conn.CreateNodeView(@"
                    CREATE NODE VIEW NV3 AS
                    SELECT *
                    FROM EmployeeNode
                    UNION ALL
                    SELECT *
                    FROM ClientNode");
                conn.CreateNodeView(@"
                    CREATE NODE VIEW NV4 AS
                    SELECT null
                    FROM EmployeeNode
                    UNION ALL
                    SELECT null
                    FROM ClientNode");
            }

        }

        [TestMethod]
        public void EdgeViewTest()
        {
            NodeViewTest();
            using (var conn = new GraphViewConnection(TestInitialization.ConnectionString))
            {
                conn.Open();
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
                conn.CreateEdgeView(@"
                    CREATE EDGE VIEW EmployeeNode.EV3 AS
                    SELECT *
                    FROM EmployeeNode.Clients
                    UNION ALL
                    SELECT *
                    FROM EmployeeNode.Colleagues
                    ");
                conn.CreateEdgeView(@"
                    CREATE EDGE VIEW EmployeeNode.EV4 AS
                    SELECT null
                    FROM EmployeeNode.Clients
                    UNION ALL
                    SELECT null
                    FROM EmployeeNode.Colleagues
                    ");

                conn.UpdateTableStatistics("dbo","NV1");
                conn.UpdateTableStatistics("dbo", "EmployeeNode");
                conn.UpdateTableStatistics("dbo","GlobalNodeView");
            }
        }

        [TestMethod]
        public void SelectTest()
        {
            EdgeViewTest();
            using (var conn = new GraphViewConnection(TestInitialization.ConnectionString))
            {
                conn.Open();
                conn.ExecuteNonQuery(@" SELECT e1.WorkId, e2.WorkId, c1.ClientId, c2.ClientId, NV1.id, NV2.id
                FROM 
                 EmployeeNode AS e1, EmployeeNode AS e2, ClientNode as c1, ClientNode as c2, NV1, NV2
                MATCH [e1]-[Colleagues as c]->[e2], c1-[Colleagues]->c2, nv1-[ev1]->c1, nv1-[ev2]->nv2, e2-[ev2]->e1
                WHERE e1.workid != NV1.id and NV1.id = 10 and c.a=1 and ev1.a=1");
            }
        }

        [TestMethod]
        public void GlobalViewTest()
        {
            TestInitialization.ClearDatabase();
            TestInitialization.CreateTableAndProc();
            TestInitialization.InsertDataByProc(NodeNum,1);
            using (var conn = new GraphViewConnection(TestInitialization.ConnectionString))
            {
                conn.Open();
                string testQuery = @" 
                SELECT n1.name, n2.name, n3.name
                FROM globalnodeview n1, GlobalNodeView n2, globalnodeview n3
                MATCH [n1]-[colleagues]->[n2]-[clients]->[n3]";
                using (var reader = conn.ExecuteReader(testQuery))
                {
                    int cnt = 0;
                    while (reader.Read())
                    {
                        cnt++;
                    }
                    if (cnt!=NodeNum)
                        Assert.Fail();
                }
            }
        }

        [TestMethod]
        public void GlobalViewTest2()
        {
            EdgeViewTest();
            using (var conn = new GraphViewConnection(TestInitialization.ConnectionString))
            {
                conn.Open();
                const string testQuery = @" 
                SELECT n1.name, n2.name, n3.name
                FROM globalnodeview n1, GlobalNodeView n2, globalnodeview n3
                MATCH [n1]-[ev2]->[n2]-[clients]->[n3]";
                using (var reader = conn.ExecuteReader(testQuery))
                {
                    int cnt = 0;
                    while (reader.Read())
                    {
                        cnt++;
                    }
                    if (cnt != NodeNum*NodeDegree*NodeDegree)
                        Assert.Fail();
                }
            }
        }

        [TestMethod]
        public void ViewAPITest()
        {
            TestInitialization.ClearDatabase();
            using (var graph = new GraphViewConnection(TestInitialization.ConnectionString))
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
    }
}
