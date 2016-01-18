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
                    FROM ClientNode
                    ");
                conn.CreateNodeView(@"
                    CREATE NODE VIEW NV2 AS
                    SELECT Workid as id, name
                    FROM EmployeeNode
                    WHERE Workid = 'A'");
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
    }
}
