using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.IO;
using GraphView;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewUnitTest
{
    [TestClass]
    public class GraphInsertDeleteTest
    {
        private readonly string _connStr =
            System.Configuration.ConfigurationManager
                .ConnectionStrings["GraphViewDbConnectionString"].ConnectionString;

        [TestMethod]
        public void InsertNodes()
        {
            TestInitialization.ClearDatabase();
            TestInitialization.CreateGraphTable();
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string sqlStr = @"
                INSERT NODE INTO ClientNode (ClientId, name) VALUES ('A', 'Ava');
                INSERT NODE INTO ClientNode (ClientId, name) VALUES ('B', 'Bob');
                INSERT NODE INTO ClientNode (ClientId, name) VALUES ('C', 'Chad');

                INSERT NODE INTO EmployeeNode (WorkId, name) VALUES ('D', 'David');
                INSERT NODE INTO EmployeeNode (WorkId, name) VALUES ('E', 'Elisa');
                INSERT NODE INTO EmployeeNode (WorkId, name) VALUES ('F', 'Frank');
                INSERT NODE INTO EmployeeNode (WorkId, name) VALUES ('G', 'Gerge');
               
                ";
                var command = new GraphViewCommand(sqlStr, graph);
                command.ExecuteNonQuery();
            }
        }

        [TestMethod]
        public void InsertEdges()
        {
            InsertNodes();
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string sqlStr = @"
                INSERT EDGE INTO EmployeeNode.Colleagues
                SELECT D,E
                FROM EmployeeNode D, EmployeeNode E
                WHERE D.WORKID = 'D' AND E.WORKID = 'E'

                INSERT EDGE INTO EmployeeNode.Colleagues
                SELECT E,F
                FROM EmployeeNode E, EmployeeNode F
                WHERE E.WORKID = 'E' AND F.WORKID = 'F'

                INSERT EDGE INTO EmployeeNode.Colleagues
                SELECT D,F
                FROM EmployeeNode D, EmployeeNode E, EmployeeNode F
                MATCH [D]-[COLLEAGUES]->[E]-[COLLEAGUES]->[F]
                WHERE D.WORKID = 'D' AND F.WORKID = 'F'

                INSERT EDGE INTO EmployeeNode.Clients
                SELECT D,A,null,null,null
                FROM EmployeeNode D, ClientNode A
                WHERE D.WORKID = 'D' AND A.CLIENTID = 'A'
                ";
                var command = new GraphViewCommand(sqlStr, graph);
                command.ExecuteNonQuery();
            }
        }

        [TestMethod]
        public void DeleteEdge()
        {
            InsertEdges();
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string sqlStr = @"
                DELETE EDGE [D]-[Colleagues]->[F]
                FROM EmployeeNode as D, EmployeeNode as E, EmployeeNode as F
                MATCH [D]-[COLLEAGUES]->[E]-[COLLEAGUES]->[F]

                DELETE EDGE [D]-[Clients]->[A]
                FROM EmployeeNode D, ClientNode A
                WHERE D.WORKID = 'D' AND A.CLIENTID = 'A'";

                var command = new GraphViewCommand(sqlStr, graph);
                command.ExecuteNonQuery();
            }
        }

        [TestMethod]
        public void DeleteNode()
        {
            InsertEdges();
            try
            {
                using (var graph = new GraphViewConnection(_connStr))
                {
                    graph.Open();
                    const string sqlStr = @"
                        DELETE NODE FROM EmployeeNode";

                    var command = new GraphViewCommand(sqlStr, graph);
                    command.ExecuteNonQuery();
                }
                Assert.Fail();
            }
            catch (Exception e)
            {
                if (!e.Message.Contains("being deleted still has/have ingoing or outdoing edge(s)"))
                    Assert.Fail();

            }

        }

        [TestMethod]
        public void TestInsertDeleteEdge()
        {
            TestInitialization.Init();
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string sqlStr = @"
                INSERT EDGE INTO EmployeeNode.Clients
                SELECT En, Cn, 1, 0, 'test'
                FROM EmployeeNode En, ClientNode Cn
                WHERE En.Workid LIKE 'B%' AND Cn.name LIKE 'A%';

                DELETE EDGE [EmployeeNode]-[Clients]->[ClientNode]
                FROM EmployeeNode, ClientNode
                WHERE EmployeeNode.Workid LIKE 'B%' AND ClientNode.name LIKE 'A%';
                ";

                var command = new GraphViewCommand(sqlStr, graph);
                command.ExecuteNonQuery();
            }
        }

        [TestMethod]
        public void TestInsertDeleteNode()
        {
            TestInitialization.Init();
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string sqlStr = @"
                INSERT NODE INTO ClientNode (ClientId, name) VALUES ('lena', 'lena');
                DELETE NODE FROM ClientNode WHERE name = 'lena';";
                var command = new GraphViewCommand(sqlStr, graph);
                command.ExecuteNonQuery();
            }
        }

        [TestMethod]
        public void MergeDeleteCol()
        {
            DeleteEdge();
            using (var graph = new GraphViewConnection(_connStr))
            {

                graph.Open();
                graph.MergeAllDeleteColumn("dbo", "EmployeeNode");
            }

        }

        [TestMethod]
        public void InsertWrongEdges()
        {
            InsertNodes();
            try
            {
                using (var graph = new GraphViewConnection(_connStr))
                {
                    graph.Open();
                    string sqlStr = @"
                        INSERT EDGE INTO EmployeeNode.Colleagues
                        SELECT D,E
                        FROM ClientNode D, ClientNode E
                        WHERE D.ClientId = 'D' AND E.ClientId = 'E'";
                    graph.ExecuteNonQuery(sqlStr);
                }
                Assert.Fail();
            }
            catch (GraphViewException e)
            {
                if (e.Message!="Source node table in the SELECT is mismatched with the INSERT source")
                    Assert.Fail();
            }
           
        }

        [TestMethod]
        public void InsertWrongEdges2()
        {
            InsertNodes();
            try
            {
                using (var graph = new GraphViewConnection(_connStr))
                {
                    graph.Open();
                    string sqlStr = @"
                        INSERT EDGE INTO ClientNode.Manager
                        SELECT D,E
                        FROM ClientNode D, ClientNode E
                        WHERE D.ClientId = 'D' AND E.ClientId = 'E'";
                    graph.ExecuteNonQuery(sqlStr);
                }
                Assert.Fail();
            }
            catch (GraphViewException e)
            {
                if (e.Message != "Node table ClientNode not exists or edge Manager not exists in node table ClientNode")
                    Assert.Fail();
            }
           
        }

        [TestMethod]
        public void InsertWrongEdges3()
        {
            InsertNodes();
            try
            {
                using (var graph = new GraphViewConnection(_connStr))
                {
                    graph.Open();
                    string sqlStr = @"
                        INSERT EDGE INTO ClientNode.Colleagues
                        SELECT D,E
                        FROM ClientNode D, EmployeeNode E
                        WHERE D.ClientId = 'D' AND E.WorkId = 'E'";
                    graph.ExecuteNonQuery(sqlStr);
                }
                Assert.Fail();
            }
            catch (GraphViewException e)
            {
                if (e.Message != "Mismatch sink node table reference EmployeeNode")
                    Assert.Fail();
            }
            
        }
    }
}
