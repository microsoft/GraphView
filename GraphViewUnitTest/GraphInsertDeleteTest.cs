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

            const string sqlStr = @"
                INSERT INTO Node (name, age) VALUES ('A', 10);
/*
                INSERT INTO Node (name, age) VALUES ('B', 70);
                INSERT INTO Node (name, age) VALUES ('C', 60);
                INSERT INTO Node (name, age) VALUES ('D', 50);
*/
                ";
            var command = new GraphViewCommand(sqlStr);
            command.ExecuteNonQuery();
            
        }

        [TestMethod]
        public void InsertEdges()
        {
            //InsertNodes();
            
            const string sqlStr = @"

                INSERT INTO Edge (Long)
                SELECT A,B,10
                FROM Node A, Node B
                WHERE A.name = 'A' AND B.name = 'B'

                INSERT INTO Edge (Long)
                SELECT A,C,30
                FROM Node A, Node C
                WHERE A.name = 'A' AND C.name = 'C'
/*
                INSERT INTO Edge (Long)
                SELECT A,D,40
                FROM Node A, Node D
                WHERE A.name = 'A' AND D.name = 'D'

                INSERT INTO Edge (Long)
                SELECT B,C,20
                FROM Node B, Node C
                WHERE B.name = 'B' AND C.name = 'C'
*/
";
            var command = new GraphViewCommand(sqlStr);
            command.ExecuteNonQuery();
            
        }

        [TestMethod]
        public void DeleteEdge()
        {
//            InsertEdges();
                const string sqlStr = @"
                DELETE EDGE [A]-[Edge as e]->[C]
                FROM Node A, Node C
                MATCH [A]-[Edge as f]->[C]
                WHERE e.Long<20 AND A.name = 'A' AND f.Long>25"


                var command = new GraphViewCommand(sqlStr);
                command.ExecuteNonQuery();
        }

        [TestMethod]
        public void DeleteNode()
        {
            //InsertEdges();
            try
            {
                using (var graph = new GraphViewConnection(_connStr))
                {
                    graph.Open();
                    const string sqlStr = @"
                        DELETE FROM Node
                        WHERE Node.name = 'A' AND Node.id = 'ad9f8e29-34c3-40a7-ae7b-91aa717b2d5f'
";

                    var command = new GraphViewCommand(sqlStr, graph);
                    command.ExecuteNonQuery();
                }
                //Assert.Fail();
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
            //TestInitialization.Init();
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string sqlStr = @"
                INSERT INTO Node (ClientId, name) VALUES ('lena', 'lena');
                INSERT INTO Node (ClientId, name, age, locate) VALUES ('A', 'Alice', 13, null);
                DELETE FROM Node";
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
