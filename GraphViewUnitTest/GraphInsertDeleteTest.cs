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
                INSERT INTO Node (name, age) VALUES ('B', 70);
                INSERT INTO Node (name, age) VALUES ('C', 60);
                INSERT INTO Node (name, age) VALUES ('D', 50);
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
                SELECT A,C,60
                FROM Node A, Node C
                MATCH [A]-[Edge As e]->[B]-[Edge As f]->[C],[A]-[Edge As g]->[C],[A]-[Edge As h]->[D]
                WHERE A.age < 20 AND C.age >55 AND g.Long > 55
                ";
            var command = new GraphViewCommand(sqlStr);
            command.ExecuteNonQuery();
            
        }

        [TestMethod]
        public void DeleteEdge()
        {
//            InsertEdges();
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string sqlStr = @"/*
                DELETE EDGE [D]-[Colleagues]->[F]
                FROM EmployeeNode as D, EmployeeNode as E, EmployeeNode as F
                MATCH [D]-[COLLEAGUES]->[E]-[COLLEAGUES]->[F]
*/
                DELETE EDGE [D]-[Edge as c]->[E]
                FROM Node D, Node E
                WHERE c.hhh='byeworld' AND D.WorkId = 'D' AND E.WorkId = 'E'

                DELETE EDGE [D]-[Edge]->[A]
                FROM Node D, Node A
                WHERE D.WorkId = 'D'
/*
                DELETE EDGE [E]-[Clients]->[A]
                FROM EmployeeNode D, EmployeeNode E, ClientNode A
                MATCH [D]-[COLLEAGUES]->[E]-[Clients as EdgeEA]->[A]
*/                ";

                var command = new GraphViewCommand(sqlStr, graph);
                command.ExecuteNonQuery();
            }
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
                        DELETE FROM Node";

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
