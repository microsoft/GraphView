using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewUnitTest
{
    [TestClass]
    public class EdgePropertiesTest
    {
        private readonly string _connStr =
            System.Configuration.ConfigurationManager
                  .ConnectionStrings["GraphViewDbConnectionString"].ConnectionString;

        public EdgePropertiesTest()
        {
            TestInitialization.Init();
        }


        [TestMethod]
        public void TestInsertEdgeWithProperties()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                INSERT EDGE INTO EmployeeNode.Clients
                SELECT e1, c1, 1, 1.1, 'helloworld'
                FROM EmployeeNode e1, ClientNode c1
                WHERE e1.name='Patrick' and c1.name='PatrickClient';
                
                INSERT EDGE INTO EmployeeNode.Clients
                SELECT e1, c1, 2, 2.2, 'byeworld'
                FROM EmployeeNode e1, ClientNode c1
                WHERE e1.name='Karmen' and c1.name='KarmenClient'
                
                INSERT EDGE INTO EmployeeNode.Clients
                SELECT e1, c1, 2, 2.2, 'byeworld'
                FROM EmployeeNode e1, ClientNode c1
                WHERE e1.name='Patrick'

                INSERT EDGE INTO EmployeeNode.Colleagues
                SELECT e1, e2
                FROM EmployeeNode e1, EmployeeNode e2
                WHERE e1.name='Patrick'

                INSERT EDGE INTO ClientNode.Colleagues
                SELECT e1, c1
                FROM ClientNode e1, ClientNode c1
                WHERE e1.name='PatrickClient'

                DELETE EDGE [e1]-[Clients]->[c1]
                FROM EmployeeNode e1, ClientNode c1
                WHERE e1.name = 'Patrick' or e1.name = 'Karmen'

                DELETE EDGE [e1]-[Colleagues]->[c1]
                FROM EmployeeNode e1, EmployeeNode c1
                WHERE e1.name = 'Patrick'

                DELETE EDGE [e1]-[Colleagues]->[c1]
                FROM ClientNode e1, ClientNode c1
                WHERE e1.name = 'PatrickClient'";
                var command = new GraphViewCommand(q2, graph);

                command.ExecuteNonQuery();
            }
        }

        [TestMethod]
        public void TestInsertEdgeWithMatch()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                INSERT EDGE INTO EmployeeNode.Clients
                SELECT e1, c1, 2, 2.2, 'byeworld'
                FROM EmployeeNode e1, ClientNode c1
                MATCH [e1]-[Clients]->[c1]
                WHERE e1.name='Karmen';
                               
                DELETE EDGE [e1]-[Clients]->[c1]
                FROM EmployeeNode e1, ClientNode c1
                WHERE e1.name = 'Karmen'";
                var command = new GraphViewCommand(q2,graph);command.ExecuteNonQuery();
            }
        }

        [TestMethod]
        public void TestInsertDeleteEdgeWithProperties()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                INSERT EDGE INTO EmployeeNode.Clients
                SELECT e1, c1, 2, 2.2, 'byeworld'
                FROM EmployeeNode e1, ClientNode c1
                WHERE e1.name='Karmen' and c1.name='KarmenClient'

                DELETE EDGE [EmployeeNode]-[Clients as c]->[ClientNode]
                FROM EmployeeNode, ClientNode
                WHERE c.hhh='byeworld'";
                var command = new GraphViewCommand(q2,graph);command.ExecuteNonQuery();
            }
        }

        [TestMethod]
        public void TestDeleteEdgeWithTableAlias()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                DELETE EDGE [EmployeeNode]-[Clients as c]->[c1]
                FROM EmployeeNode, ClientNode AS c1
                WHERE hhh ='byeworld' and WorkId='1' and c1.name='1'
                
                DELETE EDGE [EmployeeNode]-[Clients]->[ClientNode]
                FROM EmployeeNode, ClientNode
                WHERE EmployeeNode.name ='Patrick' AND Clients.hhh='helloworld'";
                var command = new GraphViewCommand(q2,graph);command.ExecuteNonQuery();
            }
        }

        [TestMethod]
        public void TestMatchClauseWithEdgeProperties()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                SELECT e1.name, e2.name, x.hhh
                FROM EmployeeNode AS e1, ClientNode AS e2, EmployeeNode as e3
                MATCH [e1]-[Colleagues]->[e3]-[Clients AS x]->[e2];

                SELECT e1.name, e2.name, x.credit
                FROM EmployeeNode AS e1, ClientNode AS e2
                MATCH [e1]-[Clients AS x]->[e2]
                WHERE x.credit is null;

                SELECT e1.name, e2.name, x.credit
                FROM EmployeeNode AS e1, ClientNode AS e2
                MATCH [e1]-[Clients AS x]->[e2]
                WHERE x.hhh='123' and e1.Workid like 'a%' and x.credit<2;";
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
        public void TestMatchClauseWithEdgePropertiesNested()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                SELECT e1.name, c2.name, x.credit
                FROM EmployeeNode AS e1, ClientNode AS c2, ClientNode AS c3
                MATCH [e1]-[Clients AS x]->[c3],[e1]-[Clients AS y]->[c2]-[Colleagues as z]->[c3]
                WHERE 
                    EXISTS(
                            SELECT x.credit
                            FROM EmployeeNode AS e1, ClientNode AS c2
                            MATCH [e1]-[Clients as x]->[c2]
                            WHERE x.credit>1 and y.credit>2 and x.aaa<0.8
                          )
                    AND x.credit>4;";
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
        public void TestMultiEdge()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                SELECT e1.name, e2.name, x.hhh, y.credit
                FROM EmployeeNode AS e1, ClientNode as e2
                MATCH [e1]-[Clients as x]->[e2], [e1]-[Clients AS y]->[e2];";
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


    }
}
