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
    public class GraphOptimizationTest
    {
        private readonly string _connStr =
            System.Configuration.ConfigurationManager
                  .ConnectionStrings["GraphViewDbConnectionString"].ConnectionString;

        public GraphOptimizationTest()
        {
            TestInitialization.Init();
        }

        [TestMethod]
        public void Test8LeafToLeafMatch()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                SELECT e1.name, e2.name, e3.name, e4.name, e5.name, e6.name
                FROM EmployeeNode e1, EmployeeNode e2, EmployeeNode e3, EmployeeNode e4, EmployeeNode e5, EmployeeNode e6
                MATCH e1-[Colleagues as a]->e2, e1-[Colleagues as b]->e3, e1-[Colleagues as c]->e4, e1-[Colleagues as d]->e5,
                      e6-[Colleagues as e]->e2, e6-[Colleagues as f]->e3, e6-[Colleagues as g]->e4, e6-[Colleagues as h]->e5
                WHERE a.sink<5 and b.sink<5 and c.sink<5 and d.sink<5 and e.sink<5 and f.sink<5 and g.sink<5 and h.sink<5 and E1.name = 'Abc'
                --WHERE a.edgeId<5 and b.edgeId<5 and b.edgeId<5 and c.edgeId<5 and d.edgeId<5 and e.edgeId<5 and f.edgeId<5 and g.edgeid<5 and h.edgeid<5";
                using (var command = new GraphView.GraphViewCommand(q2, graph))
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
        public void Test2LevelBinaryTreeMatch()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                SELECT e1.name, e2.name, e3.name, e4.name, e5.name, e6.name, e7.name
                FROM EmployeeNode e1, EmployeeNode e2, EmployeeNode e3, EmployeeNode e4, EmployeeNode e5, EmployeeNode e6, EmployeeNode e7
                MATCH e1-[Colleagues as a]->e2, e1-[Colleagues as b]->e3, e2-[Colleagues as c]->e4, e2-[Colleagues as d]->e5, e3-[Colleagues as e]->e6, e3-[Colleagues as f]->e7
                WHERE a.edgeId<5 and b.edgeId<5 and b.edgeId<5 and c.edgeId<5 and d.edgeId<5 and e.edgeId<5 and f.edgeId<5";
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
        public void TestMultiNodeMatch()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                SELECT e1.workid, e2.workid, e3.workid, e4.workid, c5.name, c6.name, c7.name
                FROM EmployeeNode e1, EmployeeNode e2, EmployeeNode e3, EmployeeNode e4, ClientNode c5, ClientNode c6, ClientNode c7
                MATCH   e3-[Manager as a]->e1, 
                        e3-[Colleagues as b]->e2, 
                        e3-[Colleagues as c]->e4, 
                        e2-[Clients as d]->c5, 
                        e3-[Clients as e]->c5, 
                        e3-[Clients as f]->c6,
                        e3-[Clients as g]->c7, 
                        e4-[Clients as h]->c7,
                WHERE /*e1.workid like 'N%' and */a.edgeId<5 and b.edgeId<5 and b.edgeId<5 and c.edgeId<5 and d.edgeId<5 and e.edgeId<5 and f.edgeId<5 and g.edgeId<5 and h.edgeId<5";
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
        public void Test2LeafToLeaf()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                SELECT e1.workid, e3.workid
                FROM EmployeeNode e1, EmployeeNode e2, EmployeeNode e3, ClientNode c1
                MATCH   e1-[Manager as a]->e2, 
                        e3-[Colleagues as b]->e2, 
                        e1-[Clients as c]->c1, 
                        e3-[Clients as d]->c1, 
                --where a.edgeid<5 and b.edgeid<5 and c.edgeid<5 and d.edgeid<5
                --WHERE e1.workid like 'N%' and a.edgeId<5 and b.edgeId<5 and b.edgeId<5 and c.edgeId<5 and d.edgeId<5";
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
        public void Test2LeafToLeaf2()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                SELECT e1.workid, e2.workid
                FROM EmployeeNode e1, EmployeeNode e2, ClientNode c1, ClientNode c2
                MATCH   e1-[Clients as a]->c1,
                        e1-[Clients as b]->c2,
                        e2-[Clients as c]->c1,
                        e2-[Clients as d]->c2,
                WHERE [a].[edgeid] < 5 AND [b].[edgeid] < 5 AND [c].[edgeid] < 5 AND [d].[edgeid] < 5
                --where a.credit<10 and b.credit<10
                --WHERE e1.workid like 'N%' and a.edgeId<5 and b.edgeId<5 and b.edgeId<5 and c.edgeId<5 and d.edgeId<5";
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
        public void TestMultiLeafToLeaf()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                SELECT e1.name,e2.name,e3.name
                FROM EmployeeNode e1, EmployeeNode e2, EmployeeNode e3, EmployeeNode e4
                MATCH   [e1]-[Colleagues as a]->e4,[e3]-[Colleagues as b]->e4,[e2]-[Colleagues as c]->e4,[e1]-[Colleagues as d]->e2 
                WHERE [a].[edgeid] < 25 AND [b].[edgeid] < 25 AND [c].[edgeid] < 25 AND [d].[edgeid] < 25
                --WHERE a.edgeid<5 and b.edgeid<5 and c.edgeid<5 and d.edgeid<5
                --WHERE e1.workid like 'N%' and a.edgeId<5 and b.edgeId<5 and b.edgeId<5 and c.edgeId<5 and d.edgeId<5";
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
        public void Test8LeafToLeafMatchHistogram()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                SELECT e1.name, e2.name, e3.name, e4.name, e5.name, e6.name
                FROM EmployeeNode e1, EmployeeNode e2, EmployeeNode e3, EmployeeNode e4, EmployeeNode e5, EmployeeNode e6
                MATCH e1-[Colleagues as a]->e2, e1-[Colleagues as b]->e3, e1-[Colleagues as c]->e4, e1-[Colleagues as d]->e5,
                      e6-[Colleagues as e]->e2, e6-[Colleagues as f]->e3, e6-[Colleagues as g]->e4, e6-[Colleagues as h]->e5
                WHERE e1.GlobalNodeId<5 and e2.GlobalNodeId<5 and e3.GlobalNodeId<5 and e4.GlobalNodeId<5 and e5.GlobalNodeId<5 and e6.GlobalNodeId<5";
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
        public void TestEstimationSize()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                SELECT count(*) as cnt
                FROM EmployeeNode e1, EmployeeNode e2, EmployeeNode e3
                MATCH e1-[Colleagues as a]->e2,e3-[Colleagues as b]->e2, e2-[Colleagues as c]->e3
                ";
                using (var command = new GraphViewCommand(q2, graph))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        var count = 0;
                        while (reader.Read())
                        {
                            Trace.WriteLine(reader["cnt"].ToString());
                            count++;
                        }
                        //reader.Close();
                    }
                }
            }
        }

    }
}
