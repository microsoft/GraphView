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
    public class GraphSelectQueryTest
    {
        private readonly string _connStr =
            System.Configuration.ConfigurationManager
                .ConnectionStrings["GraphViewDbConnectionString"].ConnectionString;

        public GraphSelectQueryTest()
        {
            TestInitialization.Init();
        }

        [TestMethod]
        public void TestGroupByClause()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                SELECT e1.name
                FROM EmployeeNode AS e1, ClientNode AS e2
                MATCH [e1]-[Clients]->[e2]
                GROUP BY e1.name
                HAVING count(*)>1;";
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
        public void TestDoubleStars()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                SELECT e1.name, *, *
                FROM EmployeeNode AS e1, ClientNode AS e2
                MATCH [e1]-[Clients]->[e2];";
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
        public void TestLongIdentifiers()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                SELECT dbo.EmployeeNode.name, e2.name
                FROM EmployeeNode, ClientNode AS e2
                MATCH [EmployeeNode]-[Clients]->[e2];";
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
        public void TestAttachWhereClause()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                /*
                const string selectStr = @"SELECT En2.Name, Cn.Name 
                                    FROM EmployeeNode AS En1, EmployeeNode AS En2, EmployeeNode AS En5,
                                         ClientNode AS Cn
                                    MATCH En1-[Colleagues]->En2,
                                          En5-[Clients]->Cn
                                    WHERE En1.Name = 'Jane Doe' AND En2.WorkId IN (
                                        SELECT En2.WorkId
                                        FROM EmployeeNode AS En3
                                        MATCH En2-[Colleagues]->En3
                                        WHERE En3.WorkId != En1.WorkId
                                    )";
                */
                const string selectStr = @"SELECT WorkId, En2.Name, Cn.Name 
                                    FROM EmployeeNode, ClientNode AS Cn
                                    MATCH EmployeeNode-[Clients]->Cn
                                    WHERE EmployeeNode.Name = 'Jane Doe' AND WorkId='abc' AND 
                                          (Cn.Name='cde' or WorkId='fgh')";

                var parser = new GraphViewParser();
                IList<ParseError> errors;
                var sr = new StringReader(selectStr);
                var script = parser.Parse(sr, out errors) as WSqlScript;
                Assert.IsNotNull(script);
                var visitor = new TranslateMatchClauseVisitor(graph.Conn);
                visitor.Invoke(script);
            }
        }

        [TestMethod]
        public void TestExecuteSelectStatement()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string selectStr = @"
                SELECT En2.Name
                FROM EmployeeNode AS En1, EmployeeNode AS En2, EmployeeNode AS En5,
                ClientNode AS Cn
                MATCH En1-[Colleagues]->En2,
                En5-[Clients]->Cn
                WHERE En1.Name = 'Jane Doe' AND En2.WorkId IN (
                    SELECT En2.WorkId
                    FROM EmployeeNode AS En3
                    MATCH En3-[Colleagues]->En2
                    --WHERE En3.WorkId != En1.WorkId
                    WHERE En2.WorkId in (select En4.WorkId from EmployeeNode as En4 match En2-[Colleagues]->En4)
                )";
                //                const string selectStr = @"
                //              SELECT En2.Name, Cn.Name 
                //              FROM EmployeeNode AS En1, EmployeeNode AS En2, EmployeeNode AS En5,
                //              ClientNode AS Cn
                //              MATCH En1-[Colleagues]->En2,
                //              En5-[Clients]->Cn
                //              WHERE En1.Name = 'Jane Doe'";

                using (var command = new GraphViewCommand(selectStr, graph))
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
        public void TestMatchClause()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                SELECT e1.WorkId, e2.WorkId, c1.name, c2.name
                FROM 
                 EmployeeNode AS e1, EmployeeNode AS e2, ClientNode as c1, ClientNode as c2
                MATCH [e1]-[Colleagues]->[e2], c1-[Colleagues]->c2";
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
        public void TestMatchClause2()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                SELECT e1.WorkId, e2.WorkId, c1.name, c2.name
                FROM 
                 EmployeeNode AS e1, EmployeeNode AS e2, ClientNode as c1, ClientNode as c2
                MATCH [e1]-[Colleagues]->[e2]-[Clients]->c1-[Colleagues]->c2";
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
        public void TestMatchClause3()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                SELECT count(*)
                FROM 
                 EmployeeNode AS e1, EmployeeNode AS e2, ClientNode as c1, ClientNode as c2
                MATCH [e1]-[Colleagues AS a]->[e2]-[Clients AS b]->c1-[Colleagues AS c]->c2
                WHERE b.credit<10
                --WHERE a.edgeid<10 and b.edgeid<10 and c.edgeid<10";
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
                    }
                }
            }
        }

        #region currently not supported

        //        [TestMethod]
        //        public void TestMatchSelf()
        //        {
        //            using (var graph = new GraphViewConnection(_connStr))
        //            {
        //                graph.Open();
        //                const string q2 = @"
        //                SELECT *
        //                FROM EmployeeNode e1
        //                MATCH e1-[Colleagues]->e1";
        //                using (var reader = graph.ExecuteReader(q2))
        //                {
        //                    while (reader.Read())
        //                    {
        //                    }
        //                    //reader.Close();
        //                }
        //            }
        //        }

        //        [TestMethod]
        //        public void TestMatchSelf2()
        //        {
        //            using (var graph = new GraphViewConnection(_connStr))
        //            {
        //                graph.Open();
        //                const string q2 = @"
        //                SELECT e1.name, e2.name
        //                FROM EmployeeNode e1, EmployeeNode e2
        //                MATCH e1-[Colleagues]->e1-[Colleagues]->e2-[Colleagues]->e2";
        //                using (var reader = graph.ExecuteReader(q2))
        //                {
        //                    while (reader.Read())
        //                    {
        //                    }
        //                    //reader.Close();
        //                }
        //            }
        //        }

        #endregion

        [TestMethod]
        public void TestNoAlias()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                SELECT WorkId
                FROM EmployeeNode, ClientNode
                MATCH EmployeeNode-[Clients]->ClientNode";
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
        public void TestDuplicateAlias()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                SELECT *
                FROM EmployeeNode as e1, ClientNode as e1
                MATCH e1-[Clients]->e1";
                try
                {
                    using (var command = new GraphViewCommand(q2, graph))
                    {
                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                            }
                        }
                    }
                    Assert.Fail();
                }
                catch (GraphViewException e)
                {
                    Assert.IsNotNull(e.Message);
                }




            }
        }

        [TestMethod]
        public void TestCheckWorngTailReference()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                SELECT *
                FROM EmployeeNode as e1, EmployeeNode as e2
                MATCH e1-[Clients]->e2";
                try
                {
                    using (var command = new GraphViewCommand(q2, graph))
                    {
                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                            }
                        }
                    }
                    Assert.Fail();
                }
                catch (GraphViewException e)
                {
                    Assert.IsNotNull(e.Message);
                }
            }
        }

        [TestMethod]
        public void TestCheckWorngSourceReference()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                SELECT *
                FROM mployeeNode as e1, ClientNode as e2
                MATCH e1-[Clients]->e2";
                try
                {
                    using (var command = new GraphViewCommand(q2, graph))
                    {
                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                            }
                        }
                    }
                    Assert.Fail();
                }
                catch (GraphViewException e)
                {
                    Assert.IsNotNull(e.Message);
                }
            }
        }

        [TestMethod]
        public void TestCheckWorngEdgeReference()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                SELECT *
                FROM EmployeeNode as e1, EmployeeNode as e2
                MATCH e1-[Clientss]->e2";
                try
                {
                    using (var command = new GraphViewCommand(q2, graph))
                    {
                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                            }
                        }
                    }
                    Assert.Fail();
                }
                catch (GraphViewException e)
                {
                    Assert.IsNotNull(e.Message);
                }
            }
        }

        [TestMethod]
        public void TestInvalidTable()
        {
            try
            {
                using (var graph = new GraphViewConnection(_connStr))
                {
                    graph.Open();
                    const string q2 = @"
                SELECT *
                FROM _NodeTableColumnCollection;";
                    using (var command = new GraphViewCommand(q2, graph))
                    {
                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                            }
                        }
                    }
                }
            }

            catch (GraphViewException e)
            {
                Assert.IsNotNull(e.Message);
            }
        }

        [TestMethod]
        public void TestSelectStar()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                SELECT *,e1.*
                FROM EmployeeNode as e1, ClientNode as c1, (Select * from EmployeeNode) as e2
                MATCH [e1]-[Clients]->[c1]
                WHERE EXISTS (select * from ClientNode where name=e1.workid);

                select e1.* from employeenode e1, employeenode e2 match [e1]-[colleagues]->[e2]";
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
