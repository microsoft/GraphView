using System.IO;
using GraphView;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace GraphViewUnitTest
{
    [TestClass]
    public class ParseMatchTest
    {
        private readonly string _connStr =
            System.Configuration.ConfigurationManager
                  .ConnectionStrings["GraphViewDbConnectionString"].ConnectionString;

        private static void TestMatchExistence(WSqlFragment fragment)
        {
            var script = fragment as WSqlScript;
            Assert.IsNotNull(script);
            Assert.AreNotEqual(script.Batches.Count, 0);
            Assert.AreNotEqual(script.Batches[0].Statements.Count, 0);
            //foreach (var queryBlock in script.Batches[0].Statements.Select(statement => statement as WSelectQueryBlock))
            //{
            //    Assert.IsNotNull(queryBlock);
            //    Assert.IsNotNull(queryBlock.MatchClause);
            //}
            foreach (var selectStat in script.Batches[0].Statements.Select(statement => statement as WSelectStatement))
            {
                Assert.IsNotNull(selectStat);
                var queryBlock = selectStat.QueryExpr as WSelectQueryBlock;
                Assert.IsNotNull(queryBlock);
                Assert.IsNotNull(queryBlock.MatchClause);
            }
        }

        [TestMethod]
        public void TestMatchWithoutWhereClause()
        {
            const string sqlStr = @"SELECT En2.Name, Cn.Name 
                                    FROM EmployeeNode AS En1, EmployeeNode AS En2
                                    MATCH En1-[Colleagues]->En2";

            var preParser = new GraphViewParser();
            IList<ParseError> errors;
            var sr = new StringReader(sqlStr);
            var fragment = preParser.Parse(sr, out errors);
            var script = fragment as WSqlScript;
            Assert.IsNotNull(script);
            Assert.AreNotEqual(script.Batches.Count, 0);
            Assert.AreNotEqual(script.Batches[0].Statements.Count, 0);
            //var queryBlock = script.Batches[0].Statements[0] as WSelectQueryBlock;
            //Assert.IsNotNull(queryBlock);
            var selectStat = script.Batches[0].Statements[0] as WSelectStatement;
            Assert.IsNotNull(selectStat);
            var queryBlock = selectStat.QueryExpr as WSelectQueryBlock;
            Assert.IsNotNull(queryBlock);
            Assert.IsNotNull(queryBlock.MatchClause);
            Assert.AreEqual(queryBlock.MatchClause.Paths.Count, 1);
            Assert.AreEqual(queryBlock.MatchClause.Paths[0].PathEdgeList.Count, 1);
            Assert.IsNotNull(queryBlock.MatchClause.Paths[0].Tail);
        }

        [TestMethod]
        public void TestMultipleStatements()
        {
            const string sqlStr = @"SELECT En2.Name, Cn.Name 
                                    FROM EmployeeNode AS En1, EmployeeNode AS En2,
                                         ClientNode AS Cn JOIN Region AS R
                                         ON R.RegionID = Cn.RegionID
                                    MATCH En1-[Colleagues]->En2-[Clients]->Cn,
                                          En1-[Clients]->Cn;
                                    SELECT En2.Name, Cn.Name 
                                    FROM EmployeeNode AS En1, EmployeeNode AS En2,
                                         ClientNode AS Cn JOIN Region AS R
                                         ON R.RegionID = Cn.RegionID
                                    MATCH En1-[Colleagues]->En2-[Clients]->Cn,
                                          En1-[Clients]->Cn;";

            var preParser = new GraphViewParser();
            IList<ParseError> errors;
            var sr = new StringReader(sqlStr);
            var fragment = preParser.Parse(sr, out errors);
            TestMatchExistence(fragment);
        }

        [TestMethod]
        public void TestMultipleStatements2()
        {
            const string sqlStr = @"SELECT En2.Name, Cn.Name 
                                    FROM EmployeeNode AS En1, EmployeeNode AS En2,
                                         ClientNode AS Cn JOIN Region AS R
                                         ON R.RegionID = Cn.RegionID;
                                    SELECT En2.Name, Cn.Name 
                                    FROM EmployeeNode AS En1, EmployeeNode AS En2,
                                         ClientNode AS Cn JOIN Region AS R
                                         ON R.RegionID = Cn.RegionID
                                    MATCH En1-[Colleagues]->En2-[Clients]->Cn,
                                          En1-[Clients]->Cn";

            var preParser = new GraphViewParser();
            IList<ParseError> errors;
            var sr = new StringReader(sqlStr);
            var fragment = preParser.Parse(sr, out errors);
            var script = fragment as WSqlScript;
            Assert.IsNotNull(script);
            Assert.AreNotEqual(script.Batches.Count, 0);
            Assert.AreNotEqual(script.Batches[0].Statements.Count, 0);
            //var queryBlock = script.Batches[0].Statements[0] as WSelectQueryBlock;
            var selectStat = script.Batches[0].Statements[0] as WSelectStatement;
            Assert.IsNotNull(selectStat);
            var queryBlock = selectStat.QueryExpr as WSelectQueryBlock;
            Assert.IsNotNull(queryBlock);
            Assert.IsNull(queryBlock.MatchClause);
            //queryBlock = script.Batches[0].Statements[1] as WSelectQueryBlock;
            selectStat = script.Batches[0].Statements[1] as WSelectStatement;
            Assert.IsNotNull(selectStat);
            queryBlock = selectStat.QueryExpr as WSelectQueryBlock;
            Assert.IsNotNull(queryBlock);
            Assert.IsNotNull(queryBlock.MatchClause);
        }

        [TestMethod]
        public void TestMatchClauseSplitNormalStatement()
        {
            const string sqlStr = @"SELECT En2.Name, Cn.Name 
                                    FROM EmployeeNode AS En1, EmployeeNode AS En2,
                                         ClientNode AS Cn JOIN Region AS R
                                         ON R.RegionID
                                    MATCH En1-[Colleagues]->En2-[Clients]->Cn,
                                          En1-[Clients]->Cn = Cn.RegionID";
            var parser = new GraphViewParser();
            try
            {
                IList<ParseError> errors;
                var sr = new StringReader(sqlStr);
                var fragment = parser.Parse(sr, out errors);
                Assert.Fail();
            }
            catch (Exception)
            {
                //Assert.AreEqual(e.Message, "MATCH clause should exactly follow FROM clause");
            }
        }

        [TestMethod]
        public void TestIncompleteMatchClause1()
        {
            const string sqlStr = @"SELECT En2.Name, Cn.Name 
                                    FROM EmployeeNode AS En1, EmployeeNode AS En2,
                                         ClientNode AS Cn JOIN Region AS R
                                         ON R.RegionID = Cn.RegionID
                                    MATCH En1-[Colleagues]->En2-[Clients]->";
            var parser = new GraphViewParser();
            try
            {
                IList<ParseError> errors;
                var queryStr = new StringReader(sqlStr);
                var fragment = parser.Parse(queryStr, out errors);
                Assert.AreNotEqual(errors.Count, 0);

                var script = fragment as WSqlScript;
                Assert.IsNotNull(script);
                Assert.AreNotEqual(script.Batches.Count, 0);
                Assert.AreNotEqual(script.Batches[0].Statements.Count, 0);
                var queryBlock = script.Batches[0].Statements[0] as WSelectQueryBlock;
                Assert.IsNotNull(queryBlock);
                Assert.IsNull(queryBlock.MatchClause);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        [TestMethod]
        public void TestIncompleteMatchClause2()
        {
            const string sqlStr = @"SELECT En2.Name, Cn.Name 
                                    FROM EmployeeNode AS En1, EmployeeNode AS En2,
                                         ClientNode AS Cn JOIN Region AS R
                                         ON R.RegionID = Cn.RegionID
                                    MATCH En1";
            var parser = new GraphViewParser();
            try
            {
                IList<ParseError> errors;
                var sr = new StringReader(sqlStr);
                var fragment = parser.Parse(sr, out errors);
                Assert.AreNotEqual(errors.Count, 0);

                var script = fragment as WSqlScript;
                Assert.IsNotNull(script);
                Assert.AreNotEqual(script.Batches.Count, 0);
                Assert.AreNotEqual(script.Batches[0].Statements.Count, 0);

                //var queryBlock = script.Batches[0].Statements[0] as WSelectQueryBlock;
                var selectStat = script.Batches[0].Statements[0] as WSelectStatement;
                Assert.IsNotNull(selectStat);
                var queryBlock = selectStat.QueryExpr as WSelectQueryBlock;
                Assert.IsNotNull(queryBlock);
                Assert.IsNull(queryBlock.MatchClause);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            
        }

        [TestMethod]
        public void TestNoMatchClause1()
        {
            const string sqlStr = @"SELECT En2.Name, Cn.Name 
                                    FROM EmployeeNode AS En1, EmployeeNode AS En2,
                                         ClientNode AS Cn JOIN Region AS R
                                         ON R.RegionID = Cn.RegionID";
            var parser = new GraphViewParser();
            IList<ParseError> errors;
            var sr = new StringReader(sqlStr);
            var script = parser.Parse(sr, out errors) as WSqlScript;
            Assert.IsNotNull(script);
            Assert.AreNotEqual(script.Batches.Count, 0);
            Assert.AreNotEqual(script.Batches[0].Statements.Count, 0);
            //var queryBlock = script.Batches[0].Statements[0] as WSelectQueryBlock;
            var selectStat = script.Batches[0].Statements[0] as WSelectStatement;
            Assert.IsNotNull(selectStat);
            var queryBlock = selectStat.QueryExpr as WSelectQueryBlock;
            Assert.IsNotNull(queryBlock);
            Assert.IsNull(queryBlock.MatchClause);
        }

        [TestMethod]
        public void TestParseCreateTableStatement()
        {
            const string sqlStr = @"
CREATE TABLE [dbo].[ClientNode](
    [ColumnRole: ""Property""]
    [name] [varchar] NOT NULL,
    [ColumnRole: ""Edge"", Reference: ""ClientNode""]
    [Friends] [varchar](max),
    [a] [decimal](5,5),
    [ColumnRole: ""NodeId""]
    [id] [bigint] IDENTITY(0, 1)
)";
            var preParser = new GraphViewParser();
            List<WNodeTableColumn> graphColumnList;
            IList<ParseError> errors;
            var script = preParser.ParseCreateNodeTableStatement(sqlStr, out graphColumnList, out errors);
            Assert.AreEqual(graphColumnList.Count, 3);
        }

        [TestMethod]
        public void TestInsertDeleteNodeStatement()
        {
            const string sqlStr = @"
                INSERT NODE INTO EmployeeNode (prop1, prop2) VALUES (1, 2);
                DELETE NODE FROM EmployeeNode WHERE prop1 = 1;
                INSERT INTO normalTable (prop1, prop2) VALUES (2, 3);
                DELETE NODE FROM EmployeeNode WHERE prop1 = 1;
                DELETE FROM normalTable WHERE prop1 = 2;";
            var sr = new StringReader(sqlStr);
            var parser = new GraphViewParser();

            IList<ParseError> errors;
            var script = parser.Parse(sr, out errors) as WSqlScript;
            Assert.IsNotNull(script);
            Assert.AreNotEqual(script.Batches.Count, 0);
            Assert.AreNotEqual(script.Batches[0].Statements.Count, 0);
            Assert.IsTrue(script.Batches[0].Statements[0] is WInsertNodeSpecification);
            Assert.IsTrue(script.Batches[0].Statements[1] is WDeleteNodeSpecification);
            Assert.IsTrue(script.Batches[0].Statements[2] is WInsertSpecification);
            Assert.IsTrue(script.Batches[0].Statements[3] is WDeleteNodeSpecification);
            Assert.IsTrue(script.Batches[0].Statements[4] is WDeleteSpecification);
        }

        [TestMethod]
        public void TestInsertEdgeStatement()
        {
            const string sqlStr = @"
                INSERT EDGE INTO Source.Edge (edgeProp1, edgeProp2)
                SELECT Source, Sink, Value1, Value2
                FROM Source, Sink, S3
                MATCH [Source]-[Edge]->[Sink]
                WHERE S3.Name = 'foo'";
            var sr = new StringReader(sqlStr);
            var parser = new GraphViewParser();

            IList<ParseError> errors;
            var script = parser.Parse(sr, out errors) as WSqlScript;
            Assert.IsNotNull(script);
            Assert.AreNotEqual(script.Batches.Count, 0);
            Assert.AreNotEqual(script.Batches[0].Statements.Count, 0);
            Assert.IsTrue(script.Batches[0].Statements[0] is WInsertEdgeSpecification);
            var ins = script.Batches[0].Statements[0] as WInsertEdgeSpecification;
            var source = ins.SelectInsertSource;
            var qb = source.Select as WSelectQueryBlock;
            Assert.IsNotNull(qb);
            Assert.IsNotNull(qb.MatchClause);
        }

        [TestMethod]
        public void TestDeleteEdgeStatement()
        {
            const string sqlStr = @"
                DELETE EDGE [Source]-[EdgeColumn]->[Sink]
                WHERE Source.Name = 'bar'";
            var sr = new StringReader(sqlStr);
            var parser = new GraphViewParser();

            IList<ParseError> errors;
            var script = parser.Parse(sr, out errors) as WSqlScript;
            Assert.IsNotNull(script);
            Assert.AreNotEqual(script.Batches.Count, 0);
            Assert.AreNotEqual(script.Batches[0].Statements.Count, 0);
            Assert.IsTrue(script.Batches[0].Statements[0] is WDeleteEdgeSpecification);
        }

        [TestMethod]
        public void TestTableNameContext()
        {
            const string sqlStr = @"
                SELECT p.FirstName, p.LastName, e.JobTitle
                FROM Person.Person AS p 
                JOIN HumanResources.Employee AS e
                    ON e.BusinessEntityID = p.BusinessEntityID 
                MATCH [d]-[Job]->[e]
                WHERE EXISTS
                (SELECT *
                    FROM HumanResources.Department AS d
                    JOIN HumanResources.EmployeeDepartmentHistory AS edh
                       ON d.DepartmentID = edh.DepartmentID
                    MATCH [p]-[HR]->[d]
                    WHERE e.BusinessEntityID = edh.BusinessEntityID
                    AND d.Name LIKE 'P%');";
            var sr = new StringReader(sqlStr);
            var parser = new GraphViewParser();
            IList<ParseError> errors;
            var script = parser.Parse(sr, out errors) as WSqlScript;
            Assert.IsNotNull(script);
            using (var conn = new SqlConnection(_connStr))
            {
                conn.Open();
                using (SqlTransaction tx = conn.BeginTransaction())
                {
                    var visitor = new TranslateMatchClauseVisitor(tx);
                    try
                    {
                        visitor.Invoke(script);
                        Assert.Fail();
                    }
                    catch (Exception e)
                    {
                        Assert.IsNotNull(e);
                    }
                }
            }
        }

        [TestMethod]
        public void TestUnknownSqlStatementParsing()
        {
            string sqlStr = @"CREATE PROCEDURE AddTrade
                    @buyerId nvarchar(50),
                    @platform INT,
                    @mobile varchar(20),
                    @telephone varchar(20),
                    @orderId varchar(50),
                    @fullname nvarchar(20)
                    AS
                    BEGIN
                        if (@telephone<>@mobile)
                            INSERT EDGE INTO Account.UseMobile
	                                SELECT a,t FROM Account a , Mobile t
                                    MATCH a-[x]->t
		                                WHERE a.id = @buyerId AND t.id = @telephone ;
                    END";

            var parser = new GraphViewParser();
            var sr = new StringReader(sqlStr);
            IList<ParseError> errors;
            var script = parser.Parse(sr, out errors) as WSqlScript;
            if (errors.Count > 0)
                throw new SyntaxErrorException(errors);

            Console.WriteLine(script.ToString());
        }
    }
}
