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

        private void CreateTestTable()
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
                    [ColumnRole: ""Edge"", Reference: ""EmployeeNode"", Attributes: {a:""int"", c:""string"", d:""int"", e:""double""}]
                    [Colleagues] [varchar](max)
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
                    MATCH [E1]-[Colleagues * 0 .. 5]->[E2]";

            var parser = new GraphViewParser();
            IList<ParseError> errors;
            var stat = parser.Parse(new StringReader(query), out errors);
        }
        [TestMethod]
        public void PathTranslationTest()
        {
            CreateTestTable();
            using (var conn = new GraphViewConnection(TestInitialization.ConnectionString))
            {
                conn.Open();
                var command = conn.CreateCommand();
                command.CommandText = @" 
                    SELECT e1.WorkId, e2.WorkId
                    FROM 
                     EmployeeNode AS e1, EmployeeNode AS e2
                    MATCH [e1]-[Colleagues* as c]->[e2]
                    WHERE c.a = 1 and c.c = '123' and c.e = 0.5;
                    
                    SELECT e1.WorkId, e2.WorkId
                    FROM 
                     EmployeeNode AS e1, EmployeeNode AS e2
                    MATCH [e1]-[Colleagues*1..5 as c]->[e2]
                    WHERE c.a = 1 and c.c = '123' and c.e = 0.5
                    
                    SELECT e1.WorkId, e2.WorkId
                    FROM 
                     EmployeeNode AS e1, EmployeeNode AS e2
                    MATCH [e1]-[Colleagues*1..1 as c]->[e2]
                    WHERE c.a > 1 and c.c = '123' and c.e = 0.5
                    
                    SELECT e1.name, e2.name
                    FROM 
                     GlobalNodeView AS e1, GlobalNodeView AS e2
                    MATCH [e1]-[Colleagues* as c]->[e2]
                    WHERE c.a = 1 and c.c = '123' and c.e = 0.5
                    
                    SELECT e1.name, e2.name
                    FROM 
                     GlobalNodeView AS e1, GlobalNodeView AS e2
                    MATCH [e1]-[Colleagues*1..1 as c]->[e2]
                    WHERE c.a > 1 and c.c = '123' and c.e = 0.5";
                Trace.WriteLine(command.GetTsqlQuery());
            }
        }

    }
}
