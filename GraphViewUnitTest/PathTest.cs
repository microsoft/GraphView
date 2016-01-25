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

        private void Init()
        {
            TestInitialization.ClearDatabase();
            TestInitialization.CreateTableAndProc();
            TestInitialization.InsertDataByProc(10);
        }

        private void CreateView()
        {
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
                conn.UpdateTableStatistics("dbo", "NV1");
                conn.UpdateTableStatistics("dbo", "EmployeeNode");
                conn.UpdateTableStatistics("dbo", "GlobalNodeView");
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
                    MATCH [E1]-[Colleagues * 0 .. 5]->[E2]
                    
                    SELECT e1.WorkId, e2.WorkId
                    FROM 
                     EmployeeNode AS e1, EmployeeNode AS e2
                    MATCH [e1]-[Colleagues as c {a:1, c:""str""}]->[e2]

                    SELECT e1.WorkId, e2.WorkId
                    FROM 
                     EmployeeNode AS e1, EmployeeNode AS e2
                    MATCH [e1]-[Colleagues*1..5 as c {a:1, c:""str""}]->[e2]";

            var parser = new GraphViewParser();
            IList<ParseError> errors;
            var stat = parser.Parse(new StringReader(query), out errors);
        }


        [TestMethod]
        public void PathTranslationTest()
        {
            Init();
            CreateView();
            using (var conn = new GraphViewConnection(TestInitialization.ConnectionString))
            {
                conn.Open();
                var command = conn.CreateCommand();
                command.CommandText = @" 
                    SELECT e1.WorkId, e2.WorkId
                    FROM 
                     EmployeeNode AS e1, EmployeeNode AS e2
                    MATCH [e1]-[Colleagues* as c]->[e2]
                    
                    SELECT e1.WorkId, e2.WorkId
                    FROM 
                     EmployeeNode AS e1, EmployeeNode AS e2
                    MATCH [e1]-[Colleagues*1..5 as c {a:1, c:""str""}]->[e2];

                    SELECT e1.WorkId, e2.WorkId
                    FROM 
                     EmployeeNode AS e1, EmployeeNode AS e2
                    MATCH [e1]-[Colleagues*1..5 as c]->[e2]

                    SELECT e1.WorkId, e2.WorkId
                    FROM 
                     EmployeeNode AS e1, EmployeeNode AS e2
                    MATCH [e1]-[EV2* as c {a:1, c_new:'str'}]->[e2]

                    SELECT e1.WorkId, e2.WorkId
                    FROM 
                     EmployeeNode AS e1, EmployeeNode AS e2
                    MATCH [e1]-[EV2* as c]->[e2]
                    
                    SELECT e1.WorkId, e2.WorkId
                    FROM 
                     EmployeeNode AS e1, EmployeeNode AS e2
                    MATCH [e1]-[Colleagues * 1 .. 1 as c]->[e2]
                    
                    SELECT e1.name, e2.name
                    FROM 
                     GlobalNodeView AS e1, GlobalNodeView AS e2
                    MATCH [e1]-[Colleagues* as c]->[e2];
                    
                    SELECT e1.name, e2.name
                    FROM 
                     GlobalNodeView AS e1, GlobalNodeView AS e2
                    MATCH [e1]-[Colleagues*1..1 as c]->[e2]
                    ";
                command.ExecuteNonQuery();
                //Trace.WriteLine(command.GetTsqlQuery());
            }
        }

        [TestMethod]
        public void RegularEdgePathDisplayTest()
        {
            Init();
            using (var conn = new GraphViewConnection(TestInitialization.ConnectionString))
            {
                conn.Open();
                var command = conn.CreateCommand();
                //Show Path in GV
                string gvQuery = @"
                select path.*
				from ClientNode as c1, ClientNode as c2
				match c1-[colleagues*1 .. 3 as path]->c2
                where c1.ClientId = 0";
                command.CommandText = gvQuery;
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Trace.WriteLine(reader[0]);
                    }
                }
                //Trace.WriteLine(command.GetTsqlQuery());
            }
        }

        [TestMethod]
        public void EdgeViewPathDisplayTest()
        {
            Init();
            CreateView();
            using (var conn = new GraphViewConnection(TestInitialization.ConnectionString))
            {
                conn.Open();
                var command = conn.CreateCommand();
                //Show Path in GV
                string gvQuery = @"
                select path.*
				from NV1 as n1, NV1 as n2
				match n1-[EV1*1 .. 3 as path]->n2
                where n1.id = 0";
                command.CommandText = gvQuery;
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Trace.WriteLine(reader[0]);
                    }
                }
                //Trace.WriteLine(command.GetTsqlQuery());
            }
        }

        [TestMethod]
        public void GlobalViewPathDisplayTest()
        {
            Init();
            //CreateView();
            using (var conn = new GraphViewConnection(TestInitialization.ConnectionString))
            {
                conn.Open();
                var command = conn.CreateCommand();
                //Show Path in GV
                string gvQuery = @"
                select path.*
				from GlobalNodeView  as n1, GlobalNodeView  as n2
				match n1-[Colleagues*1 .. 3 as path]->n2
                where n1._NodeId = 0";
                command.CommandText = gvQuery;
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Trace.WriteLine(reader[0]);
                    }
                }
                //Trace.WriteLine(command.GetTsqlQuery());
            }
        }
       

    }
}
