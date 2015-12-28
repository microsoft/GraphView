using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using GraphView;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewUnitTest
{
    //[TestClass]
    public class PatentMatchExperimentTest
    {
        private readonly string _connStr = @"Data Source=4QQBZX12;Integrated Security=True;Database=GraphExperiment";

        private void ClearDatabase()
        {
            using (var conn = new SqlConnection(_connStr))
            {
                var sr = new StreamReader("ClearDatabase.sql");

                conn.Open();
                var command = conn.CreateCommand();
                var transaction = conn.BeginTransaction("ClearDB");
                var clearQuery = sr.ReadToEnd().Split(new string[] {"GO"}, StringSplitOptions.None);

                command.Connection = conn;
                command.Transaction = transaction;

                foreach (var query in clearQuery)
                {
                    if (query == "") continue;
                    command.CommandText = query;
                    command.ExecuteNonQuery();
                }
                //var server = new Server(new ServerConnection(conn));
                //server.ConnectionContext.ExecuteNonQuery(sr.ReadToEnd());

                transaction.Commit();
            }
            //using (var graph = new GraphViewConnection(_connStr))
            //{
            //    graph.Open();
            //    var tables = graph.GetNodeTables();
            //    foreach (var table in tables)
            //    {
            //        graph.DropNodeTable("DROP TABLE " + table.Item1 + "." + table.Item2);
            //    }
            //    graph.DropMetadata();
            //}
        }
        [TestMethod]
        public void CreateGraphTable()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                //ClearDatabase();
                graph.Open();

                //const string createPatentStr = @"
                //CREATE TABLE [Patent_NT] (
                //    [ColumnRole: ""NodeId""]
	            //    patentId INT NOT NULL,
	            //    [ColumnRole: ""Property""]
	            //    gyear INT,
	            //    [ColumnRole: ""Property""]
	            //    gdate INT,
	            //    [ColumnRole: ""Property""]
	            //    ayear INT,
	            //    [ColumnRole: ""Property""]
	            //    country VARCHAR(10),
	            //    [ColumnRole: ""Property""]
	            //    postate VARCHAR(10),
	            //    [ColumnRole: ""Property""]
	            //    assignee INT,
	            //    [ColumnRole: ""Property""]
	            //    asscode INT,
	            //    [ColumnRole: ""Property""]
	            //    claims INT,
	            //    [ColumnRole: ""Property""]
	            //    nclass INT,
	            //    [ColumnRole: ""Property""]
	            //    cat INT,
	            //    [ColumnRole: ""Property""]
	            //    subcat INT,
	            //    [ColumnRole: ""Property""]
	            //    cmade INT,
	            //    [ColumnRole: ""Property""]
	            //    creceive INT,
	            //    [ColumnRole: ""Property""]
	            //    ratiocit DECIMAL(12,5),
	            //    [ColumnRole: ""Property""]
	            //    general DECIMAL(12,5),
	            //    [ColumnRole: ""Property""]
	            //    original DECIMAL(12,5),
	            //    [ColumnRole: ""Property""]
	            //    fwdaplag DECIMAL(12,5),
	            //    [ColumnRole: ""Property""]
	            //    bckgtlag DECIMAL(12,5),
	            //    [ColumnRole: ""Property""]
	            //    selfctub DECIMAL(12,5),
	            //    [ColumnRole: ""Property""]
	            //    selfctlb DECIMAL(12,5),
	            //    [ColumnRole: ""Property""]
	            //    secdupbd DECIMAL(12,5),
	            //    [ColumnRole: ""Property""]
	            //    secdlwbd DECIMAL(12,5),
	            //    [ColumnRole: ""Edge"", Reference: ""Patent_NT""]
	            //    adjacencyList varbinary(8000)
                //)";
                //graph.CreateNodeTable(createPatentStr);
                //graph.UpdateTableStatistics("dbo", "Patent_NT");
                //graph.BatchInsertNode(@"D:\GraphView Patter Matching Exp\apat63_99_new.txt", "Patent_NT",null, null, ",", "\n");
                graph.BulkInsertEdge(@"D:\GraphView Patter Matching Exp\cite75_99_new.txt", "dbo", "Patent_NT", "patentId",
                    "Patent_NT", "patentId", "adjacencyList", null, ",", "\n", true);
            }
        }

        [TestMethod]
        public void DropTable()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();

                const string dropPatentStr = @"drop table Patent_NT_New";
                graph.DropNodeTable(dropPatentStr);
            }
        }
        [TestMethod]
        public void BulkInsertNode()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string filedterminator = @",";
                const string rowterminator = "\n";
                graph.BulkInsertNode(@"D:\GraphView Patter Matching Exp\apat63_99_new.txt", "Patent_NT_New", "dbo", null, filedterminator, rowterminator);
            }
        }

        [TestMethod]
        public void BulkInsertEdge()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string filedterminator = @",";
                const string rowterminator = "\n";
                graph.BulkInsertEdge(@"D:\GraphView Patter Matching Exp\cite75_99_new.txt", "dbo", "Patent_NT_New", "patentid", "Patent_NT_New", "patentid",
                    "adjacencyList",
                    null, filedterminator, rowterminator, false);
            }
        }

        [TestMethod]
        public void Query1()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                set transaction isolation level read uncommitted;
                SELECT count(*)
                FROM Patent_NT as N1a, Patent_NT as N2, Patent_NT as N3
                MATCH N1a-[adjacencyList]->N2, [N1a]-[adjacencyList]->N3
                WHERE N1a.gyear = 1990 and N2.gyear = 1965 and N3.gyear = 1966;";
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
        public void Query2()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                set transaction isolation level read uncommitted;
                SELECT count(*)
                FROM Patent_NT as N1a, Patent_NT as N2, Patent_NT as N3, Patent_NT as N4
                MATCH N1a-[adjacencyList]->N2-[adjacencyList]->N4,N1a-[adjacencyList]->N3-[adjacencyList]->N4
                where N1a.gyear = 1990 and
                     N2.gyear > 1985 and
                     N4.gyear > 1990 and
                     N3.gyear > 1986;";
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
        public void Query3()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                set transaction isolation level read uncommitted;
                SELECT count(*)
                FROM Patent_NT as A, Patent_NT as B, Patent_NT as C, Patent_NT as D
                MATCH A-[adjacencyList as E1]->B-[adjacencyList as E3]->D,A-[adjacencyList as E2]->C-[adjacencyList as E4]->D
                where 
	                A.gyear = 1990
                and B.gyear > 1985
                and C.gyear > 1986
                and D.gyear = 1990;";
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
        public void Triangle()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                DBCC FREEPROCCACHE;
                DBCC DROPCLEANBUFFERS;
                GO
                set transaction isolation level read uncommitted;
                set statistics io on
                set statistics time on
                go
                SELECT count(*)
                FROM Patent_NT as A, Patent_NT as B, Patent_NT as C
                MATCH A-[adjacencyList as E1]->B,
                      A-[adjacencyList as E2]->C,
                      B-[adjacencyList as E3]->C
                WHERE
	                A.gyear = 1990
                and B.gyear > 1985
                and C.gyear > 1986";
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
        public void Rectangle()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                DBCC FREEPROCCACHE;
                DBCC DROPCLEANBUFFERS;
                GO
                set transaction isolation level read uncommitted;
                set statistics io on
                set statistics time on
                go
                SELECT count(*)
                FROM 
                    Patent_NT as A, 
                    Patent_NT as B, 
                    Patent_NT as C, 
                    Patent_NT as D
                MATCH 
                    A-[adjacencyList as E1]->B,
                    B-[adjacencyList as E2]->C,
                    C-[adjacencyList as E3]->D,
                    A-[adjacencyList as E4]->D
                WHERE
	                A.gyear = 1990
                and B.gyear > 1985
                and C.gyear > 1986
                and D.gyear > 1985 ";
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
        public void LongChian()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                DBCC FREEPROCCACHE;
                DBCC DROPCLEANBUFFERS;
                GO
                set transaction isolation level read uncommitted;
                set statistics io on
                set statistics time on
                go
                SELECT count(*)
                FROM 
                    Patent_NT as A, 
                    Patent_NT as B, 
                    Patent_NT as C, 
                    Patent_NT as D,
                    Patent_NT as E,
                    Patent_NT as F
                MATCH 
                    A-[adjacencyList as E1]->B-[adjacencyList as E2]->C-[adjacencyList as E3]->D-[adjacencyList as E4]->E-[adjacencyList as E5]->F
                WHERE
	                A.gyear = 1990
                and B.gyear > 1985
                and C.gyear > 1986
                and D.gyear > 1985
                and E.gyear > 1985
                and F.gyear = 1990
                and A.patentid != B.patentid and B.patentid != C.patentid and C.patentid != D.patentid and D.patentid != E.patentid and E.patentid != F.patentid
                ";
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
        public void Pentagon()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                DBCC FREEPROCCACHE;
                DBCC DROPCLEANBUFFERS;
                GO
                set transaction isolation level read uncommitted;
                set statistics io on
                set statistics time on
                go
                SELECT count(*)
                FROM 
                    Patent_NT as A, 
                    Patent_NT as B, 
                    Patent_NT as C, 
                    Patent_NT as D,
                    Patent_NT as E
                MATCH 
                    A-[adjacencyList as E1]->B-[adjacencyList as E2]->C-[adjacencyList as E3]->D,
                    A-[adjacencyList as E4]->E-[adjacencyList as E5]->D
                WHERE
	                A.gyear = 1990
                and B.gyear > 1985
                and C.gyear > 1986
                and E.gyear > 1985
                and D.gyear = 1990";
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
        public void ThreeLeavesToLeaves()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                DBCC FREEPROCCACHE;
                DBCC DROPCLEANBUFFERS;
                GO
                set transaction isolation level read uncommitted;
                set statistics io on
                set statistics time on
                go
                SELECT count(*)
                FROM 
                    Patent_NT as A, 
                    Patent_NT as B, 
                    Patent_NT as C, 
                    Patent_NT as D,
                    Patent_NT as E
                MATCH 
                    A-[adjacencyList as E1]->B,
                    A-[adjacencyList as E2]->C,
                    A-[adjacencyList as E3]->D,
                    E-[adjacencyList as E4]->B,
                    E-[adjacencyList as E5]->C,
                    E-[adjacencyList as E6]->D,                    
                WHERE
	                A.gyear = 1990
	            and E.gyear = 1990
                and B.gyear = 1990 and B.patentid != C.patentid
                and C.gyear = 1990 and C.patentid != D.patentid
                and D.gyear = 1990 and D.patentid != B.patentid
                and A.patentid != E.patentid";
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
        public void DoubleDiamon()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                DBCC FREEPROCCACHE;
                DBCC DROPCLEANBUFFERS;
                GO
                set transaction isolation level read uncommitted;
                set statistics io on
                set statistics time on
                go
                SELECT count(*)
                FROM 
                    Patent_NT as A, 
                    Patent_NT as B, 
                    Patent_NT as C, 
                    Patent_NT as D,
                    Patent_NT as En,
                    Patent_NT as S1,
                    Patent_NT as S2
                MATCH 
                    S1-[adjacencyList as E1]->A-[adjacencyList as E2]->En,
                    S1-[adjacencyList as E3]->B-[adjacencyList as E4]->En,
                    S2-[adjacencyList as E5]->C-[adjacencyList as E6]->En,
                    S2-[adjacencyList as E7]->D-[adjacencyList as E8]->En                   
                WHERE
	                S1.gyear = 1990
	            and S2.gyear = 1990
                and S1.patentid != S2.patentid
                and A.gyear = 1990
                and B.gyear = 1990
                and C.gyear = 1990
                and D.gyear = 1990
                and En.gyear = 1990";
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
        public void TripleTriangle()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                DBCC FREEPROCCACHE;
                DBCC DROPCLEANBUFFERS;
                GO
                set transaction isolation level read uncommitted;
                set statistics io on
                set statistics time on
                go
                SELECT count(*)
                FROM 
                    Patent_NT as A, 
                    Patent_NT as B, 
                    Patent_NT as C, 
                    Patent_NT as D,
                    Patent_NT as S
                MATCH 
                    S-[adjacencyList as E1]->A,
                    S-[adjacencyList as E2]->B,
                    S-[adjacencyList as E3]->C,
                    S-[adjacencyList as E4]->D,
                    A-[adjacencyList as E5]->B-[adjacencyList as E6]->C-[adjacencyList as E7]->D         
                WHERE
	                S.gyear = 1990
                and A.gyear > 1985
                and B.gyear > 1985
                and C.gyear > 1985
                and D.gyear > 1985
                ";
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
        public void InFlux()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
                SELECT A.patentid, B.patentid, C.patentid, D.patentid, E.patentid
                FROM Patent_NT as A, Patent_NT as B, Patent_NT as C, Patent_NT as D, Patent_NT as E
                MATCH A-[adjacencyList]->B,
                      A-[adjacencyList]->C,
                      B-[adjacencyList]->C,
                      D-[adjacencyList]->C,          
                      E-[adjacencyList]->B
                WHERE
                    A.gyear = 1990
                and B.gyear = 1990
                and C.gyear = 1990
                and D.gyear = 1990
                and E.gyear = 1990         
          
                ";
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
        public void Stat()
        {
            using (var graph = new GraphViewConnection(_connStr))
            {
                graph.Open();
                const string q2 = @"
DBCC SHOW_STATISTICS (""Patent_NT"", [dboPatent_NT_PK_GlobalNodeId]) with DENSITY_VECTOR
DBCC SHOW_STATISTICS (""Patent_NT"", [dboPatent_NT_PK_GlobalNodeId]) with DENSITY_VECTOR
DBCC SHOW_STATISTICS (""Patent_NT"", [dboPatent_NT_PK_GlobalNodeId]) with DENSITY_VECTOR
                ";
                using (var command = new GraphViewCommand(q2, graph))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        var count = 0;
                        string a = "1";
                        while (reader.Read())
                        {
                            a = Convert.ToString(reader["All density"]);
                            count ++;
                        }
                        var b = a;
                        Trace.WriteLine(count);
                        //reader.Close();
                    }
                }
            }
        }
    }
}