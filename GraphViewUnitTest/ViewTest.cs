using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using GraphView;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewUnitTest
{
    [TestClass]
    public class ViewTest
    {
        
        private void Init()
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
                    [ColumnRole: ""Edge"", Reference: ""ClientNode"", Attributes: {a:""int"", c:""string"", d:""int"", e:""double""}]
                    [Colleagues] [varchar](max)
                )";
                graph.CreateNodeTable(createEmployeeStr2);

                graph.CreateProcedure(@"
                    CREATE PROCEDURE InsertNode @id varchar(32)
                    as
                    BEGIN
                    INSERT NODE INTO ClientNode (ClientId) VALUES (@id);
                    INSERT NODE INTO EmployeeNode (WorkId) VALUES (@id);
                    END
                    ");

                graph.CreateProcedure(@"
                    CREATE PROCEDURE InsertEmployeeNodeClients @src varchar(32),@sink varchar(32),@a int, @b float, @d int
                    as
                    BEGIN
                    INSERT EDGE INTO EmployeeNode.Clients
                    SELECT En, Cn, @a, @b, @d
                    FROM EmployeeNode En, ClientNode Cn
                    WHERE En.Workid = @src AND Cn.ClientId = @sink;
                    END
                    ");

                graph.CreateProcedure(@"
                    CREATE PROCEDURE InsertEmployeeNodeColleagues @src varchar(32),@sink varchar(32),@a int, @c nvarchar(4000), @d int, @e float
                    as
                    BEGIN
                    INSERT EDGE INTO EmployeeNode.Colleagues
                    SELECT En, Cn, @a, @c, @d, @e
                    FROM EmployeeNode En, EmployeeNode Cn
                    WHERE En.Workid = @src AND Cn.Workid = @sink;
                    END
                    ");

                graph.CreateProcedure(@"
                    CREATE PROCEDURE InsertClientNodeColleagues @src varchar(32),@sink varchar(32),@a int, @c nvarchar(4000), @d int, @e float
                    as
                    BEGIN
                    INSERT EDGE INTO ClientNode.Colleagues
                    SELECT En, Cn, @a, @c, @d, @e
                    FROM ClientNode En, ClientNode Cn
                    WHERE En.ClientId = @src AND Cn.ClientId = @sink;
                    END
                    ");
                const int degree = 40;
                const int num = 100;
                using (var command = graph.CreateCommand())
                {
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = "InsertNode";
                    command.Parameters.Add("@id", SqlDbType.VarChar, 32);
                    for (int i = 0; i < num; i++)
                    {
                        command.Parameters["@id"].Value = i;
                        command.ExecuteNonQuery();
                    }

                    command.CommandText = "InsertEmployeeNodeClients";
                    command.Parameters.Clear();
                    command.Parameters.Add("@src", SqlDbType.VarChar, 32);
                    command.Parameters.Add("@sink", SqlDbType.VarChar, 32);
                    command.Parameters.Add("@a", SqlDbType.Int);
                    command.Parameters.Add("@b", SqlDbType.Float);
                    command.Parameters.Add("@d", SqlDbType.Int);
                    var rnd = new Random();
                    for (int i = 0; i < degree; i++)
                    {
                        command.Parameters["@src"].Value = rnd.Next(0, num - 1);
                        command.Parameters["@sink"].Value = rnd.Next(0, num - 1);
                        command.Parameters["@a"].Value = rnd.Next();
                        command.Parameters["@b"].Value = rnd.NextDouble();
                        command.Parameters["@d"].Value = rnd.Next();
                        command.ExecuteNonQuery();
                    }

                    command.CommandText = "InsertEmployeeNodeColleagues";
                    command.Parameters.Clear();
                    command.Parameters.Add("@src", SqlDbType.VarChar, 32);
                    command.Parameters.Add("@sink", SqlDbType.VarChar, 32);
                    command.Parameters.Add("@a", SqlDbType.Int);
                    command.Parameters.Add("@c", SqlDbType.NVarChar, 4000);
                    command.Parameters.Add("@d", SqlDbType.Int);
                    command.Parameters.Add("@e", SqlDbType.Float);

                    rnd = new Random();
                    for (int i = 0; i < degree; i++)
                    {
                        command.Parameters["@src"].Value = rnd.Next(0, num - 1);
                        command.Parameters["@sink"].Value = rnd.Next(0, num - 1);
                        command.Parameters["@a"].Value = rnd.Next();
                        command.Parameters["@e"].Value = rnd.NextDouble();
                        command.Parameters["@d"].Value = rnd.Next();
                        command.Parameters["@c"].Value = Path.GetRandomFileName().Replace(".", "").Substring(0, 8);
                        command.ExecuteNonQuery();
                    }

                    command.CommandText = "InsertClientNodeColleagues";
                    command.Parameters.Clear();
                    command.Parameters.Add("@src", SqlDbType.VarChar, 32);
                    command.Parameters.Add("@sink", SqlDbType.VarChar, 32);
                    command.Parameters.Add("@a", SqlDbType.Int);
                    command.Parameters.Add("@c", SqlDbType.NVarChar, 4000);
                    command.Parameters.Add("@d", SqlDbType.Int);
                    command.Parameters.Add("@e", SqlDbType.Float);

                    rnd = new Random();
                    for (int i = 0; i < degree; i++)
                    {
                        command.Parameters["@src"].Value = rnd.Next(0, num - 1);
                        command.Parameters["@sink"].Value = rnd.Next(0, num - 1);
                        command.Parameters["@a"].Value = rnd.Next();
                        command.Parameters["@e"].Value = rnd.NextDouble();
                        command.Parameters["@d"].Value = rnd.Next();
                        command.Parameters["@c"].Value = Path.GetRandomFileName().Replace(".", "").Substring(0, 8);
                        command.ExecuteNonQuery();
                    }



                }
                graph.UpdateTableStatistics("dbo", "employeenode");
                graph.UpdateTableStatistics("dbo", "clientnode");

            }
        }

        [TestMethod]
        public void NodeViewTest()
        {
            Init();
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
            }

        }

        [TestMethod]
        public void EdgeViewTest()
        {
            NodeViewTest();
            using (var conn = new GraphViewConnection(TestInitialization.ConnectionString))
            {
                conn.Open();
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

                conn.UpdateTableStatistics("dbo","NV1");
                conn.UpdateTableStatistics("dbo", "EmployeeNode");

            }

        }

        [TestMethod]
        public void SelectTest()
        {
            EdgeViewTest();
            using (var conn = new GraphViewConnection(TestInitialization.ConnectionString))
            {
                conn.Open();
                conn.ExecuteNonQuery(@" SELECT e1.WorkId, e2.WorkId, c1.ClientId, c2.ClientId, NV1.id, NV2.id
                FROM 
                 EmployeeNode AS e1, EmployeeNode AS e2, ClientNode as c1, ClientNode as c2, NV1, NV2
                MATCH [e1]-[Colleagues as c]->[e2], c1-[Colleagues]->c2, NV1-[EV1]->c1, NV1-[EV2]->NV2
                --WHERE e1.workid != NV1.id and NV1.id = 10 and c.a=1 and ev1.a=1");
            }

        }
    }
}
