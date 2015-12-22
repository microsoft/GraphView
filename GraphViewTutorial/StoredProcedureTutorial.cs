using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;

namespace GraphViewTutorial
{
    class StoredProcedureTutorial
    {
        //  This is a tutorial for GraphView user about how to create SP and execute SP
        //  Using SP is much more faster than call GraphViewConnection.executeNonQuery()
        //  We highly recommend you create SP for the queries
        //  Please notice that following script will drop all the table in database when it finished
        
        public static void run()
        {
            GraphViewConnection con = new GraphViewConnection(TutorialConfiguration.getConnectionString());
            con.Open();
            con.ClearData();
            try
            {
                #region Create a schema for node
                con.CreateNodeTable(@"CREATE TABLE [Node] ( 
                    [ColumnRole:""NodeId""] id INT, 
                    [ColumnRole:""Edge"",Reference:""Node""] Edges VARBINARY(max) )");
                System.Console.WriteLine("Create table successed!");

                #endregion

                #region Create nodes
                con.ExecuteNonQuery("INSERT INTO [Node](id) VALUES(1)");
                con.ExecuteNonQuery("INSERT INTO [Node](id) VALUES(2)");
                con.ExecuteNonQuery("INSERT INTO [Node](id) VALUES(3)");
                #endregion

                #region Create SP
                con.CreateProcedure(@"CREATE PROCEDURE AddEdge
                        @st INT,
                        @ed INT
                        AS
                        BEGIN
                            INSERT EDGE INTO Node.Edges
                            SELECT s,t FROM
                            Node s , Node t WHERE s.id = @st AND t.id= @ed ;
                        END");
                con.CreateProcedure(@"CREATE PROCEDURE SelectNeighbors
                        @id INT
                        AS
                        BEGIN
                            SELECT y.* FROM Node x, Node y
                                MATCH x-[Edges]->y
                                WHERE x.id = @id
                        END");

                #endregion

                #region Using SP to insert edges
                using (var command = con.CreateCommand())
                {
                    command.CommandType = System.Data.CommandType.StoredProcedure;
                    command.CommandText = "AddEdge";
                    command.Parameters.AddWithValue("@st", 1);
                    command.Parameters.AddWithValue("@ed", 2);
                    command.ExecuteNonQuery();

                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@st", 1);
                    command.Parameters.AddWithValue("@ed", 3);
                    command.ExecuteNonQuery();
                }
                #endregion

                #region Using SP to query
                using (var command = con.CreateCommand())
                {
                    command.CommandType = System.Data.CommandType.StoredProcedure;
                    command.CommandText = "SelectNeighbors";
                    command.Parameters.AddWithValue("@id", 1);
                    var res = command.ExecuteReader();
                    try
                    {
                        while (res.Read())
                        {
                            System.Console.WriteLine(res["id"].ToString());
                        }
                    }
                    finally
                    {
                        if (res != null)
                            res.Close();
                    }
                }
                #endregion
            }
            catch (System.Exception ex)
            {
                System.Console.WriteLine(ex.Message);
            }
            finally //  close the connection and drop table
            {
                con.ClearGraphDatabase();
                con.Close();
            }
        }
    }
}
