using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;

namespace GraphViewTutorial
{
    class InsAndDelTutorial
    {
        //  This is a tutorial for GraphView user showing how to insert or delete nodes and edges
        //  Please notice that following script will drop all the table in databse when it finished
        public static void run()
        {
            GraphViewConnection con = new GraphViewConnection( TutorialConfiguration.getConnectionString());
            con.Open();
            con.ClearData();
            try
            {
                #region Create a schema for node
                con.CreateNodeTable(@"CREATE TABLE [People] ( 
                    [ColumnRole:""NodeId""] id INT, 
                    [ColumnRole:""Property""] name varchar(20), 
                    [ColumnRole:""Edge"",Reference:""People""] Knows VARBINARY(max) )");
                System.Console.WriteLine("Create table successed!");
                #endregion

                #region Create nodes
                con.ExecuteNonQuery("INSERT INTO [People](id,name) VALUES(1,'Alice')");
                con.ExecuteNonQuery("INSERT INTO [People](id,name) VALUES(2,'Bob')");
                con.ExecuteNonQuery("INSERT INTO [People](id,name) VALUES(3,'Caven')");
                con.ExecuteNonQuery("INSERT INTO [People](id,name) VALUES(4,'David')");
                //  You must insert with ColumnRole 'NodeId' if you have
                //  i.e. Following insertion is invalid
                //  con.ExecuteNonQuery("INSERT INTO [People](name) VALUES('Eva')");
                #endregion

                #region Insert edges

                // Alice knows Bob
                con.ExecuteNonQuery(@"INSERT EDGE INTO People.Knows
                                    SELECT x,y FROM People x , People y 
                                        WHERE x.name = 'Alice' AND y.name = 'Bob' ");
                // Bob knows Caven
                con.ExecuteNonQuery(@"INSERT EDGE INTO People.Knows
                                    SELECT x,y FROM People x , People y 
                                        WHERE x.name = 'Bob' AND y.name = 'caven' ");

                // Bob knows David
                con.ExecuteNonQuery(@"INSERT EDGE INTO People.Knows
                                    SELECT x,y FROM People x , People y 
                                        WHERE x.name = 'Bob' AND y.name = 'David' ");
                #endregion

                #region Query 1: find out knowers of knowers

                System.Console.WriteLine("\nQuery 1:");

                var res = con.ExecuteReader(@"SELECT C.* FROM People A, People B, People C
                                MATCH A-[Knows]->B-[Knows]->C
                                WHERE A.name = 'Alice' ");
                try
                {

                    while (res.Read())
                    {
                        System.Console.WriteLine("Name: " + res["name"].ToString());
                    }
                }
                finally
                {
                    if (res != null)
                        res.Close();
                }
                #endregion

                #region Delete edges
                con.ExecuteNonQuery(@"DELETE EDGE [x]-[Knows]->[y] 
                                        FROM People as x, People as y
                                        WHERE y.name='Bob' or y.name = 'Caven' ");
                #endregion

                #region Query 2: find out all edges
                System.Console.WriteLine("\nQuery 2:");

                res = con.ExecuteReader(@"SELECT x.name as name1, y.name as name2 FROM People x, People y
                                MATCH x-[Knows]->y ");

                try
                {
                    while (res.Read())
                    {
                        System.Console.WriteLine(res["name1"].ToString() + " knows " + res["name2"].ToString());
                    }
                }
                finally
                {
                    if (res != null)
                        res.Close();
                }
                #endregion

                #region Delete node
                con.ExecuteNonQuery("DELETE NODE FROM People WHERE People.name <> 'Bob' and People.name <> 'David' "); //  Try to delete all nodes.
                //  Notice that you can not delete a node with edge linked to it.
                //  Above query will not delete Bob and David since there's an edge from Bob to David
                #endregion

                #region Query 3: find out remaining nodes
                System.Console.WriteLine("\nQuery 3:");
                res = con.ExecuteReader("SELECT * FROM [People] ");
                try
                {
                    while (res.Read())
                    {
                        System.Console.WriteLine(res["name"].ToString());
                    }
                }
                finally
                {
                    if (res != null)
                        res.Close();
                }
                #endregion

                #region Delete all edges and nodes
                con.ExecuteNonQuery(@"DELETE EDGE [x]-[Knows]->[y] 
                                        FROM People as x, People as y ");
                con.ExecuteNonQuery("DELETE NODE FROM People ");
                #endregion

                #region Query 4: find out remaining nodes
                System.Console.WriteLine("\nQuery 4:");
                res = con.ExecuteReader("SELECT * FROM [People] ");
                try
                {
                    while (res.Read())
                    {
                        System.Console.WriteLine(res["name"].ToString());
                    }
                    System.Console.WriteLine("Result should be empty");
                }
                finally
                {
                    if (res != null)
                        res.Close();
                }
                #endregion
            }
            catch (System.Exception ex)
            {
                System.Console.WriteLine(ex.Message);
            }
            finally //  close the connection and drop table
            {
                // Drop table and clear all the data
                con.ClearGraphDatabase();
                con.Close();
            }
        }
    }
}
