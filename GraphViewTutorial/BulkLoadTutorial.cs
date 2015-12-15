using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;

namespace GraphViewTutorial
{
    class BulkLoadTutorial
    {
        static string ReaderFileName = "Reader.txt";
        static string BookFileName = "Book.txt";
        static string readRelationFileName = "Read.txt";
        static void generateFiles()
        {
            using (System.IO.StreamWriter file =new System.IO.StreamWriter(ReaderFileName))
            {
                file.WriteLine("Alice,Female");
                file.WriteLine("Bob,Male");
                file.WriteLine("Clever,");
                file.Close();
            }
            using (System.IO.StreamWriter file = new System.IO.StreamWriter(BookFileName))
            {
                file.WriteLine("\"The Three-Body Problem\"");
                file.WriteLine("\"Harry Potter\"");
                file.Close();
            }
            using (System.IO.StreamWriter file = new System.IO.StreamWriter(readRelationFileName))
            {
                file.WriteLine("Alice,\"The Three-Body Problem\"");
                file.WriteLine("Bob,\"The Three-Body Problem\"");
                file.WriteLine("Clever,\"Harry Potter\"");
                file.Close();
            }
        }
        static void deleteFiles()
        {
            System.IO.File.Delete(ReaderFileName);
            System.IO.File.Delete(BookFileName);
            System.IO.File.Delete(readRelationFileName);
        }
        public static void run()
        {
            GraphViewConnection con = new GraphViewConnection(TutorialConfiguration.getConnectionString());
            con.Open();
            con.ClearData();
            try
            {
                generateFiles();

                Console.WriteLine("Creating tables...");
                #region Create a schema for reader and book
                con.CreateNodeTable(@"CREATE TABLE [Book] ( 
                    [ColumnRole:""NodeId""] name VARCHAR(40) )");

                con.CreateNodeTable(@"CREATE TABLE [Reader] ( 
                    [ColumnRole:""NodeId""] name VARCHAR(30),
                    [ColumnRole:""Property""] gender VARCHAR(10),
                    [ColumnRole:""Edge"",Reference:""Book""] Reads VARBINARY(max) )");
                #endregion

                Console.WriteLine("BulkLoading...");
                #region Bulk Load Nodes
                con.BulkInsertNode(ReaderFileName, "Reader", "dbo", null, ",", "\n");
                con.BulkInsertNode(BookFileName, "Book", "dbo", null, ",", "\n");
                con.BulkInsertEdge(readRelationFileName, "dbo", "Reader", "name", "Book", "name", "Reads", null, ",", "\n");
                #endregion

                Console.WriteLine("Querying...");
                #region Query
                var res = con.ExecuteReader(@"SELECT x.name as name1, y.name as name2 FROM Reader x, Book y
                                MATCH x-[Reads]->y ");
                try
                {
                    while (res.Read())
                    {
                        System.Console.WriteLine(res["name1"].ToString() + " Reads " + res["name2"].ToString());
                    }
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
                con.ClearGraphDatabase();
                con.Close();
                deleteFiles();
            }
        }
    }
}
