using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using GraphView;

namespace GraphViewFlowTest
{
    //  GraphViewFlowTest is a project for testing GraphView
    //  All testcases need for data "cite75_99.txt" and "apat63_99.txt"
    class Program
    {
        static void Main(string[] args)
        {
            //  You should modify your database information in TestConfiguration
            TestConfiguration.localTest = true;

            var connection = new GraphViewConnection(TestConfiguration.getConnectionString());

            try
            {
                Console.WriteLine("Opening the DB...");
                connection.Open();

                connection.ClearData();
                
                Console.WriteLine("Creating table...");
                connection.CreateNodeTable(TestConfiguration.getCreateTableString());

                string Table = TestConfiguration.TestCaseNodeTableName;
                string NodeId = TestConfiguration.TestCaseNodeIdName;
                string EdgeName = TestConfiguration.TestCaseEdgeName;
                string NodeDataFile = TestConfiguration.NodeDataFileLocation;
                string EdgeDataFile = TestConfiguration.EdgeDataFileLocation;

                Console.WriteLine("BulkLoading Nodes...");
                connection.BulkInsertNode(NodeDataFile , Table, "dbo", null, ",", "\n");

                Console.WriteLine("BulkLoading Edges...");
                connection.BulkInsertEdge(EdgeDataFile ,"dbo", Table, NodeId, Table, NodeId, EdgeName, null, ",", "\n");

                Console.WriteLine("Updating Statistics...");
                connection.UpdateTableStatistics("dbo",Table);  //  updating staticestics allows GraphView do optimization on query precisely

                Console.WriteLine("Tests begin!");

                (new RectangleMatchTest()).run( connection );
                (new DoubleDiamondMatchTest()).run(connection);
                (new InfluxMatchTest()).run(connection);
                (new TripleTriangleMatchTest()).run(connection);
                (new PentagonMatchTest()).run(connection);
                (new TriangleMatchTest()).run(connection);
                (new LongChainsMatchTest()).run(connection);
            }
            finally
            {
                connection.ClearGraphDatabase();
                connection.Close();
            }
        }
    }
}
