using GraphView;
using GraphViewUnitTest;
using GraphViewUnitTest.Gremlin;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Metrics
{
    class Program
    {
        static GraphViewConnection initializeGraph(GraphViewConnection connection)
        {
            connection = GraphViewConnection.ResetGraphAPICollection(
                connection.DocDBUrl, connection.DocDBPrimaryKey, connection.DocDBDatabaseId, connection.DocDBCollectionId,
                connection.UseReverseEdges, connection.EdgeSpillThreshold
            );
            GraphViewCommand graphCommand = new GraphViewCommand(connection);

            graphCommand.g().AddV().Property("name", "node1").Next();
            graphCommand.g().AddV().Property("name", "node2").Next();
            graphCommand.g().AddV().Property("name", "node3").Next();
            graphCommand.g().AddV().Property("name", "node4").Next();
            graphCommand.g().AddV().Property("name", "node5").Next();

            graphCommand.g().V().Has("name", "node1").AddE("e").To(graphCommand.g().V().Has("name", "node2")).Next();
            graphCommand.g().V().Has("name", "node2").AddE("e").To(graphCommand.g().V().Has("name", "node3")).Next();
            graphCommand.g().V().Has("name", "node1").AddE("e").To(graphCommand.g().V().Has("name", "node3")).Next();
            graphCommand.g().V().Has("name", "node2").AddE("e").To(graphCommand.g().V().Has("name", "node4")).Next();
            graphCommand.g().V().Has("name", "node3").AddE("e").To(graphCommand.g().V().Has("name", "node4")).Next();
            graphCommand.g().V().Has("name", "node4").AddE("e").To(graphCommand.g().V().Has("name", "node5")).Next();
            graphCommand.g().V().Has("name", "node2").AddE("e").To(graphCommand.g().V().Has("name", "node5")).Next();


            graphCommand.Dispose();

            return connection;
        }

        static void Main(string[] args)
        {
            // Try to connect your local emulator if the online server is tooooo slow.
            // online
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "Wiki_Temp", GraphType.GraphAPIOnly, false, 1, null);
            // local
            /*GraphViewConnection connection = new GraphViewConnection(
                "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
                "https://localhost:8081", "NetworkS", "QAQ", GraphType.GraphAPIOnly, false, 1, null);*/

            // connection = initializeGraph(connection);

            GraphViewCommand g = new GraphViewCommand(connection);

            Console.WriteLine("#triangles: {0}", Metrics.TriangleCounting(g));

            Console.WriteLine("Please press any key to exit");
            Console.ReadKey();
        }
    }
}
