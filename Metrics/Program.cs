using GraphView;
using GraphViewUnitTest;
using GraphViewUnitTest.Gremlin;
using System;
using System.Diagnostics;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Metrics
{
    class Program
    {
        static void Main(string[] args)
        {
            // Try to connect your local emulator if the online server is tooooo slow.
            // online
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "Wiki_Temp", GraphType.GraphAPIOnly, false, 1, null);

            /*GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "Cit_Network", GraphType.GraphAPIOnly, false, 1, null);*/

            // local
            /*GraphViewConnection connection = new GraphViewConnection("https://localhost:8081",
                "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
                "NetworkS", "QAQ", GraphType.GraphAPIOnly, false, 1, null);*/

            //connection = GraphOperation.BuildSimpleGraph(connection);
            //connection = GraphOperation.ReadGraphFromEdgesFile(connection, @"D:\network-science\dataset\web-BerkStan.mtx");

            GraphViewCommand g = new GraphViewCommand(connection);

            Console.WriteLine("connection established");
            //Console.WriteLine("{0} vertices", g.g().V().Count().FirstOrDefault());
            //Console.WriteLine("{0} edges", g.g().E().Count().FirstOrDefault());

            //Console.WriteLine("#triangles: {0}", Metrics.TriangleCounting(g));

            Stopwatch st = new Stopwatch();
            st.Start();
            Console.WriteLine("approx. #triangles: {0}", Metrics.ApproxTriangleCountingBySamplingB(g));
            Console.WriteLine("{0} ms", st.ElapsedMilliseconds);

            Console.WriteLine("Please press any key to exit");
            Console.ReadKey();
        }
    }
}
