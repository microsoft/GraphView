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
        static void Main(string[] args)
        {
            // Try to connect your local emulator if the online server is tooooo slow.
            // online
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "Wiki_Temp", GraphType.GraphAPIOnly, false, 1, null);
            // local
            /*GraphViewConnection connection = new GraphViewConnection("https://localhost:8081",
                "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
                "NetworkS", "QAQ", GraphType.GraphAPIOnly, false, 1, null);*/

            // connection = Utils.ReadGraphFromEdgesFile(connection, @"D:\network-science\dataset\web-BerkStan.mtx");

            GraphViewCommand g = new GraphViewCommand(connection);

            Console.WriteLine("connection established");

            //Console.WriteLine("#triangles: {0}", Metrics.TriangleCounting(g));

            Console.WriteLine("approx. #triangles: {0}", Metrics.ApproxTriangleCountingBySamplingA(g));

            Console.WriteLine("Please press any key to exit");
            Console.ReadKey();
        }
    }
}
