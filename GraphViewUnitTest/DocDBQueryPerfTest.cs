using System;
using System.Diagnostics;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView;

namespace GraphViewUnitTest
{
    [TestClass]
    public class DocDBQueryPerfTest
    {
        [TestMethod]
        public void query1()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
"GroupMatch", "IOTTest");
            var expTimes = 100;
            var sumTime = 0.0;

            for (int i = 0; i < expTimes; i++)
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();
                // Note: update as the random number
                string DeviceID = "25015";
                GraphTraversal g = new GraphTraversal(connection);
                var q = g.V()
                        .has("label", "DeviceModel")
                        .has("id", DeviceID)
                        .As("DeviceModel")
                        .@select("DeviceModel");
                
                foreach (var x in q)
                {

                }

                sw.Stop();
                sumTime += sw.Elapsed.TotalMilliseconds;
                Console.WriteLine("query{0} time is:{1}", i, sw.Elapsed.TotalMilliseconds);
            }

            var avgTime = sumTime / expTimes;
            Console.WriteLine("avg query time is: {0}", avgTime);
        }
        [TestMethod]
        public void query2()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
"GroupMatch", "IOTTest");
            var expTimes = 100;
            var sumTime = 0.0;

            for (int i = 0; i < expTimes; i++)
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();
                // Note: update as the random number
                string DeviceID = "25015";
                GraphTraversal g = new GraphTraversal(connection);
                var q = g.V()
                    .has("label", "DeviceModel")
                    .has("id", DeviceID)
                    .As("DeviceModel")
                    .Out("type_of")
                    .has("label", "DeviceTwin")
                    .As("device")
                    .@select("device", "DeviceModel");
                
                foreach (var x in q)
                {

                }

                sw.Stop();
                sumTime += sw.Elapsed.TotalMilliseconds;
                Console.WriteLine("query{0} time is:{1}", i, sw.Elapsed.TotalMilliseconds);
            }

            var avgTime = sumTime / expTimes;
            Console.WriteLine("avg query time is: {0}", avgTime);
        }
    }
}
