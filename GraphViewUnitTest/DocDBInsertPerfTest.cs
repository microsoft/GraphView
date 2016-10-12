using System;
using System.Collections.Generic;
using System.Linq;
using System.Diagnostics;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView;
using GraphView;

namespace GraphViewUnitTest
{
    [TestClass]
    public class DocDBInsertPerfTest
    {

        public class Info
        {
            internal string id;
            internal string type;
            internal Dictionary<string, string> properties;
        }

        public class NodeInfo : Info
        {
            internal List<EdgeInfo> edges;
        }

        public enum direction
        {
            In,
            Out
        }
        public class EdgeInfo : Info
        {
            internal string name;
            internal direction dir;
            internal NodeInfo target;
        }

        [TestMethod]
        public void insert1()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
"GroupMatch", "MarvelTest");
            ResetCollection("MarvelTest");
            var expTimes = 50;
            var sumTime = 0.0;
            var result = new List<Double>();

            for (int i = 0; i < expTimes; i++)
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();
                // Note: update as the random number
                GraphViewCommand gcmd = new GraphViewCommand();
                gcmd.GraphViewConnection = connection;

                connection.SetupClient();

                gcmd.CommandText = @"
                INSERT INTO Node (name, age, type) VALUES ('saturn', 10000" + i + ", 'titan');";
                gcmd.ExecuteNonQuery();

                sw.Stop();
                sumTime += sw.Elapsed.TotalMilliseconds;
                result.Add(sw.Elapsed.TotalMilliseconds);
                //Console.WriteLine("query{0} time is:{1}", i, sw.Elapsed.TotalMilliseconds);
                Console.WriteLine("{0} time is:{1}", i, sw.Elapsed.TotalMilliseconds);
            }

            Console.WriteLine("max insert time is: {0}", result.Max());
            Console.WriteLine("min insert time is: {0}", result.Min());
            Console.WriteLine("avg insert time is: {0}", result.Average());
            Console.WriteLine("stdDev insert time is: {0}", stdDev(result));
            Console.WriteLine("avg,max,min,stdDev");
            Console.WriteLine("{0}, {1}, {2}, {3}", result.Average(), result.Max(), result.Min(), stdDev(result));
        }

        public static GraphTraversal _find(GraphViewConnection connection, Info info, GraphTraversal source = null)
        {
            GraphTraversal t;
            if (source != null) t = source;
            else
            {
                t = new GraphTraversal(connection);
                t = t.V();
            }
            if (info.id != null)
                t = t.has("id", info.id);
            if (info.type != null)
                t = t.has("type", info.type);
            if (info.properties != null && info.id == null)
                t = info.properties.Aggregate(t, (current, prop) => current.has(prop.Key, prop.Value));
            return t;
        }

        public double stdDev(List<double> values)
        {
            double ret = 0;
            if (values.Count() > 0)
            {
                //Compute the Average      
                double avg = values.Average();
                //Perform the Sum of (value-avg)_2_2      
                double sum = values.Sum(d => Math.Pow(d - avg, 2));
                //Put it all together      
                ret = Math.Sqrt((sum) / (values.Count() - 1));
            }
            return ret;
        }

        public static void ResetCollection(string collection)
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", collection);
            connection.SetupClient();
            connection.DocDB_finish = false;
            connection.BuildUp();

            while (!connection.DocDB_finish)
                System.Threading.Thread.Sleep(10);

            connection.ResetCollection();
            connection.DocDB_finish = false;
            connection.BuildUp();

            while (!connection.DocDB_finish)
                System.Threading.Thread.Sleep(10);
        }
    }
}
