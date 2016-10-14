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
        public void insert0()
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
            Console.WriteLine("item count" + result.Count);
            Console.WriteLine("avg,max,min,stdDev");
            Console.WriteLine("{0}, {1}, {2}, {3}", result.Average(), result.Max(), result.Min(), stdDev(result));
        }

        [TestMethod]
        public void insert1()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
"GroupMatch", "MarvelTest");
            ResetCollection("MarvelTest");
            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;
            connection.SetupClient();
            var allExpTimes = 100;
            var result = new List<Double>();

            for (int k = 0; k < allExpTimes; k++)
            {
                var expTimes = 5;
                var sumTime = 0.0;

                // Insert node
                for (int i = 0; i < expTimes; i++)
                {
                    Stopwatch sw = new Stopwatch();
                    var tempSQL = @"
                    INSERT INTO Node (id,label,properties_name_id,properties_name_value) VALUES ('" + k + "1" + i + "','TelemetryDataModel','1','DataModel-906e71b0-59a2-11e6-8cd0-3717b83c0677');";
                    // Note: update as the random number

                    gcmd.CommandText = tempSQL;
                    sw.Start();
                    gcmd.ExecuteNonQuery();
                    sw.Stop();
                }

                var tempResult = new List<Double>();
                // Insert edge
                for (int i = 1; i < expTimes; i++)
                {
                    var tempSQL = @"
                INSERT INTO Edge (id,type)
                SELECT A, B, '" + i + @"','has'
                FROM   Node A, Node B
                WHERE  A.id = '" + k + "1" + i + "' AND B.id = '" + k + "10'";
                    Stopwatch sw = new Stopwatch();
                    // Note: update as the random number
                    gcmd.CommandText = tempSQL;
                    sw.Start();
                    gcmd.ExecuteNonQuery();
                    sw.Stop();
                    sumTime += sw.Elapsed.TotalMilliseconds;
                    tempResult.Add(sw.Elapsed.TotalMilliseconds);
                    Console.WriteLine("{0} time is:{1}", i, sw.Elapsed.TotalMilliseconds);
                }
                result.Add(tempResult.Sum());
                tempResult.Clear();
            }
            Console.WriteLine("max insert time is: {0}", result.Max());
            Console.WriteLine("min insert time is: {0}", result.Min());
            Console.WriteLine("avg insert time is: {0}", result.Average());
            Console.WriteLine("stdDev insert time is: {0}", stdDev(result));
            Console.WriteLine("item count" + result.Count);
            Console.WriteLine("avg,max,min,stdDev");
            Console.WriteLine("{0}, {1}, {2}, {3}", result.Average(), result.Max(), result.Min(), stdDev(result));
        }

        [TestMethod]
        public void insert2()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
"GroupMatch", "MarvelTest");
            ResetCollection("MarvelTest");
            var result = new List<Double>();
            var allExpTimes = 100;
            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;
            connection.SetupClient();

            for (int k = 0; k < allExpTimes; k++)
            {
                var expTimes = 5;
                var sumTime = 0.0;

                // Insert node
                for (int i = 0; i < expTimes; i++)
                {

                    // Note: update as the random number
                    var tempSQL1 = @"
                    INSERT INTO Node (id,label,properties_name_id,properties_name_value) VALUES ('" + k + "1" + i + "','TelemetryDataModel','1','DataModel-906e71b0-59a2-11e6-8cd0-3717b83c0677');";
                    gcmd.CommandText = tempSQL1;
                    gcmd.ExecuteNonQuery();
                }

                int j = 1;
                Stopwatch sw = new Stopwatch();
                var tempSQL = @"
                INSERT INTO Edge (id,type)
                SELECT A, B, '" + j + @"','has'
                FROM   Node A, Node B
                WHERE  A.id IN (" + k + "1, " + k + "2, " + k + "3, " + k + "4) AND B.id = '" + k + "10'";
                // Note: update as the random number
                gcmd.CommandText = tempSQL;
                sw.Start();
                gcmd.ExecuteNonQuery();
                sw.Stop();
                sumTime += sw.Elapsed.TotalMilliseconds;
                result.Add(sw.Elapsed.TotalMilliseconds);
                Console.WriteLine("{0} time is:{1}", j, sw.Elapsed.TotalMilliseconds);
            }

            Console.WriteLine("max insert time is: {0}", result.Max());
            Console.WriteLine("min insert time is: {0}", result.Min());
            Console.WriteLine("avg insert time is: {0}", result.Average());
            Console.WriteLine("stdDev insert time is: {0}", stdDev(result));
            Console.WriteLine("avg,max,min,stdDev");
            Console.WriteLine("item count" + result.Count);
            Console.WriteLine("{0}, {1}, {2}, {3}", result.Average(), result.Max(), result.Min(), stdDev(result));
        }
        [TestMethod]
        public void insert4()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
"GroupMatch", "MarvelTest");
            ResetCollection("MarvelTest");
            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;
            connection.SetupClient();
            var expTimes = 50;
            var sumTime = 0.0;
            var result = new List<Double>();

            for (int i = 0; i < expTimes; i++)
            {
                Stopwatch sw = new Stopwatch();
                var tempSQL = @"
                    INSERT INTO Node (id,label,properties_name_id,properties_name_value) VALUES ('1" + i + "','TelemetryDataModel','1','DataModel-906e71b0-59a2-11e6-8cd0-3717b83c0677');";
                // Note: update as the random number

                gcmd.CommandText = tempSQL;
                sw.Start();
                gcmd.ExecuteNonQuery();
                sw.Stop();
                sumTime += sw.Elapsed.TotalMilliseconds;
                result.Add(sw.Elapsed.TotalMilliseconds);
                Console.WriteLine("{0} time is:{1}", i, sw.Elapsed.TotalMilliseconds);
            }

            Console.WriteLine("max insert time is: {0}", result.Max());
            Console.WriteLine("min insert time is: {0}", result.Min());
            Console.WriteLine("avg insert time is: {0}", result.Average());
            Console.WriteLine("stdDev insert time is: {0}", stdDev(result));
            Console.WriteLine("item count" + result.Count);
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
