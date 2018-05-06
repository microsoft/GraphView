using System;
using System.IO;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;

namespace GraphViewBenchmark
{
    internal class LoadData
    {
        public static bool USE_REVERSE_EDGE = true;
        public static string PARTITION_BY_KEY = "partition";
        public static int SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI = 1;

        public static int PARTITION_NUM = 100;

        public static string CalculatePartition(int key)
        {
            return (key % PARTITION_NUM).ToString();
        }

        /// <summary>
        /// Load graph from file.
        /// Graph data set format:
        ///  First line: number of vertices
        ///  Second line: number of edges
        ///  Following lines: with one edge per line. The meaning of the columns are: 
        ///                   First column: ID of from node
        ///                   Second column: ID of to node
        /// </summary>
        public static void LoadTwitterListsGraph()
        {
            string[] lines = File.ReadAllLines(@"..\..\GraphDataSet\twitter-lists.txt");

            Debug.Assert(lines.Length >= 2);
            int numVertex = int.Parse(lines[0]);
            int numEdge = int.Parse(lines[1]);

            List<Tuple<int, int>> edges = new List<Tuple<int, int>>();
            for (int i = 2; i < lines.Length; i++)
            {
                string[] ids = lines[i].Split(new[] { " ", "\t", "\r\n", "\r", "\n" }, StringSplitOptions.RemoveEmptyEntries);
                Debug.Assert(ids.Length == 2);
                edges.Add(new Tuple<int, int>(int.Parse(ids[0]), int.Parse(ids[1])));
            }
            Debug.Assert(numEdge == edges.Count);

            string docDBEndPoint = ConfigurationManager.AppSettings["DocDBEndPoint"];
            string docDBKey = ConfigurationManager.AppSettings["DocDBKey"];
            string docDBDatabaseId = ConfigurationManager.AppSettings["DocDBDatabaseId"];
            string docDBCollectionId = ConfigurationManager.AppSettings["DocDBCollectionId-TwitterLists"];

            //GraphViewConnection graphConnection = GraphViewConnection.ResetGraphAPICollection(
            //    docDBEndPoint, docDBKey, docDBDatabaseId, docDBCollectionId,
            //    USE_REVERSE_EDGE, SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, PARTITION_BY_KEY);
            GraphViewConnection graphConnection = new GraphViewConnection(
                docDBEndPoint, docDBKey, docDBDatabaseId, docDBCollectionId, GraphType.GraphAPIOnly,
                USE_REVERSE_EDGE, SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, PARTITION_BY_KEY);
            GraphViewCommand graphCommand = new GraphViewCommand(graphConnection);

            // Add vertex
            for (int id = 1; id <= numVertex; id++)
            {
                graphCommand.g().AddV("User").Property("id", id.ToString()).Property(PARTITION_BY_KEY, CalculatePartition(id)).Next();
                Console.WriteLine($"Add vertex {id} successfully");
            }

            // Add edge
            foreach (Tuple<int, int> tuple in edges)
            {
                string source = tuple.Item1.ToString();
                string sink = tuple.Item2.ToString();
                graphCommand.g().V().Has("id", source).AddE("Follow").To(graphCommand.g().V().Has("id", sink)).Next();
                Console.WriteLine($"Add Edge {source} -> {sink} successfully");
            }
        }
    }
}
