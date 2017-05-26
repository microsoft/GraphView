using GraphView;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Metrics
{
    static class GraphOperation
    {
        public static void AddV(GraphViewCommand g, string v)
        {
            g.CommandText = "g.addV('id', '" + v + "').next()";
            g.Execute();
        }

        public static void AddE(GraphViewCommand g, string v, string u)
        {
            g.CommandText = "g.V('" + v + "').addE('w').to(g.V('" + u + "')).next()";
            g.Execute();
        }

        public static GraphViewConnection BuildSimpleGraph(GraphViewConnection connection)
        {
            connection = GraphViewConnection.ResetGraphAPICollection(
                connection.DocDBUrl, connection.DocDBPrimaryKey, connection.DocDBDatabaseId, connection.DocDBCollectionId,
                connection.UseReverseEdges, connection.EdgeSpillThreshold
            );
            GraphViewCommand g = new GraphViewCommand(connection);

            g.g().AddV().Property("name", "node1").Next();
            g.g().AddV().Property("name", "node2").Next();
            g.g().AddV().Property("name", "node3").Next();
            g.g().AddV().Property("name", "node4").Next();
            g.g().AddV().Property("name", "node5").Next();

            g.g().V().Has("name", "node1").AddE("e").To(g.g().V().Has("name", "node2")).Next();
            g.g().V().Has("name", "node2").AddE("e").To(g.g().V().Has("name", "node3")).Next();
            g.g().V().Has("name", "node1").AddE("e").To(g.g().V().Has("name", "node3")).Next();
            g.g().V().Has("name", "node2").AddE("e").To(g.g().V().Has("name", "node4")).Next();
            g.g().V().Has("name", "node3").AddE("e").To(g.g().V().Has("name", "node4")).Next();
            g.g().V().Has("name", "node4").AddE("e").To(g.g().V().Has("name", "node5")).Next();
            g.g().V().Has("name", "node2").AddE("e").To(g.g().V().Has("name", "node5")).Next();

            g.Dispose();

            return connection;
        }

        public static GraphViewConnection ReadGraphFromEdgesFile(GraphViewConnection connection, string filename, int skip_lines = 0)
        {
            connection = GraphViewConnection.ResetGraphAPICollection(
                connection.DocDBUrl, connection.DocDBPrimaryKey, connection.DocDBDatabaseId, connection.DocDBCollectionId,
                connection.UseReverseEdges, connection.EdgeSpillThreshold
            );
            GraphViewCommand g = new GraphViewCommand(connection);

            using (var reader = new StreamReader(filename))
            {
                string[] sp;

                for (int i = 0; i < skip_lines; i++)
                    reader.ReadLine();

                HashSet<string> idset = new HashSet<string>();

                int nE = 0;

                while (true)
                {
                    string line = reader.ReadLine();
                    if (line == null)
                        break;

                    sp = line.Split();

                    string v = sp[0], u = sp[1];

                    if (!idset.Contains(v))
                    {
                        AddV(g, v);
                        idset.Add(v);
                    }
                    if (!idset.Contains(u))
                    {
                        AddV(g, u);
                        idset.Add(u);
                    }

                    AddE(g, v, u);

                    nE++;

                    if (nE % 100 == 0)
                        Console.WriteLine("{0} edges added", nE);
                }
            }
            
            g.Dispose();

            return connection;
        }

        public static LocalGraph EdgeSample(GraphViewCommand g, double p)
        {
            LocalGraph h = new LocalGraph();

            Console.WriteLine("start sampling");

            g.OutputFormat = OutputFormat.GraphSON;
            var edges = JsonConvert.DeserializeObject<JArray>(g.g().E().Coin(p).FirstOrDefault());
            foreach (var e in edges)
            {
                string v = (string)e["inV"], u = (string)e["outV"];
                h.AddE(v, u);
            }
            g.OutputFormat = OutputFormat.Regular;

            Console.WriteLine("end sampling");

            return h;
        }
    }
}
