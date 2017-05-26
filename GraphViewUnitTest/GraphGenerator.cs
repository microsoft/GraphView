using GraphView;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace QuickStart
{
    class Program
    {
        static void print(string label, List<string> res)
        {
            System.Console.WriteLine(label);
            foreach (var x in res)
            {
                System.Console.WriteLine(x);
            }
            System.Console.WriteLine();
        }

        static int min(int a, int b) {
            if (a < b)
                return a;
            return b;
        }

        static void Main(string[] args)
        {
            // Azure DocumentDB configuration
            string DOCDB_URL = "https://localhost:8081/";
            string DOCDB_AUTHKEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
            string DOCDB_DATABASE = "NetworkS";
            string DOCDB_COLLECTION = "ontest";

            // create collection
            GraphViewConnection connection = new GraphViewConnection(DOCDB_URL, DOCDB_AUTHKEY, DOCDB_DATABASE, DOCDB_COLLECTION);
            connection.ResetCollection();
            GraphViewCommand graph = new GraphViewCommand(connection);

            // set n,d,o
            int n = 10;
            int d = 3;
            int o = 2;

            //begin to generate graph
            int[] deg = new int[n];
            bool[] tmp = new bool[n];
            deg[0] = d;

            //Adding the first d+1 nodes
            graph.g().AddV("node").Property("No.", 1).Next();
            for (int i = 1; i <= d; i++) {
                deg[i] = d;
                graph.g().AddV("node").Property("No.", i + 1).Next();
                graph.g().V().Where(GraphTraversal2.__().Values("No.").Is(Predicate.neq(i+1))).AddE("edge").To(graph.g().V().Has("No.",i+1)).Next();
            }
            Random rad = new Random();
            //Adding other nodes
            for (int i = d + 1; i < n; i++) {
                graph.g().AddV("node").Property("No.", i + 1).Next();
                //randomly pick d nodes
                deg[i] = d;
                int d1 = 0;
                for (int j = 0; j < i; j++) {
                    tmp[j] = false;
                }
                while (d1 < d) {
                    int sum = 0;
                    for (int x = 0; x < i; x++) {
                        if (!tmp[x])
                            sum += deg[x];
                    }
                    int t = rad.Next(1, sum);
                    int k = 0;
                    sum = 0;
                    while (sum < t) {
                        if (!tmp[k])
                            sum += deg[k];
                        k++;
                    }
                    tmp[k - 1] = true;
                    d1++;
                }
                //add edges to these nodes
                for (int j = 1; j < i; j++) {
                    if (tmp[j])
                    {
                        graph.g().V().Has("No.", i + 1).AddE("edge").To(graph.g().V().Has("No.", j + 1)).Next();
                        deg[j]++;
                    }
                }
                //look at o pairs of wedges
                for (int x = 0; x < o; x++) {
                    int a1 = rad.Next(1, i);
                    int a2 = rad.Next(1, i);
                    while (a2 == a1)
                        a2 = rad.Next(1, i);
                    var t = graph.g().V().Has("No.", a1).Both("edge").Has("No.", a2).Values("No.").Next();
                    if (t.Count()==0)
                        graph.g().V().Has("No.", a1).AddE("edge").To(graph.g().V().Has("No.", a2)).Next();
                }

            }



            System.Console.WriteLine("Finished");
            System.Console.ReadKey();
        }
    }
}
