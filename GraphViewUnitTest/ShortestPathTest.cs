using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphViewUnitTest
{
    [TestClass]
    public class ShortestPathTest
    {
        [TestMethod]
        public void LoadClassicGraphData()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
              "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
              "GroupMatch", "MarvelTest");
            connection.ResetCollection();
            GraphViewCommand graphCommand = new GraphViewCommand(connection);

            graphCommand.g().AddV("person").Property("name", "marko").Property("age", 29).Next();
            graphCommand.g().AddV("person").Property("name", "vadas").Property("age", 27).Next();
            graphCommand.g().AddV("software").Property("name", "lop").Property("lang", "java").Next();
            graphCommand.g().AddV("person").Property("name", "josh").Property("age", 32).Next();
            graphCommand.g().AddV("software").Property("name", "ripple").Property("lang", "java").Next();
            graphCommand.g().AddV("person").Property("name", "peter").Property("age", 35).Next();
            graphCommand.g().V().Has("name", "marko").AddE("knows").Property("weight", 0.5d).To(graphCommand.g().V().Has("name", "vadas")).Next();
            graphCommand.g().V().Has("name", "marko").AddE("knows").Property("weight", 1.0d).To(graphCommand.g().V().Has("name", "josh")).Next();
            graphCommand.g().V().Has("name", "marko").AddE("created").Property("weight", 0.4d).To(graphCommand.g().V().Has("name", "lop")).Next();
            graphCommand.g().V().Has("name", "josh").AddE("created").Property("weight", 1.0d).To(graphCommand.g().V().Has("name", "ripple")).Next();
            graphCommand.g().V().Has("name", "josh").AddE("created").Property("weight", 0.4d).To(graphCommand.g().V().Has("name", "lop")).Next();
            graphCommand.g().V().Has("name", "peter").AddE("created").Property("weight", 0.2d).To(graphCommand.g().V().Has("name", "lop")).Next();

            graphCommand.Dispose();
            connection.Dispose();
        }

        [TestMethod]
        public void ShortestPath()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
              "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
              "GroupMatch", "MarvelTest");
            GraphViewCommand graph = new GraphViewCommand(connection);

            var src1 = graph.g().V().HasLabel("person").Has("name", Predicate.eq("marko")).Values("id").Next()[0];
            var des1 = graph.g().V().HasLabel("software").Has("name", Predicate.eq("lop")).Values("id").Next()[0];
            int result1 = GetShortestPath(src1, des1, graph);
            Assert.AreEqual(result1, 1);

            var src2 = graph.g().V().HasLabel("person").Has("name", Predicate.eq("marko")).Values("id").Next()[0];
            var des2 = graph.g().V().HasLabel("software").Has("name", Predicate.eq("ripple")).Values("id").Next()[0];
            var result2 = GetShortestPath(src2, des2, graph);
            Assert.AreEqual(result2, 2);

            var src3 = graph.g().V().HasLabel("person").Has("name", Predicate.eq("peter")).Values("id").Next()[0];
            var des3 = graph.g().V().HasLabel("software").Has("name", Predicate.eq("lop")).Values("id").Next()[0];
            var result3 = GetShortestPath(src3, des3, graph);
            Assert.AreEqual(result3, 1);
        }
        public int GetShortestPath(String src, String des, GraphViewCommand graph)
        {
            Queue<String> vertexIdQ1 = new Queue<String>();
            Queue<String> vertexIdQ2 = new Queue<String>();
            HashSet<String> historyVertex = new HashSet<string>();
            Boolean reachDes = false;
            int depth = 1;
            vertexIdQ1.Enqueue(src);

            while(!reachDes && vertexIdQ1.Count != 0)
            {
                var id = vertexIdQ1.Dequeue();
                var tempVertexIds = graph.g().V().HasId(id).Out().Values("id").Next();

                foreach (var vertexId in tempVertexIds)
                {
                    if(historyVertex.Contains(vertexId))
                    {
                        continue;
                    } else
                    {
                        historyVertex.Add(vertexId);
                    }
                    if(vertexId == des)
                    {
                        reachDes = true;
                        break;
                    } else if(vertexId != src)
                    {
                        vertexIdQ2.Enqueue(vertexId);
                    }
                }
                // the uppper level queue become empty, move to next level of graph
                if(vertexIdQ1.Count == 0 && !reachDes)
                {
                    var swap = vertexIdQ1;
                    vertexIdQ1 = vertexIdQ2;
                    vertexIdQ2 = swap;
                    depth ++;
                }
            }

            if (reachDes)
            {
                Console.WriteLine("Shortest Path from {0} to {1}, depth is {2}", src, des, depth);
            } else
            {
                Console.WriteLine("No path from {0} to {1}, depth is {2}", src, des, 0);
            }
            return depth;
        }
    }
}
