using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphViewUnitTest
{
    [TestClass]
    public class ShortestPathTest
    {
        [TestMethod]
        public void GraphViewCreateMarvelDataSet()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            connection.ResetCollection();
            GraphViewCommand cmd = new GraphViewCommand(connection);

            cmd.CommandText = "g.addV('character').property('name', 'VENUS II').property('weapon', 'shield').next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('comicbook').property('name', 'AVF 4').next()";
            cmd.Execute();
            cmd.CommandText = "g.V().has('name', 'VENUS II').addE('appeared').to(g.V().has('name', 'AVF 4')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('character').property('name', 'HAWK').property('weapon', 'claws').next()";
            cmd.Execute();
            cmd.CommandText = "g.V().as('v').has('name', 'HAWK').addE('appeared').to(g.V().has('name', 'AVF 4')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('character').property('name', 'WOODGOD').property('weapon', 'lasso').next()";
            cmd.Execute();
            cmd.CommandText = "g.V().as('v').has('name', 'WOODGOD').addE('appeared').to(g.V().has('name', 'AVF 4')).next()";
            cmd.Execute();
        }
        [TestMethod]
        public void ShortestPath()
        {
            String src = "3b93929f-6ec7-4af1-b62c-89f0c46cc1da";
            String des = "7974ea88-e418-4803-9b75-6ad6d83f775e";
            GetShortestPath(src, des);
        }
        [TestMethod]
        public void GetShortestPath(String src, String des)
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
              "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
              "GroupMatch", "MarvelTest");
            GraphViewCommand graph = new GraphViewCommand(connection);
            graph.OutputFormat = OutputFormat.GraphSON;
            Queue<String> vertexIdQ1 = new Queue<String>();
            Queue<String> vertexIdQ2 = new Queue<String>();
            Boolean reachDes = false;
            int depth = 0;
            vertexIdQ1.Enqueue(src);

            while(!reachDes && vertexIdQ1.Count != 0)
            {
                var id = vertexIdQ1.Dequeue();
                var tempVertexIds = graph.g().V().Has("id", id).Values("id").Next();
                foreach(var vertexId in tempVertexIds)
                {
                    if(vertexId == des)
                    {
                        reachDes = true;
                        depth ++;
                        break;
                    } else
                    {
                        vertexIdQ2.Enqueue(vertexId);
                    }
                }
                // the uppper level queue become empty, move to next level of graph
                if(vertexIdQ1.Count == 0)
                {
                    var swap = vertexIdQ1;
                    vertexIdQ1 = vertexIdQ2;
                    vertexIdQ2 = swap;
                    depth ++;
                }
            }

            if (reachDes)
            {
                Console.WriteLine("Shortest Path from {0} to {1} is {2}", src, des, depth);
            } else
            {
                Console.WriteLine("No path from {0} to {1} is {2}", src, des, depth);
            }
        }
    }
}
