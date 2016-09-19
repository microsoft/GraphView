using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Newtonsoft.Json;
using System.Text;
using Newtonsoft.Json.Linq;
using System.IO;
namespace GraphViewUnitTest
{
    [TestClass]
    public class GraphViewMarvelSelectTest
    {
        [TestMethod]
        public void SelectMarvelQuery1()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            var op = parser.Parse("g.V().as('character').has('weapon', 'shield').out('appeared').as('comicbook').select('character', 'comicbook')").Generate(connection);
            Record rc = null;

            while (op.Status())
            {
                rc = op.Next();
            }
        }
        [TestMethod]
        public void SelectMarvelQuery2()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            var op = parser.Parse("g.V().as('character').has('weapon', 'lasso').out('appeared').as('comicbook').select('character', 'comicbook')").Generate(connection);
            Record rc = null;

            while (op.Status())
            {
                rc = op.Next();
            }
        }
        [TestMethod]
        public void SelectMarvelQuery3()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            var op = parser.Parse("g.V().has('comicbook', 'AVF 4').in('appeared').values('character')").Generate(connection);
            Record rc = null;

            while (op.Status())
            {
                rc = op.Next();
            }
        }
        [TestMethod]
        public void SelectMarvelQuery4()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            var op = parser.Parse("g.V().has('comicbook', 'AVF 4').in('appeared').has('weapon', 'shield').values('character')").Generate(connection);
            Record rc = null;

            while (op.Status())
            {
                rc = op.Next();
            }
        }
        /// <summary>
        /// Print the characters and the comic-books they appeared in where the characters had a weapon that was a shield or claws.
        /// </summary>
        [TestMethod]
        public void SelectMarvelQueryNativeAPI1()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest1");
            GraphTraversal g1 = new GraphTraversal(ref connection);
            var r1 = g1.V().As("character").has("weapon", GraphTraversal.within("shield", "claws")).Out("appeared").As("comicbook").select("character").path();

            foreach (var x in r1)
            {
                var y = x;
                if(y != null)
                {
                    Console.WriteLine(y.RetriveRow().ToString());
                }
            }
        }
        /// <summary>
        /// Print the characters and the comic-books they appeared in where the characters had a weapon that was not a shield or claws.
        /// </summary>
        [TestMethod]
        public void SelectMarvelQueryNativeAPI2()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest1");
            GraphTraversal g1 = new GraphTraversal(ref connection);
            var r1 = g1.V().As("character").has("weapon", GraphTraversal.without("shield", "claws")).Out("appeared").As("comicbook").select("character", "comicbook");

            foreach (var x in r1)
            {
                var y = x;
                if(y != null)
                {
                    Console.WriteLine(y.RetriveRow()); 
                }
            }
        }
        /// <summary>
        /// Print a sorted list of the characters that appear in comic-book AVF 4.
        /// </summary>
        [TestMethod]
        public void SelectMarvelQueryNativeAPI3()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest1");
            GraphTraversal g1 = new GraphTraversal(ref connection);
            var r1 = g1.V().has("comicbook", "AVF 4").In("appeared").values("character").order();

            foreach (var x in r1)
            {
                var y = x;
                if(y != null)
                {
                    Console.WriteLine(y.RetriveRow());
                }
            }
        }
        /// <summary>
        /// Print a sorted list of the characters that appear in comic-book AVF 4 that have a weapon that is not a shield or claws.
        /// </summary>
        [TestMethod]
        public void SelectMarvelQueryNativeAPI4()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest1");
            GraphTraversal g1 = new GraphTraversal(ref connection);
            var r1 = g1.V().has("comicbook", "AVF 4").In("appeared").has("weapon", GraphTraversal.without("shield", "claws")).values("character").order();

            foreach (var x in r1)
            {
                var y = x;
                if(y != null)
                {
                    Console.WriteLine(y.RetriveRow());
                }
            }
        }

        
        public void parseInEdge()
        {

        }

        public void parseOutEdge()
        {

        }

    }

    [TestClass]
    public class GraphViewMarvelInsertDeleteTest
    {
        /// <summary>
        /// Clear the collections
        /// </summary>
        /// <param name="collection">Collection name</param>
        [TestMethod]
        public void ResetCollection(string collection)
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
        /// <summary>
        /// Insert All Marvel Test Data
        /// </summary>
        [TestMethod]
        public void AddSimpleEdgeMarvelNativeAPIAllRecords()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest1");
            ResetCollection("MarvelTest1");
            GraphTraversal g = new GraphTraversal(ref connection);
            g.V().addV("character", "VENUS II", "weapon", "shield");
            g.V().addV("comicbook", "AVF 4");
            g.V().has("character", "VENUS II").addE("type","appeared").to(g.V().has("comicbook", "AVF 4"));
            g.V().addV("character", "HAWK", "weapon", "claws");
            g.V().As("v").has("character", "HAWK").addE("type", "appeared").to(g.V().has("comicbook", "AVF 4"));
            g.V().addV("character", "WOODGOD", "weapon", "lasso");
            g.V().As("v").has("character", "WOODGOD").addE("type", "appeared").to(g.V().has("comicbook", "AVF 4"));
        }
    }
}
