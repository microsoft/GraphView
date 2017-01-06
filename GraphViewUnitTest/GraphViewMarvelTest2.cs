using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphViewUnitTest
{
    [TestClass]
    public class GraphViewMarvelTest2
    {
        [TestMethod]
        public void SelectMarvelQuery1()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GraphTraversal2.SetGraphViewConnection(connection);

            var results = GraphTraversal2.g().V().has("weapon", "shield").As("character").Out("appeared").As("comicbook").select("character").next();

            foreach (var record in results)
            {
                foreach (var fieldValue in record.fieldValues)
                {
                    Console.Write(fieldValue + "  ");
                }
                Console.WriteLine();
            }
        }

        [TestMethod]
        public void SelectMarvelQuery2()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GraphTraversal2.SetGraphViewConnection(connection);

            var results =
                GraphTraversal2.g()
                    .V()
                    .has("weapon", "lasso")
                    .As("character")
                    .Out("appeared")
                    .As("comicbook")
                    .select("comicbook")
                    .next();

            foreach (var record in results)
            {
                foreach (var fieldValue in record.fieldValues)
                {
                    Console.Write(fieldValue + "  ");
                }
                Console.WriteLine();
            }
        }

        [TestMethod]
        public void SelectMarvelQuery3()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GraphTraversal2.SetGraphViewConnection(connection);

            var results = GraphTraversal2.g().V().has("comicbook", "AVF 4").In("appeared").values("character").next();

            foreach (var record in results)
            {
                foreach (var fieldValue in record.fieldValues)
                {
                    Console.Write(fieldValue + "  ");
                }
                Console.WriteLine();
            }
        }

        [TestMethod]
        public void SelectMarvelQuery4()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GraphTraversal2.SetGraphViewConnection(connection);

            var results = GraphTraversal2.g().V().has("comicbook", "AVF 4").In("appeared").has("weapon", "shield").values("character").next();

            foreach (var record in results)
            {
                foreach (var fieldValue in record.fieldValues)
                {
                    Console.Write(fieldValue + "  ");
                }
                Console.WriteLine();
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
                "GroupMatch", "MarvelTest");
            GraphTraversal2.SetGraphViewConnection(connection);

            var results =
                GraphTraversal2.g().V()
                    .As("character")
                    .has("weapon", Predicate.within("shield", "claws"))
                    .Out("appeared")
                    .As("comicbook")
                    .select("character")
                    .next();

            foreach (var record in results)
            {
                foreach (var fieldValue in record.fieldValues)
                {
                    Console.Write(fieldValue + "  ");
                }
                Console.WriteLine();
            }
        }

        [TestMethod]
        public void SelectMarvelQueryNativeAPI2()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GraphTraversal2.SetGraphViewConnection(connection);

            var results =
                GraphTraversal2.g().V()
                    .As("CharacterNode")
                    .values("character")
                    .As("character")
                    .@select("CharacterNode")
                    .has("weapon", Predicate.without("shield", "claws"))
                    .Out("appeared")
                    .values("comicbook")
                    .As("comicbook")
                    .select("comicbook")
                    .next();

            foreach (var record in results)
            {
                foreach (var fieldValue in record.fieldValues)
                {
                    Console.Write(fieldValue + "  ");
                }
                Console.WriteLine();
            }
        }

        [TestMethod]
        public void GraphViewMarvelInsertDeleteTest()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            //connection.ResetCollection();
            GraphTraversal2.SetGraphViewConnection(connection);
            GraphTraversal2.g().V().addV().property("character", "VENUS II").property("weapon", "shield").next();
            GraphTraversal2.g().V().addV().property("comicbook", "AVF 4").next();
            GraphTraversal2.g().V().has("character", "VENUS II").addE("appeared").to(GraphTraversal2.g().V().has("comicbook", "AVF 4")).next();
            GraphTraversal2.g().V().addV().property("character", "HAWK").property("weapon", "claws").next();
            GraphTraversal2.g().V().As("v").has("character", "HAWK").addE("appeared").to(GraphTraversal2.g().V().has("comicbook", "AVF 4")).next();
            GraphTraversal2.g().V().addV().property("character", "WOODGOD").property("weapon", "lasso").next();
            GraphTraversal2.g().V().As("v").has("character", "WOODGOD").addE("appeared").to(GraphTraversal2.g().V().has("comicbook", "AVF 4")).next();
        }
    }
}
