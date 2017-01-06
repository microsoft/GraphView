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

            var results = GraphTraversal2.g().V().Has("weapon", "shield").As("character").Out("appeared").As("comicbook").Select("character").next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
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
                    .Has("weapon", "lasso")
                    .As("character")
                    .Out("appeared")
                    .As("comicbook")
                    .Select("comicbook")
                    .next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void SelectMarvelQuery3()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GraphTraversal2.SetGraphViewConnection(connection);

            var results = GraphTraversal2.g().V().Has("comicbook", "AVF 4").In("appeared").Values("character").next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void SelectMarvelQuery4()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GraphTraversal2.SetGraphViewConnection(connection);

            var results = GraphTraversal2.g().V().Has("comicbook", "AVF 4").In("appeared").Has("weapon", "shield").Values("character").next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
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
                    .Has("weapon", Predicate.within("shield", "claws"))
                    .Out("appeared")
                    .As("comicbook")
                    .Select("character")
                    .next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
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
                    .Values("character")
                    .As("character")
                    .Select("CharacterNode")
                    .Has("weapon", Predicate.without("shield", "claws"))
                    .Out("appeared")
                    .Values("comicbook")
                    .As("comicbook")
                    .Select("comicbook")
                    .next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
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
            GraphTraversal2.g().V().AddV().Property("character", "VENUS II").Property("weapon", "shield").next();
            GraphTraversal2.g().V().AddV().Property("comicbook", "AVF 4").next();
            GraphTraversal2.g().V().Has("character", "VENUS II").AddE("appeared").To(GraphTraversal2.g().V().Has("comicbook", "AVF 4")).next();
            GraphTraversal2.g().V().AddV().Property("character", "HAWK").Property("weapon", "claws").next();
            GraphTraversal2.g().V().As("v").Has("character", "HAWK").AddE("appeared").To(GraphTraversal2.g().V().Has("comicbook", "AVF 4")).next();
            GraphTraversal2.g().V().AddV().Property("character", "WOODGOD").Property("weapon", "lasso").next();
            GraphTraversal2.g().V().As("v").Has("character", "WOODGOD").AddE("appeared").To(GraphTraversal2.g().V().Has("comicbook", "AVF 4")).next();
        }
    }
}
