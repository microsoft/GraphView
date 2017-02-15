using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphViewUnitTest
{
    [TestClass]
    public class GraphViewMarvelTest
    {
        [TestMethod]
        public void SelectMarvelQuery1()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");

            GraphViewCommand graph = new GraphViewCommand(connection);
            graph.OutputFormat = OutputFormat.GraphSON;
            var results = graph.g().V().Has("weapon", "shield").As("character").Out("appeared").As("comicbook").Select("character").Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void SelectMarvelQuery1b()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");

            GraphViewCommand cmd = new GraphViewCommand(connection);
            cmd.CommandText =
                "g.V().has('weapon','shield').as('character').out('appeared').as('comicbook').select('character').next()";
            cmd.OutputFormat = OutputFormat.GraphSON;
            var results = cmd.Execute();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void SelectMarvelQuery1c()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");

            GraphViewCommand cmd = new GraphViewCommand(connection);
            cmd.CommandText =
                "g.V().has('weapon','shield').as('character').outE('appeared').next()";
            cmd.OutputFormat = OutputFormat.GraphSON;
            var results = cmd.Execute();

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
            GraphViewCommand graph = new GraphViewCommand(connection);

            var results =
                graph.g()
                    .V()
                    .Has("weapon", "lasso")
                    .As("character")
                    .Out("appeared")
                    .As("comicbook")
                    .Select("comicbook")
                    .Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void SelectMarvelQuery2b()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GraphViewCommand graph = new GraphViewCommand(connection);
            graph.CommandText = "g.V().has('weapon', 'lasso').as('character').out('appeared').as('comicbook').select('comicbook').next()";
            graph.OutputFormat = OutputFormat.GraphSON;
            var results = graph.Execute();

            foreach (string result in results)
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
            GraphViewCommand graph = new GraphViewCommand(connection);
            var results = graph.g().V().Has("name", "AVF 4").In("appeared").Values("name").Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void SelectMarvelQuery3b()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GraphViewCommand graph = new GraphViewCommand(connection);
            graph.CommandText = "g.V().has('name', 'AVF 4').in('appeared').values('name').next()";
            var results = graph.Execute();

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
            GraphViewCommand graph = new GraphViewCommand(connection);
            var results = graph.g().V().Has("name", "AVF 4").In("appeared").Has("weapon", "shield").Values("name").Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void SelectMarvelQuery4b()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GraphViewCommand graph = new GraphViewCommand(connection);
            graph.CommandText = "g.V().has('name', 'AVF 4').in('appeared').has('weapon', 'shield').values('name').next()";
            var results = graph.Execute();

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
            GraphViewCommand graph = new GraphViewCommand(connection);
            var results =
                graph.g().V()
                    .As("character")
                    .Has("weapon", Predicate.within("shield", "claws"))
                    .Out("appeared")
                    .As("comicbook")
                    .Select("character");

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
            GraphViewCommand graph = new GraphViewCommand(connection);
            var results =
                graph.g().V()
                    .As("CharacterNode")
                    .Values("name")
                    .As("character")
                    .Select("CharacterNode")
                    .Has("weapon", Predicate.without("shield", "claws"))
                    .Out("appeared")
                    .Values("name")
                    .As("comicbook")
                    .Select("comicbook")
                    .Next();

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
            connection.ResetCollection();
            GraphViewCommand graph = new GraphViewCommand(connection);

            graph.g().AddV("character").Property("name", "VENUS II").Property("weapon", "shield").Next();
            graph.g().AddV("comicbook").Property("name", "AVF 4").Next();
            graph.g().V().Has("name", "VENUS II").AddE("appeared").To(graph.g().V().Has("name", "AVF 4")).Next();
            graph.g().AddV("character").Property("name", "HAWK").Property("weapon", "claws").Next();
            graph.g().V().As("v").Has("name", "HAWK").AddE("appeared").To(graph.g().V().Has("name", "AVF 4")).Next();
            graph.g().AddV("character").Property("name", "WOODGOD").Property("weapon", "lasso").Next();
            graph.g().V().As("v").Has("name", "WOODGOD").AddE("appeared").To(graph.g().V().Has("name", "AVF 4")).Next();
        }

        [TestMethod]
        public void GraphViewMarvelInsertTest()
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
    }
}
