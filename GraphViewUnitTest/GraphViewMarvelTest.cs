using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView;

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
            RawRecord rc = null;

            while (op.State())
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
            RawRecord rc = null;

            while (op.State())
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
            RawRecord rc = null;

            while (op.State())
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
            RawRecord rc = null;

            while (op.State())
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
            GraphTraversal g1 = new GraphTraversal(connection);
            var r1 = g1.V().As("character").has("weapon", GraphTraversal.within("shield", "claws")).Out("appeared").As("comicbook").select("character").path();

            foreach (var record in r1)
            {
                var path = record[0];
                Console.WriteLine(path);
            }
        }
        
        [TestMethod]
        public void SelectMarvelQueryNativeAPI2()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest1");
            GraphTraversal g1 = new GraphTraversal(connection);
            var r1 = g1.V().As("CharacterNode").values("character").As("character").@select("CharacterNode").has("weapon", GraphTraversal.without("shield", "claws")).Out("appeared").values("comicbook").As("comicbook").select("character", "comicbook");

            foreach (var record in r1)
            {
                var character = record["character"];
                var comicbook = record["comicbook"];
                Console.Write(character + " " + comicbook);
            }
        }
        
        [TestMethod]
        public void SelectMarvelQueryNativeAPI3()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest1");
            GraphTraversal g1 = new GraphTraversal(connection);
            var r1 = g1.V().has("comicbook", "AVF 4").In("appeared").values("character").order();

            foreach (var record in r1)
            {
                var N_1_character = record["N_1.character"];
                Console.WriteLine(N_1_character);
            }
        }
        
        [TestMethod]
        public void SelectMarvelQueryNativeAPI4()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest1");
            GraphTraversal g1 = new GraphTraversal(connection);
            var r1 = g1.V().has("comicbook", "AVF 4").In("appeared").has("weapon", GraphTraversal.within("shield", "claws")).values("character").order().@by(GraphTraversal.decr());

            foreach (var record in r1)
            {
                var N_1_character = record[0];
                Console.WriteLine(N_1_character);
            }
        }
        
    }

    [TestClass]
    public class GraphViewMarvelInsertDeleteTest
    {
        //[TestMethod]
        //public void AddSimpleEdgeMarvelAllRecords()
        //{
        //    GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
        //        "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
        //        "GroupMatch", "MarvelTest");
        //    ResetCollection("MarvelTest");
        //    GraphViewGremlinParser parser = new GraphViewGremlinParser();
        //    parser.Parse("g.addV('character','VENUS II','weapon','shield')").Generate(connection).Next();
        //    parser.Parse("g.addV('comicbook','AVF 4')").Generate(connection).Next();
        //    parser.Parse("g.V.as('v').has('character','VENUS II').as('a').select('v').has('comicbook','AVF 4').as('b').select('a','b').addOutE('a','appeared','b')").Generate(connection).Next();
        //    parser.Parse("g.addV('character','HAWK','weapon','claws')").Generate(connection).Next();
        //    parser.Parse("g.addV('comicbook','AVF 4')").Generate(connection).Next();
        //    parser.Parse("g.V.as('v').has('character','HAWK').as('a').select('v').has('comicbook','AVF 4').as('b').select('a','b').addOutE('a','appeared','b')").Generate(connection).Next();
        //    parser.Parse("g.addV('character','WOODGOD','weapon','lasso')").Generate(connection).Next();
        //    parser.Parse("g.addV('comicbook','H2 252')").Generate(connection).Next();
        //    parser.Parse("g.V.as('v').has('character','WOODGOD').as('a').select('v').has('comicbook','H2 252').as('b').select('a','b').addOutE('a','appeared','b')").Generate(connection).Next();
        //}
        [TestMethod]
        public void AddSimpleEdgeMarvelNativeAPIAllRecords()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest1");
            connection.ResetCollection();
            GraphTraversal g = new GraphTraversal(connection);
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
