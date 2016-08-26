using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView;
using Microsoft.SqlServer.TransactSql.ScriptDom;

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
        [TestMethod]
        public void SelectMarvelQueryNativeAPI1()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GremlinPipeline g1 = new GremlinPipeline(ref connection);
            var r1 = g1.V().As("character").has("weapon", GremlinPipeline.within("shield", "claws")).Out("appeared").As("comicbook").select("character", "comicbook");

            foreach (var x in r1)
            {
                var y = x;
            }
        }
        [TestMethod]
        public void SelectMarvelQueryNativeAPI2()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GremlinPipeline g1 = new GremlinPipeline(ref connection);
            var r1 = g1.V().As("character").has("weapon", GremlinPipeline.without("shield", "claws")).Out("appeared").As("comicbook").select("character", "comicbook");

            foreach (var x in r1)
            {
                var y = x;
            }
        }
        [TestMethod]
        public void SelectMarvelQueryNativeAPI3()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GremlinPipeline g1 = new GremlinPipeline(ref connection);
            var r1 = g1.V().has("comicbook", "AVF 4").In("appeared").values("character").order();

            foreach (var x in r1)
            {
                var y = x;
            }
        }
        [TestMethod]
        public void SelectMarvelQueryNativeAPI4()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GremlinPipeline g1 = new GremlinPipeline(ref connection);
            var r1 = g1.V().has("comicbook", "AVF 4").In("appeared").has("weapon", GremlinPipeline.without("shield", "claws")).values("character").order();

            foreach (var x in r1)
            {
                var y = x;
            }
        }

    }

    [TestClass]
    public class GraphViewMarvelInsertDeleteTest
    {
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
        [TestMethod]
        public void AddSimpleEdgeMarvelAllRecords()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            ResetCollection("MarvelTest");
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            parser.Parse("g.addV('character','VENUS II','weapon','shield')").Generate(connection).Next();
            parser.Parse("g.addV('comicbook','AVF 4')").Generate(connection).Next();
            parser.Parse("g.V.as('v').has('character','VENUS II').as('a').select('v').has('comicbook','AVF 4').as('b').select('a','b').addOutE('a','appeared','b')").Generate(connection).Next();
            parser.Parse("g.addV('character','HAWK','weapon','claws')").Generate(connection).Next();
            parser.Parse("g.addV('comicbook','AVF 4')").Generate(connection).Next();
            parser.Parse("g.V.as('v').has('character','HAWK').as('a').select('v').has('comicbook','AVF 4').as('b').select('a','b').addOutE('a','appeared','b')").Generate(connection).Next();
            parser.Parse("g.addV('character','WOODGOD','weapon','lasso')").Generate(connection).Next();
            parser.Parse("g.addV('comicbook','H2 252')").Generate(connection).Next();
            parser.Parse("g.V.as('v').has('character','WOODGOD').as('a').select('v').has('comicbook','H2 252').as('b').select('a','b').addOutE('a','appeared','b')").Generate(connection).Next();
        }
        [TestMethod]
        public void AddSimpleEdgeMarvelNativeAPIAllRecords()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            ResetCollection("MarvelTest");
            GremlinPipeline g = new GremlinPipeline(ref connection);
            var r1 = g.V().addV("character", "VENUS II", "weapon", "shield");
            var r2 = g.V().addV("comicbook", "AVF 4");
            var r3 = g.V().As("v").has("character", "VENUS II").As("a").select("v").has("comicbook", "AVF 4").As("b").select("a", "b").addOutE("a", "appeared", "b");
            var r4 = g.V().addV("character", "HAWK", "weapon", "claws");
            var r5 = g.V().addV("comicbook", "AVF 4");
            var r6 = g.V().As("v").has("character", "HAWK").As("a").select("v").has("comicbook", "AVF 4").As("b").select("a", "b").addOutE("a", "appeared", "b");
            var r7 = g.V().addV("character", "WOODGOD", "weapon", "lasso");
            //var r8 = g.V().addV("comicbook", "H2 252");
            var r9 = g.V().As("v").has("character", "WOODGOD").As("a").select("v").has("comicbook", "AVF 4").As("b").select("a", "b").addOutE("a", "appeared", "b");
        }
        [TestMethod]
        public void AddSimpleEdgeMarvelRecord1()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            ResetCollection("MarvelTest");
            GraphViewGremlinParser parser1 = new GraphViewGremlinParser();
            var ParserTree1 = parser1.Parse("g.addV('character','VENUS II','weapon','shiled')");
            var op1 = ParserTree1.Generate(connection);
            op1.Next();
            GraphViewGremlinParser parser2 = new GraphViewGremlinParser();
            var ParserTree2 = parser2.Parse("g.addV('comicbook','AVF 4')");
            var op2 = ParserTree2.Generate(connection);
            op2.Next();
            GraphViewGremlinParser parser3 = new GraphViewGremlinParser();
            var ParserTree3 = parser3.Parse("g.V.as('v').has('character','VENUS II').as('a').select('v').has('comicbook','AVF 4').as('b').select('a','b').addOutE('a','appeared','b')");
            var op3 = ParserTree3.Generate(connection);
            op3.Next();
            GraphViewGremlinParser parser4 = new GraphViewGremlinParser();
            var ParserTree4 = parser4.Parse("g.V.has('character','VENUS II').out('appeared').name");
            var op4 = ParserTree4.Generate(connection);
            Record rc = null;

            while (op4.Status())
            {
                rc = op4.Next();
            }
        }
        [TestMethod]
        public void AddSimpleEdgeMarvelRecord2()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            ResetCollection("MarvelTest");
            GraphViewGremlinParser parser1 = new GraphViewGremlinParser();
            var ParserTree1 = parser1.Parse("g.addV('character','VENUS II','weapon','claws')");
            var op1 = ParserTree1.Generate(connection);
            op1.Next();
            GraphViewGremlinParser parser2 = new GraphViewGremlinParser();
            var ParserTree2 = parser2.Parse("g.addV('comicbook','AVF 4')");
            var op2 = ParserTree2.Generate(connection);
            op2.Next();
            GraphViewGremlinParser parser3 = new GraphViewGremlinParser();
            var ParserTree3 = parser3.Parse("g.V.as('v').has('character','VENUS II').as('a').select('v').has('comicbook','AVF 4').as('b').select('a','b').addOutE('a','appeared','b')");
            var op3 = ParserTree3.Generate(connection);
            op3.Next();
            GraphViewGremlinParser parser4 = new GraphViewGremlinParser();
            var ParserTree4 = parser4.Parse("g.V.has('character','VENUS II').out('appeared').name");
            var op4 = ParserTree4.Generate(connection);
            Record rc = null;
            while (op4.Status())
            {
                rc = op4.Next();
            }
        }

        public void AddSimpleEdgeMarvelRecord3()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            ResetCollection("MarvelTest");
            GraphViewGremlinParser parser1 = new GraphViewGremlinParser();
            var ParserTree1 = parser1.Parse("g.addV('character','WOODGOD','weapon','lasso')");
            var op1 = ParserTree1.Generate(connection);
            op1.Next();
            GraphViewGremlinParser parser2 = new GraphViewGremlinParser();
            var ParserTree2 = parser2.Parse("g.addV('comicbook','H2 252')");
            var op2 = ParserTree2.Generate(connection);
            op2.Next();
            GraphViewGremlinParser parser3 = new GraphViewGremlinParser();
            var ParserTree3 = parser3.Parse("g.V.as('v').has('character','WOODGOD').as('a').select('v').has('comicbook','H2 252').as('b').select('a','b').addOutE('a','appeared','b')");
            var op3 = ParserTree3.Generate(connection);
            op3.Next();
            GraphViewGremlinParser parser4 = new GraphViewGremlinParser();
            var ParserTree4 = parser4.Parse("g.V.has('character','WOODGOD').out('appeared').name");
            var op4 = ParserTree4.Generate(connection);
            Record rc = null;

            while (op4.Status())
            {
                rc = op4.Next();
            }
        }
    }
}
