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
                "GroupMatch", "GremlinTest");
            connection.SetupClient();
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            var op = parser.Parse("g.V().as('character').has('weapon', 'shield').out('appeared').as('comicbook').select('character', 'comicbook')").Generate(connection);
            Record rc = null;

            while (op.Status())
            {
                rc = op.Next();
                Console.WriteLine(rc.RetriveData(4));
            }
        }
        [TestMethod]
        public void SelectMarvelQuery2()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "GremlinTest");
            connection.SetupClient();
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            var ParserTree = parser.Parse("");
            var op = parser.Parse("g.V().as('character').has('weapon', 'lasso').out('appeared').as('comicbook').select('character', 'comicbook')").Generate(connection);
            Record rc = null;

            while (op.Status())
            {
                rc = op.Next();
                //Console.WriteLine(rc.RetriveData(4));
            }
        }
        [TestMethod]
        public void SelectMarvelQuery3()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "GremlinTest");
            connection.SetupClient();
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            var op = parser.Parse("g.V().has('comicbook', 'AVF 4').in('appeared').values('character')").Generate(connection);
            Record rc = null;

            while (op.Status())
            {
                rc = op.Next();
                Console.WriteLine(rc.RetriveData(4));
            }
        }
        [TestMethod]
        public void SelectMarvelQuery4()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "GremlinTest");
            connection.SetupClient();
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            var op = parser.Parse("g.V().has('comicbook', 'AVF 4').in('appeared').has('weapon', 'shield').values('character')").Generate(connection);
            Record rc = null;

            while (op.Status())
            {
                rc = op.Next();
                Console.WriteLine(rc.RetriveData(4));
            }
        }
        [TestMethod]
        public void SelectMarvelQueryNativeAPI1()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "GremlinTest");
            connection.SetupClient();
            GremlinPipeline g1 = new GremlinPipeline();
            var r1 = g1.V().As("character").has("weapon", "shield").Out("appeared").As("comicbook").select("character", "comicbook");
            r1.connection = connection;

            foreach (var x in r1)
            {
                var y = x;
                Console.WriteLine(y.RetriveData(0));
            }
        }
        [TestMethod]
        public void SelectMarvelQueryNativeAPI2()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "GremlinTest");
            connection.SetupClient();
            GremlinPipeline g1 = new GremlinPipeline();
            var r1 = g1.V().As("character").has("weapon", "lasso").Out("appeared").As("comicbook").select("character", "comicbook");
            r1.connection = connection;

            foreach (var x in r1)
            {
                var y = x;
                Console.WriteLine(y.RetriveData(0));
            }
        }
        [TestMethod]
        public void SelectMarvelQueryNativeAPI3()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "GremlinTest");
            connection.SetupClient();
            GremlinPipeline g1 = new GremlinPipeline();
            var r1 = g1.V().As("character").has("comicbook", "AVF 4").In("appeared").values("character");
            r1.connection = connection;

            foreach (var x in r1)
            {
                var y = x;
                Console.WriteLine(y.RetriveData(0));
            }
        }
        [TestMethod]
        public void SelectMarvelQueryNativeAPI4()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "GremlinTest");
            connection.SetupClient();
            GremlinPipeline g1 = new GremlinPipeline();
            var r1 = g1.V().As("character").has("comicbook", "AVF 4").In("appeared").has("weapon", "shield").values("character");
            r1.connection = connection;

            foreach (var x in r1)
            {
                var y = x;
                Console.WriteLine(y.RetriveData(0));
            }
        }

    }

    [TestClass]
    public class GraphViewMarvelInsertDeleteTest
    {
        [TestMethod]
        public void AddSimpleNode()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "GremlinTest");
            connection.SetupClient();
            GraphViewGremlinParser parser1 = new GraphViewGremlinParser();
            var ParserTree1 = parser1.Parse("g.addV('label','person','name','stephen')");
            var op1 = ParserTree1.Generate(connection);
            op1.Next();
            GraphViewGremlinParser parser2 = new GraphViewGremlinParser();
            var ParserTree2 = parser2.Parse("g.V.has('label','person').name");
            var op2 = ParserTree2.Generate(connection);
            Record rc = null;

            while (op2.Status())
            {
                rc = op2.Next();
            }

            Assert.AreEqual(rc.RetriveData(2), "stephen");
        }
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
        public void AddSimpleEdge()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "GremlinTest");
            connection.SetupClient();
            GraphViewGremlinParser parser1 = new GraphViewGremlinParser();
            var ParserTree1 = parser1.Parse("g.addV('label','person','name','Adams')");
            var op1 = ParserTree1.Generate(connection);
            op1.Next();
            GraphViewGremlinParser parser2 = new GraphViewGremlinParser();
            var ParserTree2 = parser2.Parse("g.addV('label','person','name','Bob')");
            var op2 = ParserTree2.Generate(connection);
            op2.Next();
            GraphViewGremlinParser parser3 = new GraphViewGremlinParser();
            var ParserTree3 = parser3.Parse("g.V.as('v').has('name','Adams').as('a').select('v').has('name','Bob').as('b').select('a','b').addOutE('a','isfriend','b','for','10y')");
            var op3 = ParserTree3.Generate(connection);
            op3.Next();
            GraphViewGremlinParser parser4 = new GraphViewGremlinParser();
            var ParserTree4 = parser4.Parse("g.V.has('name','Adams').out('isfriend').name");
            var op4 = ParserTree4.Generate(connection);
            Record rc = null;

            while (op4.Status())
            {
                rc = op4.Next();
            }
        }
        [TestMethod]
        public void AddSimpleEdgeMarvelAllRecords()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "GremlinTest");
            connection.SetupClient();
            ResetCollection("GremlinTest");
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            parser.Parse("g.addV('character','VENUS II','weapon','shield')").Generate(connection).Next();
            parser.Parse("g.addV('comicbook','AVF 4')").Generate(connection).Next();
            parser.Parse("g.V.as('v').has('character','VENUS II').as('a').select('v').has('comic-book','AVF 4').as('b').select('a','b').addOutE('a','appeared','b')").Generate(connection).Next();
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
                "GroupMatch", "GremlinTest");
            connection.SetupClient();
            ResetCollection("GremlinTest");
            GremlinPipeline g = new GremlinPipeline();
            var r1 = g.V().addV("character", "VENUS II", "weapon", "shield");
            r1.connection = connection;
            var r2 = g.V().addV("comicbook", "AVF 4");
            r2.connection = connection;
            var r3 = g.V().As("v").has("character", "VENUS II").As("a").select("v").has("comicbook", "AVF 4").As("b").select("a", "b").addOutE("a", "appeared", "b");
            r3.connection = connection;
            var r4 = g.V().addV("character", "HAWK", "weapon", "claws");
            r4.connection = connection;
            var r5 = g.V().addV("comicbook", "AVF 4");
            r5.connection = connection;
            var r6 = g.V().As("v").has("character", "HAWK").As("a").select("v").has("comicbook", "AVF 4").As("b").select("a", "b").addOutE("a", "appeared", "b");
            r6.connection = connection;
            var r7 = g.V().addV("character", "WOODGOD", "weapon", "lasso");
            r7.connection = connection;
            var r8 = g.V().addV("comicbook", "H2 252");
            r8.connection = connection;
            var r9 = g.V().As("v").has("character", "WOODGOD").As("a").select("v").has("comicbook", "H2 252").As("b").select("a", "b").addOutE("a", "appeared", "b");
            r9.connection = connection;
        }
        [TestMethod]
        public void AddSimpleEdgeMarvelRecord1()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "GremlinTest");
            connection.SetupClient();
            GraphViewGremlinParser parser1 = new GraphViewGremlinParser();
            var ParserTree1 = parser1.Parse("g.addV('character','VENUS II','weapon','shiled')");
            var op1 = ParserTree1.Generate(connection);
            op1.Next();
            GraphViewGremlinParser parser2 = new GraphViewGremlinParser();
            var ParserTree2 = parser2.Parse("g.addV('comic-book','AVF 4')");
            var op2 = ParserTree2.Generate(connection);
            op2.Next();
            GraphViewGremlinParser parser3 = new GraphViewGremlinParser();
            var ParserTree3 = parser3.Parse("g.V.as('v').has('character','VENUS II').as('a').select('v').has('comic-book','AVF 4').as('b').select('a','b').addOutE('a','appeared','b')");
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
                "GroupMatch", "GremlinTest");
            connection.SetupClient();
            GraphViewGremlinParser parser1 = new GraphViewGremlinParser();
            var ParserTree1 = parser1.Parse("g.addV('character','VENUS II','weapon','claws')");
            var op1 = ParserTree1.Generate(connection);
            op1.Next();
            GraphViewGremlinParser parser2 = new GraphViewGremlinParser();
            var ParserTree2 = parser2.Parse("g.addV('comic-book','AVF 4')");
            var op2 = ParserTree2.Generate(connection);
            op2.Next();
            GraphViewGremlinParser parser3 = new GraphViewGremlinParser();
            var ParserTree3 = parser3.Parse("g.V.as('v').has('character','VENUS II').as('a').select('v').has('comic-book','AVF 4').as('b').select('a','b').addOutE('a','appeared','b')");
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
                "GroupMatch", "GremlinTest");
            connection.SetupClient();
            GraphViewGremlinParser parser1 = new GraphViewGremlinParser();
            var ParserTree1 = parser1.Parse("g.addV('character','WOODGOD','weapon','lasso')");
            var op1 = ParserTree1.Generate(connection);
            op1.Next();
            GraphViewGremlinParser parser2 = new GraphViewGremlinParser();
            var ParserTree2 = parser2.Parse("g.addV('comic-book','H2 252')");
            var op2 = ParserTree2.Generate(connection);
            op2.Next();
            GraphViewGremlinParser parser3 = new GraphViewGremlinParser();
            var ParserTree3 = parser3.Parse("g.V.as('v').has('character','WOODGOD').as('a').select('v').has('comic-book','H2 252').as('b').select('a','b').addOutE('a','appeared','b')");
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
