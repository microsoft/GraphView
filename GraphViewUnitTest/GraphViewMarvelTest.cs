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
            }
        }
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
            }
        }
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
            }
        }

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
                "GroupMatch", "MarvelTestEdge1");
            ResetCollection("MarvelTestEdge1");
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            parser.Parse("g.addV('character','VENUS II','weapon','shield')").Generate(connection).Next();
            parser.Parse("g.addV('comicbook','AVF 4')").Generate(connection).Next();
            parser.Parse("g.V.as('v').has('character','VENUS II').as('a').select('v').has('comicbook','AVF 4').as('b').select('a','b').addOutE('a','appeared','b')").Generate(connection).Next();
            parser.Parse("g.addV('character','HAWK','weapon','claws')").Generate(connection).Next();
            parser.Parse("g.V.as('v').has('character','HAWK').as('a').select('v').has('comicbook','AVF 4').as('b').select('a','b').addOutE('a','appeared','b')").Generate(connection).Next();
            parser.Parse("g.addV('character','WOODGOD','weapon','lasso')").Generate(connection).Next();
            parser.Parse("g.V.as('v').has('character','WOODGOD').as('a').select('v').has('comicbook','H2 252').as('b').select('a','b').addOutE('a','appeared','b')").Generate(connection).Next();
        }
        [TestMethod]
        public void AddSimpleEdgeMarvelNativeAPIAllRecords()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTestEdge1");
            ResetCollection("MarvelTestEdge1");
            GraphTraversal g = new GraphTraversal(ref connection);
            g.V().addV("character", "VENUS II", "weapon", "shield");
            g.V().addV("comicbook", "AVF 4");
            g.V().has("character", "VENUS II").addE("type","appeared").to(g.V().has("comicbook", "AVF 4"));
            g.V().addV("character", "HAWK", "weapon", "claws");
            g.V().As("v").has("character", "HAWK").addE("type", "appeared").to(g.V().has("comicbook", "AVF 4"));
            g.V().addV("character", "WOODGOD", "weapon", "lasso");
            g.V().As("v").has("character", "WOODGOD").addE("type", "appeared").to(g.V().has("comicbook", "AVF 4"));
        }
        //[TestMethod]
        //public void AddSimpleEdgeMarvelRecord1()
        //{
        //    GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
        //        "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
        //        "GroupMatch", "MarvelTest");
        //    ResetCollection("MarvelTest");
        //    GraphViewGremlinParser parser1 = new GraphViewGremlinParser();
        //    var ParserTree1 = parser1.Parse("g.addV('character','VENUS II','weapon','shiled')");
        //    var op1 = ParserTree1.Generate(connection);
        //    op1.Next();
        //    GraphViewGremlinParser parser2 = new GraphViewGremlinParser();
        //    var ParserTree2 = parser2.Parse("g.addV('comicbook','AVF 4')");
        //    var op2 = ParserTree2.Generate(connection);
        //    op2.Next();
        //    GraphViewGremlinParser parser3 = new GraphViewGremlinParser();
        //    var ParserTree3 = parser3.Parse("g.V.as('v').has('character','VENUS II').as('a').select('v').has('comicbook','AVF 4').as('b').select('a','b').addOutE('a','appeared','b')");
        //    var op3 = ParserTree3.Generate(connection);
        //    op3.Next();
        //    GraphViewGremlinParser parser4 = new GraphViewGremlinParser();
        //    var ParserTree4 = parser4.Parse("g.V.has('character','VENUS II').out('appeared').name");
        //    var op4 = ParserTree4.Generate(connection);
        //    Record rc = null;

        //    while (op4.Status())
        //    {
        //        rc = op4.Next();
        //    }
        //}
        //[TestMethod]
        //public void AddSimpleEdgeMarvelRecord2()
        //{
        //    GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
        //        "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
        //        "GroupMatch", "MarvelTest");
        //    ResetCollection("MarvelTest");
        //    GraphViewGremlinParser parser1 = new GraphViewGremlinParser();
        //    var ParserTree1 = parser1.Parse("g.addV('character','VENUS II','weapon','claws')");
        //    var op1 = ParserTree1.Generate(connection);
        //    op1.Next();
        //    GraphViewGremlinParser parser2 = new GraphViewGremlinParser();
        //    var ParserTree2 = parser2.Parse("g.addV('comicbook','AVF 4')");
        //    var op2 = ParserTree2.Generate(connection);
        //    op2.Next();
        //    GraphViewGremlinParser parser3 = new GraphViewGremlinParser();
        //    var ParserTree3 = parser3.Parse("g.V.as('v').has('character','VENUS II').as('a').select('v').has('comicbook','AVF 4').as('b').select('a','b').addOutE('a','appeared','b')");
        //    var op3 = ParserTree3.Generate(connection);
        //    op3.Next();
        //    GraphViewGremlinParser parser4 = new GraphViewGremlinParser();
        //    var ParserTree4 = parser4.Parse("g.V.has('character','VENUS II').out('appeared').name");
        //    var op4 = ParserTree4.Generate(connection);
        //    Record rc = null;
        //    while (op4.Status())
        //    {
        //        rc = op4.Next();
        //    }
        //}

        //public void AddSimpleEdgeMarvelRecord3()
        //{
        //    GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
        //        "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
        //        "GroupMatch", "MarvelTest");
        //    ResetCollection("MarvelTest");
        //    GraphViewGremlinParser parser1 = new GraphViewGremlinParser();
        //    var ParserTree1 = parser1.Parse("g.addV('character','WOODGOD','weapon','lasso')");
        //    var op1 = ParserTree1.Generate(connection);
        //    op1.Next();
        //    GraphViewGremlinParser parser2 = new GraphViewGremlinParser();
        //    var ParserTree2 = parser2.Parse("g.addV('comicbook','H2 252')");
        //    var op2 = ParserTree2.Generate(connection);
        //    op2.Next();
        //    GraphViewGremlinParser parser3 = new GraphViewGremlinParser();
        //    var ParserTree3 = parser3.Parse("g.V.as('v').has('character','WOODGOD').as('a').select('v').has('comicbook','H2 252').as('b').select('a','b').addOutE('a','appeared','b')");
        //    var op3 = ParserTree3.Generate(connection);
        //    op3.Next();
        //    GraphViewGremlinParser parser4 = new GraphViewGremlinParser();
        //    var ParserTree4 = parser4.Parse("g.V.has('character','WOODGOD').out('appeared').name");
        //    var op4 = ParserTree4.Generate(connection);
        //    Record rc = null;

        //    while (op4.Status())
        //    {
        //        rc = op4.Next();
        //    }
        //}
    }
}
