using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphViewUnitTest
{
    [TestClass]
    public class GraphViewGremlinParserTest
    {
        [TestMethod]
        public void GremlinParsingTest()
        {
            string ErrorKey = "";
            var para = GraphViewGremlinParser.LexicalAnalyzer.Tokenize(@"g.V.has('name','hercules').out('father').out('father').name", ref ErrorKey);
            GraphViewGremlinParser parser = new GraphViewGremlinParser(para.Item1, para.Item2);
            WSyntaxTree ParserTree = parser.ParseTree();
        }

        [TestMethod]
        public void SematicAnalyseTest()
        {
            string ErrorKey = "";
            var para = GraphViewGremlinParser.LexicalAnalyzer.Tokenize(@"g.V.has('name','hercules').out('father').out('father').name", ref ErrorKey);
            GraphViewGremlinParser parser = new GraphViewGremlinParser(para.Item1, para.Item2);
            var ParserTree = parser.ParseTree();
            var SematicAnalyser = new GraphViewGremlinSematicAnalyser(ParserTree, para.Item2);
            SematicAnalyser.Analyse();
        }
        [TestMethod]
        public void TransformToSqlTreeTest()
        {
            string ErrorKey = "";
            var para = GraphViewGremlinParser.LexicalAnalyzer.Tokenize(@"g.V.has('name','hercules').outE('battled').has('time', gt(1)).inV().values('name')", ref ErrorKey);
            GraphViewGremlinParser parser = new GraphViewGremlinParser(para.Item1, para.Item2);
            var ParserTree = parser.ParseTree();
            var SematicAnalyser = new GraphViewGremlinSematicAnalyser(ParserTree, para.Item2);
            SematicAnalyser.Analyse();
            SematicAnalyser.Transform(SematicAnalyser.SematicContext);
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
        "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
        "GroupMatch", "GraphTest");
            connection.SetupClient();
            var insertNode = SematicAnalyser.SqlTree as WInsertEdgeSpecification;
            var op = insertNode.Generate(connection);
            op.Next();
        }
    }

    [TestClass]
    public class GraphViewGremlinSelectTest
    {
        [TestMethod]
        public void SelectSimpleNode()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
"GroupMatch", "GremlinTest");
            connection.SetupClient();
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            var ParserTree = parser.Parse("g.V().has('name','pluto').as('pluto').place_x.as('x').select('pluto').out().place_x.where(neq('x'))");
            var op = ParserTree.Generate(connection);
            Record rc = null;
            while (op.Status())
            {
                rc = op.Next();
            }
        }
        [TestMethod]
        public void SelectSimpleEdge()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
"GroupMatch", "GremlinTest");
            connection.SetupClient();
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            var ParserTree = parser.Parse("g.E().has('place_x', lt(38))._ID");
            var op = ParserTree.Generate(connection);
            Record rc = null;
            while (op.Status())
            {
                rc = op.Next();
            }
        }
        [TestMethod]
        public void SelectLinearPattern()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
"GroupMatch", "GremlinTest");
            connection.SetupClient();
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            var ParserTree = parser.Parse("g.V().has('name', 'saturn').in('father').in('father').name");
            var op = ParserTree.Generate(connection);
            Record rc = null;
            while (op.Status())
            {
                rc = op.Next();
            }
        }
        [TestMethod]
        public void SelectBranchPattern()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
"GroupMatch", "GremlinTest");
            connection.SetupClient();
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            var ParserTree = parser.Parse("g.V().has('name', 'saturn').in('father').in('father').out('father','mother').values('name')");
            var op = ParserTree.Generate(connection);
            Record rc = null;
            while (op.Status())
            {
                rc = op.Next();
            }
        }
        [TestMethod]
        public void SelectThroughBothVandE()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
"GroupMatch", "GremlinTest");
            connection.SetupClient();
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            var ParserTree = parser.Parse("g.V().has('name', 'saturn').in('father').in('father').outE('battled').has('time', gt(1)).inV().values('name')");
            var op = ParserTree.Generate(connection);
            Record rc = null;
            while (op.Status())
            {
                rc = op.Next();
            }
        }
        [TestMethod]
        public void SelectWithAs()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
"GroupMatch", "GremlinTest");
            connection.SetupClient();
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            var ParserTree = parser.Parse("g.V().has('name', 'pluto').g.V(pluto).out('brother').as('god').out('lives').as('place').select().name");
            var op = ParserTree.Generate(connection);
            Record rc = null;
            while (op.Status())
            {
                rc = op.Next();
            }
        }
        [TestMethod]
        public void SelectWithMatch()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
"GroupMatch", "GremlinTest");
            connection.SetupClient();
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            var ParserTree = parser.Parse("g.V().match(__.as('a').out('father').as('b')).select('a','b').name");
            var op = ParserTree.Generate(connection);
            Record rc = null;
            while (op.Status())
            {
                rc = op.Next();
            }
        }
        [TestMethod]
        public void SelectWithRepeat()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
"GroupMatch", "GremlinTest");
            connection.SetupClient();
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            var ParserTree = parser.Parse("g.V.has('name','saturn').repeat(__.in('father')).times(2).name");
            var op = ParserTree.Generate(connection);
            Record rc = null;
            while (op.Status())
            {
                rc = op.Next();
            }
        }
        [TestMethod]
        public void ComplicatedSelect()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
"GroupMatch", "GremlinTest");
            connection.SetupClient();
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            var ParserTree = parser.Parse("g.E().has('reason', 'loves waves').as('source').values('reason').as('reason').select('source').outV().values('name').as('god').select('source').inV().values('name').as('thing').select('god', 'reason', 'thing')");
            var op = ParserTree.Generate(connection);
            Record rc = null;
            while (op.Status())
            {
                rc = op.Next();
            }
        }
        [TestMethod]
        public void SelectWithCoalesce()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
"GroupMatch", "GremlinTest");
            connection.SetupClient();
            GraphViewGremlinParser parser1 = new GraphViewGremlinParser();
            var ParserTree1 = parser1.Parse("g.V().has('name','saturn')coalesce(values('type'), values('age'))");
            var op1 = ParserTree1.Generate(connection);
            Record rc1 = null;
            while (op1.Status())
            {
                rc1 = op1.Next();
            }
            GraphViewGremlinParser parser2 = new GraphViewGremlinParser();
            var ParserTree2 = parser2.Parse("g.V().has('name','saturn').coalesce( values('age'), values('type'))");
            var op2 = ParserTree2.Generate(connection);
            Record rc2 = null;
            while (op2.Status())
            {
                rc2 = op2.Next();
            }
        }

        [TestMethod]
        public void SelectWithChoose()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
"GroupMatch", "GremlinTest");
            connection.SetupClient();
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            var ParserTree = parser.Parse("g.V().has('name','person'). choose(values('age')). option(27, __.in()). option(32, __.out()).values('name')");
            var op = ParserTree.Generate(connection);
            Record rc = null;
            while (op.Status())
            {
                rc = op.Next();
            }
        }

        [TestMethod]
        public void NativeAPITest()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
"GroupMatch", "GremlinTest");
            connection.SetupClient();
            GremlinPipeline g1 = new GremlinPipeline();
            var r1 =
                g1.V()
                    .has("name", "saturn")
                    .In("father")
                    .In("father")
                    .outE("battled")
                    .has("time", GremlinPipeline.gt(1))
                    .inV()
                    .values("name");
            var g2 = new GremlinPipeline();
            var r2 = g2.V().has("name", "saturn").In("father").In("father").Out("father", "mother").values("name");
            r2.connection = connection;
            r1.connection = connection;
            foreach (var x in r1)
            {
                var y = x;
            }
            foreach (var y in r2)
            {
                var z = y;
            }
        }
    }
    [TestClass]
    public class GraphViewGremlinInsertDeleteTest
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
        public void AddSimpleEdge()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
"GroupMatch", "GremlinTest");
            connection.SetupClient();
            //GraphViewGremlinParser parser1 = new GraphViewGremlinParser();
            //var ParserTree1 = parser1.Parse("g.addV('label','person','name','Adams')");
            //var op1 = ParserTree1.Generate(connection);
            //op1.Next();
            //GraphViewGremlinParser parser2 = new GraphViewGremlinParser();
            //var ParserTree2 = parser2.Parse("g.addV('label','person','name','Bob')");
            //var op2 = ParserTree2.Generate(connection);
            //op2.Next();
            //GraphViewGremlinParser parser3 = new GraphViewGremlinParser();
            //var ParserTree3 = parser3.Parse("g.V.as('v').has('name','Adams').as('a').select('v').has('name','Bob').as('b').select('a','b').addOutE('a','isfriend','b','for','10y')");
            //var op3 = ParserTree3.Generate(connection);
            //op3.Next();
            GraphViewGremlinParser parser4 = new GraphViewGremlinParser();
            var ParserTree4 = parser4.Parse("g.V.has('name','Adams').out('isfriend').name");
            var op4 = ParserTree4.Generate(connection);
            Record rc = null;
            while (op4.Status())
            {
                rc = op4.Next();
            }
        }
    }
}
