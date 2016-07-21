using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView;

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
            SematicAnalyser.Transform();
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
            var ParserTree = parser.Parse("g.V().has('name', 'saturn').next()");
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
    }
}
