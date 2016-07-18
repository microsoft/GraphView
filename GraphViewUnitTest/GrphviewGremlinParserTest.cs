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
            var para = GraphViewGremlinParser.LexicalAnalyzer.Tokenize(@"g.V(hercules).outE('battled').type", ref ErrorKey);
            GraphViewGremlinParser parser = new GraphViewGremlinParser(para.Item1, para.Item2);
            var ParserTree = parser.ParseTree();
            var SematicAnalyser = new GraphViewGremlinSematicAnalyser(ParserTree, para.Item2);
            SematicAnalyser.Analyse();
            SematicAnalyser.Transform();
            var SqlTree = (SematicAnalyser.SqlTree as WSelectStatement).QueryExpr;
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
        "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
        "GroupMatch", "GraphSeven");
            connection.SetupClient();
            var reader = SqlTree.Generate(connection);
            var rc = reader.Next();
            string result = SqlTree.ToString();
            Console.ReadKey();
        }
    }
}
