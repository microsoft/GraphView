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
            WSyntaxTree ParserTree = parser.Parse();
        }

        [TestMethod]
        public void SematicAnalyseTest()
        {
            string ErrorKey = "";
            var para = GraphViewGremlinParser.LexicalAnalyzer.Tokenize(@"g.V.has('name','hercules').out('father').out('father').name", ref ErrorKey);
            GraphViewGremlinParser parser = new GraphViewGremlinParser(para.Item1, para.Item2);
            var ParserTree = parser.Parse();
            var SematicAnalyser = new GraphViewGremlinSematicAnalyser(ParserTree, para.Item2);
            SematicAnalyser.Analyse();
        }
        [TestMethod]
        public void TransformToSqlTreeTest()
        {
            string ErrorKey = "";
            var para = GraphViewGremlinParser.LexicalAnalyzer.Tokenize(@"g.V(saturn).in('father').in('father').values('name')", ref ErrorKey);
            GraphViewGremlinParser parser = new GraphViewGremlinParser(para.Item1, para.Item2);
            var ParserTree = parser.Parse();
            var SematicAnalyser = new GraphViewGremlinSematicAnalyser(ParserTree, para.Item2);
            SematicAnalyser.Analyse();
            SematicAnalyser.Transform();
            var SqlTree = (SematicAnalyser.SqlTree as WSelectStatement).QueryExpr;
            string result = SqlTree.ToString();
            Console.ReadKey();
        }
    }
}
