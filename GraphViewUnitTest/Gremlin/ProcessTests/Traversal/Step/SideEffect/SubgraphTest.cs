using System;
using System.Linq;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GraphViewUnitTest.Gremlin
{
    [TestClass]
    public class SubgraphTest: AbstractGremlinTest
    {
        [TestMethod]
        public void SubgraphSameAsOriginalGraph()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                command.OutputFormat = OutputFormat.GraphSON;
                var originGraphSON = command.g().V().Next()[0];
                command.OutputFormat = OutputFormat.Regular;
                var subGraphSON = command.g().E().Subgraph("sg").Cap("sg").Next()[0];

                var originGraph = JToken.Parse(originGraphSON);
                var subGraph = JToken.Parse(subGraphSON);

                Assert.IsTrue(JToken.DeepEquals(originGraph, subGraph));
            }
        }

        [TestMethod]
        public void SubgraphWithMultiVp()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                command.g()
                    .V()
                    .Has("name", "peter")
                    .Property(GremlinKeyword.PropertyCardinality.List, "age", 13)
                    .Property("qqq", false)
                    .Next();

                var traversal = command.g().E().Subgraph("sg").Cap("sg");
                traversal.Next();
            }
        }
        
        [TestMethod]
        public void SubgraphAggregation()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                
                command.OutputFormat = OutputFormat.GraphSON;
                var originGraphSON = command.g().V().Next()[0];
                command.OutputFormat = OutputFormat.Regular;
                command.CommandText =
                    "g.E().hasLabel('created').subgraph('sg').bothV().bothE().subgraph('sg').cap('sg')";
                var subGraphSON = command.ExecuteAndGetResults()[0];

                var originGraph = JToken.Parse(originGraphSON);
                var subGraph = JToken.Parse(subGraphSON);

                Assert.IsTrue(JToken.DeepEquals(originGraph, subGraph));
            }
        }

        [TestMethod]
        public void SubgraphInRepeat()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                command.CommandText =
                    "g.V().has('name', 'lop').repeat(__.inE().subgraph('subGraph').outV()).times(3).cap('subGraph')";
                var subGraphSON = command.ExecuteAndGetResults()[0];
                var subGraph = JArray.Parse(subGraphSON);

                Assert.AreEqual(subGraph.Count, 4);
            }
        }

        [TestMethod]
        public void SubgraphWithDifferentKeys()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                command.CommandText =
                    "g.V().outE('knows').subgraph('knowsG').inV().outE('created').subgraph('createdG').inV().inE('created').subgraph('createdG')";
                var result = command.ExecuteAndGetResults();

                Assert.AreEqual(result.Count, 4);
            }
        }
        
    }
}
