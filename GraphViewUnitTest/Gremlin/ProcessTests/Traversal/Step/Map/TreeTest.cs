using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Linq;
using GraphView;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GraphViewUnitTest.Gremlin.ProcessTests.Traversal.Step.Map
{
    [TestClass]
    public class TreeTest : AbstractGremlinTest
    {
        private void AssertCommonA(GraphTraversal2 traversal)
        {
            dynamic results = JsonConvert.DeserializeObject<dynamic>(traversal.Next()[0]);

            Assert.AreEqual(1, results.Count);

            bool isContained = false;
            JObject markoChildren = TreeNodesContainName(results[0], "marko", out isContained);
            Assert.IsTrue(isContained);

            Assert.AreEqual(1, markoChildren.Count);
            JObject joshChildren = this.TreeNodesContainName(markoChildren, "josh", out isContained);
            Assert.IsTrue(isContained);

            this.TreeNodesContainName(joshChildren, "lop", out isContained);
            Assert.IsTrue(isContained);

            this.TreeNodesContainName(joshChildren, "ripple", out isContained);
            Assert.IsTrue(isContained);
        }

        private JObject TreeNodesContainName(JObject root, string name, out bool isContained)
        {
            isContained = false;

            foreach (JToken token in root.Children())
            {
                if (token is JProperty)
                {
                    var prop = token as JProperty;
                    JToken childToken = root[prop.Name];
                    if (childToken != null && root[prop.Name]["key"]["properties"]["name"][0]["value"].ToString() == name)
                    {
                        isContained = true;
                        return root[prop.Name]["value"] as JObject;
                    }
                }
            }
            return null;
        }

        /// <summary>
        /// Port of the g_V_out_out_tree_byXidX() UT from org/apache/tinkerpop/gremlin/process/traversal/step/sideEffect/TreeTest.java.
        /// Equivalent gremlin: "g.V().out().out().tree().by(T.id)"
        /// </summary>
        [TestMethod]
        public void VerticesOutOutTreeById()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                var traversal = graphCommand.g().V().Out().Out().Tree().By(GraphTraversal2.__().Id());

                this.AssertCommonC(traversal, graphCommand);
            }
        }

        /// <summary>
        /// Port of the g_V_out_out_tree() UT from org/apache/tinkerpop/gremlin/process/traversal/step/sideEffect/TreeTest.java.
        /// Equivalent gremlin: "g.V().out().out().tree()"
        /// </summary>
        [TestMethod]
        public void VerticesOutOutTree()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                var traversal = graphCommand.g().V().Out().Out().Tree();

                this.AssertCommonC(traversal, graphCommand);
            }
        }

        /// <summary>
        /// Port of the g_V_out_out_treeXaX_capXaX() UT from org/apache/tinkerpop/gremlin/process/traversal/step/sideEffect/TreeTest.java.
        /// Equivalent gremlin: "g.V().out().out().tree("a").cap("a")"
        /// </summary>
        [TestMethod]
        public void VerticesOutOutTreeACapA()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                var traversal = graphCommand.g().V().Out().Out().Tree("a").Cap("a");

                this.AssertCommonC(traversal, graphCommand);
            }
        }

        private void AssertCommonC(GraphTraversal2 traversal, GraphViewCommand graphCommand)
        {
            string vertexId = this.ConvertToVertexId(graphCommand, "marko");
            dynamic results = JsonConvert.DeserializeObject<dynamic>(traversal.Next()[0]);

            Assert.AreEqual(1, results.Count);

            bool isContained = false;
            JObject markoChildren = TreeNodesContainId(
                results[0], this.ConvertToVertexId(graphCommand, "marko"), out isContained);
            Assert.IsTrue(isContained);

            Assert.AreEqual(1, markoChildren.Count);
            JObject joshChildren = this.TreeNodesContainId(
                markoChildren, this.ConvertToVertexId(graphCommand, "josh"), out isContained);
            Assert.IsTrue(isContained);

            this.TreeNodesContainId(joshChildren, this.ConvertToVertexId(graphCommand, "lop"), out isContained);
            Assert.IsTrue(isContained);
            this.TreeNodesContainId(joshChildren, this.ConvertToVertexId(graphCommand, "ripple"), out isContained);
            Assert.IsTrue(isContained);
        }

        private JObject TreeNodesContainId(JObject root, string id, out bool isContained)
        {
            isContained = false;
            foreach (JToken token in root.Children())
            {
                if (token is JProperty)
                {
                    var prop = token as JProperty;

                    if (id.Contains(prop.Name))
                    {
                        isContained = true;
                        if (root[prop.Name] != null)
                        {
                            return root[prop.Name]["value"] as JObject;
                        }
                        break;
                    }
                }
            }
            return null;
        }

        /// <summary>
        /// Port of the g_V_out_out_out_tree() UT from org/apache/tinkerpop/gremlin/process/traversal/step/sideEffect/TreeTest.java.
        /// Equivalent gremlin: "g.V().out().out().out().tree()"
        /// </summary>
        [TestMethod]

        public void VerticesOutOutOutTree()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g().V().Out().Out().Out().Tree();
                dynamic results = JsonConvert.DeserializeObject<dynamic>(traversal.Next()[0]);

                Assert.AreEqual(0, results.Count);
            }
        }
    }
}
