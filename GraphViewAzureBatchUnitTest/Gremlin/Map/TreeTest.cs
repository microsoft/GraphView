using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GraphViewAzureBatchUnitTest.Gremlin.Map
{
    [TestClass]
    public class TreeTest : AbstractAzureBatchGremlinTest
    {
        /// <summary>
        /// Port of the g_V_out_out_tree_byXidX() UT from org/apache/tinkerpop/gremlin/process/traversal/step/sideEffect/TreeTest.java.
        /// Equivalent gremlin: "g.V().out().out().tree().by(T.id)"
        /// </summary>
        [TestMethod]
        public void VerticesOutOutTreeById()
        {
            using (GraphViewCommand graphCommand = this.job.Command)
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphCommand.g().V().Out().Out().Tree().By(GraphTraversal.__().Id());

                AssertCommonC(graphCommand);

            }
}

        /// <summary>
        /// Port of the g_V_out_out_tree() UT from org/apache/tinkerpop/gremlin/process/traversal/step/sideEffect/TreeTest.java.
        /// Equivalent gremlin: "g.V().out().out().tree()"
        /// </summary>
        [TestMethod]
        public void VerticesOutOutTree()
        {
            using (GraphViewCommand graphCommand = this.job.Command)
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphCommand.g().V().Out().Out().Tree();

                AssertCommonC(graphCommand);
            }
        }

        /// <summary>
        /// Port of the g_V_out_out_treeXaX_capXaX() UT from org/apache/tinkerpop/gremlin/process/traversal/step/sideEffect/TreeTest.java.
        /// Equivalent gremlin: "g.V().out().out().tree("a").cap("a")"
        /// </summary>
        [TestMethod]
        public void VerticesOutOutTreeACapA()
        {
            using (GraphViewCommand graphCommand = this.job.Command)
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphCommand.g().V().Out().Out().Tree("a").Cap("a");

                AssertCommonC(graphCommand);
            }
        }



        /// <summary>
        /// Port of the g_V_out_out_out_tree() UT from org/apache/tinkerpop/gremlin/process/traversal/step/sideEffect/TreeTest.java.
        /// Equivalent gremlin: "g.V().out().out().out().tree()"
        /// </summary>
        [TestMethod]
        public void VerticesOutOutOutTree()
        {
            using (GraphViewCommand graphCommand = this.job.Command)
            {
                this.job.Traversal = graphCommand.g().V().Out().Out().Out().Tree();
                List<string> result = this.jobManager.TestQuery(this.job);
                dynamic results = JsonConvert.DeserializeObject<dynamic>(result[0]);

                Assert.AreEqual(0, results.Count);
            }
        }

        private void AssertCommonC(GraphViewCommand graphCommand)
        {
            List<string> result = this.jobManager.TestQuery(this.job);
            dynamic results = JsonConvert.DeserializeObject<dynamic>(result[0]);

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
    }
}
