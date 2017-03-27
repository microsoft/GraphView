using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GraphViewUnitTest.Gremlin.ProcessTests.Traversal.Step.Map
{
    [TestClass]
    public class MapTest : AbstractGremlinTest
    {
        /// <summary>
        /// Port of the g_V_localXoutE_countX UT from org/apache/tinkerpop/gremlin/process/traversal/step/branch/LocalTest.java.
        /// Equivalent gremlin: "g.V.local(__.outE.count())"
        /// </summary>
        [TestMethod]
        public void VerticesLocalOutECount()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g().V().Local(GraphTraversal2.__().OutE().Count());
                var result = traversal.Next().Select(r => long.Parse(r));

                var expectedResult = new List<long> { 3, 0, 0, 0, 1, 2 };
                CheckUnOrderedResults(expectedResult, result);
            }
        }

        /// <summary>
        /// Port of the g_VX4X_localXbothEXknows_createdX_limitX1XX UT from org/apache/tinkerpop/gremlin/process/traversal/step/branch/LocalTest.java.
        /// Equivalent gremlin: "g.V(v4Id).local(__.bothE('knows', 'created').limit(1))", "v4Id", v4Id
        /// </summary>
        [TestMethod]
        public void VertexWithIdLocalBothEKnowsCreatedLimit1()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                string joshVertexId = this.ConvertToVertexId(graphCommand, "josh");

                graphCommand.OutputFormat = OutputFormat.GraphSON;
                var traversal = graphCommand.g().V(joshVertexId).Local(GraphTraversal2.__().BothE("knows", "created").Limit(1));

                var result = traversal.Next();
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(1, dynamicResult.Count);

                var edge = dynamicResult[0];
                string edgeLabel = edge["label"].ToString();
                double edgeWeight = double.Parse(edge["properties"]["weight"].ToString());

                Assert.IsTrue(edgeLabel.Equals("created") || edgeLabel.Equals("knows"));
                Assert.IsTrue(edgeWeight.Equals(1.0D) || edgeWeight.Equals(0.4D));
            }
        }

        /// <summary>
        /// Port of the g_VX4X_localXbothE_limitX1XX_otherV_name UT from org/apache/tinkerpop/gremlin/process/traversal/step/branch/LocalTest.java.
        /// Equivalent gremlin: "g.V(v4Id).local(__.bothE.limit(1)).otherV.name", "v4Id", v4Id
        /// </summary>
        [TestMethod]
        public void VertexWithIdLocalBothELimit1OtherVName()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                string joshVertexId = this.ConvertToVertexId(graphCommand, "josh");
                var traversal = graphCommand.g().V(joshVertexId).Local(GraphTraversal2.__().BothE().Limit(1)).OtherV().Values("name");

                var result = traversal.Next();

                Assert.AreEqual(1, result.Count);
                Assert.IsTrue(result[0].Equals("marko") || result[0].Equals("ripple") || result[0].Equals("lop"));
            }
        }

        /// <summary>
        /// Port of the g_VX4X_localXbothE_limitX2XX_otherV_name UT from org/apache/tinkerpop/gremlin/process/traversal/step/branch/LocalTest.java.
        /// Equivalent gremlin: "g.V(v4Id).local(__.bothE.limit(2)).otherV.name", "v4Id", v4Id
        /// </summary>
        [TestMethod]
        public void VertexWithIdLocalBothELimit2OtherVName()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                string joshVertexId = this.ConvertToVertexId(graphCommand, "josh");
                var traversal = graphCommand.g().V(joshVertexId).Local(GraphTraversal2.__().BothE().Limit(2)).OtherV().Values("name");

                var result = traversal.Next();

                Assert.AreEqual(2, result.Count);
                foreach (var res in result)
                {
                    Assert.IsTrue(res.Equals("marko") || res.Equals("ripple") || res.Equals("lop"));
                }
            }
        }

        /// <summary>
        /// Port of the g_V_localXinEXknowsX_limitX2XX_outV_name UT from org/apache/tinkerpop/gremlin/process/traversal/step/branch/LocalTest.java.
        /// Equivalent gremlin: "g.V().local(__.inE('knows').limit(2).outV).name"
        /// </summary>
        [TestMethod]
        public void VertexWithIdLocalInEKnowsLimit2OutVName()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                string markoVertexId = this.ConvertToVertexId(graphCommand, "marko");
                var traversal = graphCommand.g().V().Local(GraphTraversal2.__().InE("knows").Limit(2)).OutV().Values("name");

                var result = traversal.Next();

                Assert.AreEqual(2, result.Count);
                foreach (var res in result)
                {
                    Assert.AreEqual("marko", res);
                }
            }
        }

        /// <summary>
        /// Port of the g_VX4X_localXbothEX1_createdX_limitX1XX UT from org/apache/tinkerpop/gremlin/process/traversal/step/branch/LocalTest.java.
        /// Equivalent gremlin: "g.V(v4Id).local(__.bothE('created').limit(1))", "v4Id", v4Id);
        /// </summary>
        [TestMethod]
        public void VertexWithIdLocalBothECreatedLimit1()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                string joshVertexId = this.ConvertToVertexId(graphCommand, "josh");

                graphCommand.OutputFormat = OutputFormat.GraphSON;
                var traversal = graphCommand.g().V(joshVertexId).Local(GraphTraversal2.__().BothE("created").Limit(1));

                var result = traversal.Next();
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(1, dynamicResult.Count);

                var edge = dynamicResult[0];
                Assert.AreEqual("created", edge["label"].ToString());
                double edgeWeight = double.Parse(edge["properties"]["weight"].ToString());
                Assert.IsTrue(edgeWeight.Equals(1.0D) || edgeWeight.Equals(0.4D));
            }
        }

        /// <summary>
        /// Port of the g_VX1X_localXoutEXknowsX_limitX1XX_inV_name UT from org/apache/tinkerpop/gremlin/process/traversal/step/branch/LocalTest.java.
        /// Equivalent gremlin: "g.V(v1Id).local(__.outE('knows').limit(1)).inV.name", "v1Id", v1Id
        /// </summary>
        [TestMethod]
        public void VertexWithIdLocalOutEKnowsLimit1InVName()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                string markoVertexId = this.ConvertToVertexId(graphCommand, "marko");
                var traversal = graphCommand.g().V(markoVertexId).Local(GraphTraversal2.__().OutE("knows").Limit(1)).InV().Values("name");

                var result = traversal.Next();

                Assert.AreEqual(1, result.Count);
                Assert.IsTrue(result[0].Equals("vadas") || result[0].Equals("josh"));
            }
        }

        /// <summary>
        /// Port of the g_V_localXbothEXcreatedX_limitX1XX_otherV_name UT from org/apache/tinkerpop/gremlin/process/traversal/step/branch/LocalTest.java.
        /// Equivalent gremlin: "g.V().local(__.bothE('created').limit(1)).otherV.name");
        /// </summary>
        [TestMethod]
        public void VerticesLocalBothECreatedLimit1OtherVName()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g().V().Local(GraphTraversal2.__().BothE("created").Limit(1)).OtherV().Values("name");

                var result = traversal.Next();

                Assert.AreEqual(5, result.Count);
                foreach (var res in result)
                {
                    Assert.IsTrue(res.Equals("marko") || res.Equals("lop") || res.Equals("josh") || res.Equals("ripple") || res.Equals("peter"));
                }
            }
        }
    }
}
