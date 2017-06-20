using System;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Configuration;
using System.Linq;
using GraphView;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GraphViewUnitTest.Gremlin.ProcessTests.Traversal.Step.Filter
{
    [TestClass]
    public class AndTest : AbstractGremlinTest
    {
        /// <summary>
        /// g_V_andXhasXage_gt_27X__outE_count_gt_2X_name()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/AndTest.java
        /// Gremlin: g.V().and(has("age", P.gt(27)), outE().count().is(P.gte(2l))).values("name");
        /// </summary>
        [TestMethod]
        [TestModernCompatible(false)]
        public void AndWithParameters()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = GraphViewCommand.g().V()
                    .And(
                        GraphTraversal.__().Has("age", Predicate.gt(27)),
                        GraphTraversal.__().OutE().Count().Is(Predicate.gte(2)))
                    .Values("name");
                var result = traversal.Next();

                AbstractGremlinTest.CheckUnOrderedResults(new string[] { "marko", "josh" }, result);
            }
        }

        /// <summary>
        /// g_V_andXout__hasXlabel_personX_and_hasXage_gte_32XX_name()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/AndTest.java
        /// Gremlin: g.V().and(outE(), has(T.label, "person").and().has("age", P.gte(32))).values("name");
        /// </summary>
        /// <remarks>
        /// Bug 36109: Calling GraphTraversal.And() with no parameter throws exception.
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36511
        /// </remarks>
        [TestMethod]
        [TestModernCompatible(false)]
        public void AndAsInfixNotation()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = GraphViewCommand.g().V()
                    .And(
                        GraphTraversal.__().OutE(),
                        GraphTraversal.__().HasLabel("person").And().Has("age", Predicate.gte(32)))
                    .Values("name");
                var result = traversal.Next();

                AbstractGremlinTest.CheckUnOrderedResults(new string[] { "josh", "peter" }, result);
            }
        }

        /// <summary>
        /// g_V_asXaX_outXknowsX_and_outXcreatedX_inXcreatedX_asXaX_name()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/AndTest.java
        /// Gremlin: g.V().as("a").out("knows").and().out("created").in("created").as("a").values("name");
        /// </summary>
        /// <remarks>
        /// Bug 36109: Calling GraphTraversal.And() with no parameter throws exception.
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36511
        /// </remarks>
        [TestMethod]
        [TestModernCompatible]
        public void AndWithAs()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                string vertex = ConvertToVertexId(GraphViewCommand, "marko");
                GraphViewCommand.OutputFormat = OutputFormat.GraphSON;
                var traversal = GraphViewCommand.g().V()
                    .As("a")
                    .Out("knows")
                    .And()
                    .Out("created")
                    .In("created")
                    .As("a")
                    .Values("name");
                List<string> tmp = traversal.Next();
                var result = JsonConvert.DeserializeObject<JArray>(tmp.FirstOrDefault()).First;

                Assert.AreEqual(vertex, (string)result["id"]);
            }
        }

        /// <summary>
        /// g_V_asXaX_andXselectXaX_selectXaXX()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/AndTest.java
        /// Gremlin: g.V().as("a").and(select("a"), select("a"));
        /// </summary>
        /// <remarks>
        /// Bug 36111: And(Select(), Select()) throws exception
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36510
        /// </remarks>
        [TestMethod]
        [TestModernCompatible]
        public void AndWithSelect()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = GraphViewCommand.g().V()
                    .As("a")
                    .And(
                        GraphTraversal.__().Select("a"),
                        GraphTraversal.__().Select("a"));
                var result = traversal.Next();

                Assert.AreEqual(6, result.Count());
            }
        }
    }
}
