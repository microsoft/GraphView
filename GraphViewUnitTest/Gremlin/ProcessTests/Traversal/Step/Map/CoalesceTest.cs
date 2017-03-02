using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewUnitTest.Gremlin.ProcessTests.Traversal.Step.Map
{
    [TestClass]
    public class CoalesceTest : AbstractGremlinTest
    {
        /// <summary>
        /// g_V_coalesceXoutXfooX_outXbarXX()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/CoalesceTest.java
        /// Gremlin: g.V().coalesce(out("foo"), out("bar"));
        /// </summary>
        [TestMethod]
        public void CoalesceWithNonexistentTraversals()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = GraphViewCommand.g().V()
                    .Coalesce(
                        GraphTraversal2.__().Out("foo"),
                        GraphTraversal2.__().Out("bar"));
                var result = traversal.Next();

                Assert.IsFalse(result.Any());
            }
        }

        /// <summary>
        /// g_VX1X_coalesceXoutXknowsX_outXcreatedXX_valuesXnameX()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/CoalesceTest.java
        /// Gremlin: g.V(v1Id).coalesce(out("knows"), out("created")).values("name");
        /// </summary>
        [TestMethod]
        public void CoalesceWithTwoTraversals()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = GraphViewCommand.g().V()
                    .HasId(this.ConvertToVertexId(GraphViewCommand, "marko"))
                    .Coalesce(
                        GraphTraversal2.__().Out("knows"),
                        GraphTraversal2.__().Out("created"))
                    .Values("name");
                var result = traversal.Next();

                AbstractGremlinTest.CheckUnOrderedResults(new string[] { "josh", "vadas" }, result);
            }
        }

        /// <summary>
        /// g_VX1X_coalesceXoutXcreatedX_outXknowsXX_valuesXnameX()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/CoalesceTest.java
        /// Gremlin: g.V(v1Id).coalesce(out("created"), out("knows")).values("name");
        /// </summary>
        [TestMethod]
        public void CoalesceWithTraversalsInDifferentOrder()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = GraphViewCommand.g().V()
                    .HasId(this.ConvertToVertexId(GraphViewCommand, "marko"))
                    .Coalesce(
                        GraphTraversal2.__().Out("created"),
                        GraphTraversal2.__().Out("knows"))
                    .Values("name");
                var result = traversal.Next();

                AbstractGremlinTest.CheckUnOrderedResults(new string[] { "lop" }, result);
            }
        }

        /// <summary>
        /// g_V_coalesceXoutXlikesX_outXknowsX_inXcreatedXX_groupCount_byXnameX()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/CoalesceTest.java
        /// Gremlin: g.V().coalesce(out("likes"), out("knows"), out("created")).<String>groupCount().by("name");
        /// </summary>
        /// <remarks>
        /// GroupCount() Not Implemented on GraphTraversal2
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36609
        /// </remarks>
        [Ignore]
        [TestMethod]
        public void CoalesceWithGroupCount()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                // TODO: Implement GroupCount()

                //var traversal = GraphViewCommand.g().V()
                //    .Coalesce(
                //        GraphTraversal2.__().Out("likes"),
                //        GraphTraversal2.__().Out("knows"),
                //        GraphTraversal2.__().Out("created"))
                //    .GroupCount()
                //    .By("name");
                //Dictionary<string, long> result = traversal.Next();

                //Assert.AreEqual(4, result.Count);
                //AbstractGremlinTest.CheckUnOrderedResults(new string[] { "josh", "lop", "ripple", "vadas" }, result.Keys);
                //Assert.AreEqual(1, result["josh"]);
                //Assert.AreEqual(2, result["lop"]);
                //Assert.AreEqual(1, result["ripple"]);
                //Assert.AreEqual(1, result["vadas"]);
            }
        }

        /// <summary>
        /// g_V_coalesceXoutEXknowsX_outEXcreatedXX_otherV_path_byXnameX_byXlabelX()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/CoalesceTest.java
        /// Gremlin: g.V().coalesce(outE("knows"), outE("created")).otherV().path().by("name").by(T.label);
        /// </summary>
        /// <remarks>
        /// Coalesce() then Path() does not work
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36750
        /// 
        /// Path().By("name") does not work
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36753
        /// </remarks>

        [Ignore]
        [TestMethod]
        public void CoalesceWithPath()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                // TODO: Use Graphson to parse result.

                //var label = Enumerable.Empty<string>();
                //List<Path> expected = new List<Path>
                //{
                //    new Path().Extend("marko", label).Extend("knows", label).Extend("vadas", label),
                //    new Path().Extend("marko", label).Extend("knows", label).Extend("josh", label),
                //    new Path().Extend("josh", label).Extend("created", label).Extend("ripple", label),
                //    new Path().Extend("josh", label).Extend("created", label).Extend("lop", label),
                //    new Path().Extend("peter", label).Extend("created", label).Extend("lop", label),
                //};

                //var traversal = GraphViewCommand.g().V()
                //    .Coalesce(
                //        GraphTraversal2.__().OutE("knows"),
                //        GraphTraversal2.__().OutE("created"))
                //    .OtherV()
                //    .Path()
                //    .By("name")
                //    .By("label");
                //IEnumerable<Path> result = traversal.Next();

                //AbstractGremlinTest.CheckUnOrderedResults<Path>(expected, result);
            }
        }
    }
}