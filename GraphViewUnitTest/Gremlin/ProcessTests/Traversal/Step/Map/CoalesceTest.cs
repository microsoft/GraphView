using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

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
        [TestModernCompatible]
        public void CoalesceWithNonexistentTraversals()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = GraphViewCommand.g().V()
                    .Coalesce(
                        GraphTraversal.__().Out("foo"),
                        GraphTraversal.__().Out("bar"));
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
        [TestModernCompatible]
        public void CoalesceWithTwoTraversals()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = GraphViewCommand.g().V()
                    .HasId(this.ConvertToVertexId(GraphViewCommand, "marko"))
                    .Coalesce(
                        GraphTraversal.__().Out("knows"),
                        GraphTraversal.__().Out("created"))
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
        [TestModernCompatible]
        public void CoalesceWithTraversalsInDifferentOrder()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = GraphViewCommand.g().V()
                    .HasId(this.ConvertToVertexId(GraphViewCommand, "marko"))
                    .Coalesce(
                        GraphTraversal.__().Out("created"),
                        GraphTraversal.__().Out("knows"))
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
        [TestMethod]
        [TestModernCompatible]
        public void CoalesceWithGroupCount()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                GraphViewCommand.OutputFormat = OutputFormat.GraphSON;
                var traversal = GraphViewCommand.g().V()
                    .Coalesce(
                        GraphTraversal.__().Out("likes"),
                        GraphTraversal.__().Out("knows"),
                        GraphTraversal.__().Out("created"))
                    .GroupCount()
                    .By("name");

                var result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());
                Assert.AreEqual(1, (int)result[0]["josh"]);
                Assert.AreEqual(2, (int)result[0]["lop"]);
                Assert.AreEqual(1, (int)result[0]["ripple"]);
                Assert.AreEqual(1, (int)result[0]["vadas"]);
            }
        }

        /// <summary>
        /// g_V_coalesceXoutEXknowsX_outEXcreatedXX_otherV_path_byXnameX_byXlabelX()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/CoalesceTest.java
        /// Gremlin: g.V().coalesce(outE("knows"), outE("created")).otherV().path().by("name").by(T.label);
        /// </summary>

        [TestMethod]
        [TestModernCompatible]
        public void CoalesceWithPath()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                GraphViewCommand.OutputFormat = OutputFormat.GraphSON;
                var traversal = GraphViewCommand.g().V()
                    .Coalesce(
                        GraphTraversal.__().OutE("knows"),
                        GraphTraversal.__().OutE("created"))
                    .OtherV()
                    .Path()
                    .By("name")
                    .By("label");

                var result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());

                //CheckOrderedResults(new [] {"marko", "knows", "vadas"}, ((JArray)result[0]).Select(p=>p.ToString()).ToList());
                //CheckOrderedResults(new[] { "marko", "knows", "josh" }, ((JArray)result[1]).Select(p => p.ToString()).ToList());
                //CheckOrderedResults(new[] { "josh", "created", "ripple" }, ((JArray)result[2]).Select(p => p.ToString()).ToList());
                //CheckOrderedResults(new[] { "josh", "created", "lop" }, ((JArray)result[3]).Select(p => p.ToString()).ToList());
                //CheckOrderedResults(new[] { "peter", "created", "lop" }, ((JArray)result[3]).Select(p => p.ToString()).ToList());
            }
        }
    }
}