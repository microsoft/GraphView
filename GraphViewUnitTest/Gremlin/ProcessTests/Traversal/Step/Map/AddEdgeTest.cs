using System;
using System.Linq;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GraphViewUnitTest.Gremlin
{
    [TestClass]
    public class AddEdgeTest: AbstractGremlinTest
    {
        /// <summary>
        /// g_VX1X_asXaX_outXcreatedX_addEXcreatedByX_toXaX()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/AddEdgeTest.java
        /// Gremlin: g.V(v1Id).as("a").out("created").addE("createdBy").to("a");
        /// </summary>
        [TestMethod]
        public void AddEdgeWithNoExtraProperty()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                GraphViewCommand.OutputFormat = OutputFormat.GraphSON;
                var traversal = GraphViewCommand.g().V()
                    .Has("name", "marko")
                    .As("a")
                    .Out("created")
                    .AddE("createdBy")
                    .To("a");
                dynamic result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());
                GraphViewCommand.OutputFormat = OutputFormat.Regular;

                foreach (dynamic edge in result)
                {
                    Assert.AreEqual("createdBy", (string) edge.label);
                    Assert.IsNull(edge.properties);
                }

                Assert.AreEqual(1, result.Count);
                Assert.AreEqual(7, GraphViewCommand.g().E().Next().Count);
                Assert.AreEqual(6, GraphViewCommand.g().V().Next().Count);
            }
        }

        /// <summary>
        /// g_VX1X_asXaX_outXcreatedX_addEXcreatedByX_toXaX_propertyXweight_2X()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/AddEdgeTest.java
        /// Gremlin: g.V(v1Id).as("a").out("created").addE("createdBy").to("a").property("weight", 2.0d);
        /// </summary>
        [TestMethod]
        public void AddEdgeWithOneProperty()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                GraphViewCommand.OutputFormat = OutputFormat.GraphSON;
                var traversal = GraphViewCommand.g().V()
                    .Has("name", "marko")
                    .As("a")
                    .Out("created")
                    .AddE("createdBy")
                    .To("a")
                    .Property("weight", 2.0d);
                dynamic result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());
                GraphViewCommand.OutputFormat = OutputFormat.Regular;

                foreach (dynamic edge in result)
                {
                    Assert.AreEqual("createdBy", (string)edge.label);
                    Assert.AreEqual(2.0d, (double)edge.properties.weight, delta: 0.00001d);
                    Assert.AreEqual(1, ((JObject)edge.properties).Count);
                }

                Assert.AreEqual(1, result.Count);
                Assert.AreEqual(7, GraphViewCommand.g().E().Next().Count);
                Assert.AreEqual(6, GraphViewCommand.g().V().Next().Count);
            }
        }

        /// <summary>
        /// g_V_aggregateXxX_asXaX_selectXxX_unfold_addEXexistsWithX_toXaX_propertyXtime_nowX()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/AddEdgeTest.java
        /// Gremlin: g.V().aggregate("x").as("a").select("x").unfold().addE("existsWith").to("a").property("time", "now");
        /// </summary>
        /// <remarks>
        /// Aggregate(sideEffectKey) is not implemented
        /// Bug item: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36616
        /// </remarks>
        [TestMethod]
        public void AddMultipleEdges()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                GraphViewCommand.OutputFormat = OutputFormat.GraphSON;
                var traversal = GraphViewCommand.g().V()
                    .Aggregate("x")
                    .As("a")
                    .Select("x")
                    .Unfold()
                    .AddE("existsWith")
                    .To("a")
                    .Property("time", "now");
                dynamic result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());
                GraphViewCommand.OutputFormat = OutputFormat.Regular;

                foreach (dynamic edge in result)
                {
                    Assert.AreEqual("existsWith", (string)edge.label);
                    Assert.AreEqual("now", (string)edge.properties.time);
                    Assert.AreEqual(1, ((JObject)edge.properties).Count);
                }

                Assert.AreEqual(36, result.Count);
                Assert.AreEqual(42, GraphViewCommand.g().E().Next().Count);
                foreach (var v in GraphViewCommand.g().V().Id().Next())
                {
                    var outCount = GraphViewCommand.g().V().HasId(v).OutE().HasLabel("existsWith").Next().Count;
                    var inCount = GraphViewCommand.g().V().HasId(v).InE().HasLabel("existsWith").Next().Count;
                    Assert.AreEqual(6, outCount);
                    Assert.AreEqual(6, inCount);
                }
                Assert.AreEqual(6, GraphViewCommand.g().V().Next().Count);
            }
        }

        /// <summary>
        /// g_V_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_asXbX_addEXcodeveloperX_fromXaX_toXbX_propertyXyear_2009X()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/AddEdgeTest.java
        /// Gremlin: g.V().as("a").out("created").in("created").where(P.neq("a")).as("b").addE("codeveloper").from("a").to("b").property("year", 2009);
        /// </summary>
        [TestMethod]
        public void AddEdgeWithWhere()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                GraphViewCommand.OutputFormat = OutputFormat.GraphSON;
                var traversal = GraphViewCommand.g().V()
                    .As("a")
                    .Out("created")
                    .In("created")
                    .Where(Predicate.neq("a"))
                    .As("b")
                    .AddE("codeveloper")
                    .From("a")
                    .To("b")
                    .Property("year", 2009);
                dynamic result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());
                GraphViewCommand.OutputFormat = OutputFormat.Regular;

                foreach (dynamic edge in result)
                {
                    Assert.AreEqual("codeveloper", (string)edge.label);
                    Assert.AreEqual(2009, (int)edge.properties.year);
                    Assert.AreEqual(1, ((JObject)edge.properties).Count);
                    Assert.AreEqual("person", (string)edge.inVLabel);
                    Assert.AreEqual("person", (string)edge.outVLabel);
                    var inVName = GraphViewCommand.g().V().HasId((string)edge.inV).Values("name").Next().FirstOrDefault();
                    var outVName = GraphViewCommand.g().V().HasId((string)edge.outV).Values("name").Next().FirstOrDefault();
                    Assert.AreNotEqual("vadas", inVName);
                    Assert.AreNotEqual("vadas", outVName);
                    Assert.AreNotEqual(inVName, outVName);
                }

                Assert.AreEqual(6, result.Count);
                Assert.AreEqual(12, GraphViewCommand.g().E().Next().Count);
                Assert.AreEqual(6, GraphViewCommand.g().V().Next().Count);
            }
        }

        /// <summary>
        /// g_V_asXaX_inXcreatedX_addEXcreatedByX_fromXaX_propertyXyear_2009X_propertyXacl_publicX()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/AddEdgeTest.java
        /// Gremlin: g.V().as("a").in("created").addE("createdBy").from("a").property("year", 2009).property("acl", "public");
        /// </summary>
        [TestMethod]
        public void AddEdgeWithMultipleProperties()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                GraphViewCommand.OutputFormat = OutputFormat.GraphSON;
                var traversal = GraphViewCommand.g().V()
                    .As("a")
                    .In("created")
                    .AddE("createdBy")
                    .From("a")
                    .Property("year", 2009)
                    .Property("acl", "public");
                dynamic result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());
                GraphViewCommand.OutputFormat = OutputFormat.Regular;

                foreach (dynamic edge in result)
                {
                    Assert.AreEqual("createdBy", (string)edge.label);
                    Assert.AreEqual(2009, (int)edge.properties.year);
                    Assert.AreEqual("public", (string)edge.properties.acl);
                    Assert.AreEqual(2, ((JObject)edge.properties).Count);
                    Assert.AreEqual("person", (string)edge.inVLabel);
                    Assert.AreEqual("software", (string)edge.outVLabel);
                    var inVName = GraphViewCommand.g().V().HasId((string)edge.inV).Values("name").Next().FirstOrDefault();
                    var outVName = GraphViewCommand.g().V().HasId((string)edge.outV).Values("name").Next().FirstOrDefault();
                    if (outVName.Equals("ripple"))
                    {
                        Assert.AreEqual("josh", inVName);
                    }
                }

                Assert.AreEqual(4, result.Count);
                Assert.AreEqual(10, GraphViewCommand.g().E().Next().Count);
                Assert.AreEqual(6, GraphViewCommand.g().V().Next().Count);
            }
        }

        /// <summary>
        /// Original test
        /// Gremlin: g.V().has("name", "marko").as("a").out("created").addE("createdBy").to("a").label();
        /// </summary>
        /// <remarks>
        /// Add edge then Label() does not work
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36546
        /// </remarks>
        [TestMethod]
        public void AddEdgeThenGetLabel()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = GraphViewCommand.g().V()
                    .Has("name", "marko")
                    .As("a")
                    .Out("created")
                    .AddE("createdBy")
                    .To("a")
                    .Label();
                var result = traversal.Next();

                Assert.AreEqual(1, result.Count);
                Assert.AreEqual("createdBy", result.FirstOrDefault());
            }
        }
    }
}
