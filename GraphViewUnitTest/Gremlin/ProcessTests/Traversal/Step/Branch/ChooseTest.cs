using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewUnitTest.Gremlin.ProcessTests.Traversal.Step.Map
{
    [TestClass]
    public class ChooseTest : AbstractGremlinTest
    {
        /// <summary>
        /// get_g_V_chooseXout_countX_optionX2L__nameX_optionX3L__valueMapX()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/branch/ChooseTest.java
        /// Gremlin: g.V().choose(out().count()).option(2, __.values("name")).option(3, __.valueMap())
        /// </summary>
        [TestMethod]
        [TestModernCompatible(false)]
        public void get_g_V_chooseXout_countX_optionX2L__nameX_optionX3L__valueMapX()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = GraphViewCommand.g().V()
                    .Choose(GraphTraversal.__().Out().Count())
                    .Option(2, GraphTraversal.__().Values("name"))
                    .Option(3, GraphTraversal.__().ValueMap());
                var result = traversal.Next();

                Assert.AreEqual(2, result.Count);
                Assert.AreEqual("josh", result[1]);
                try
                {
                    Assert.AreEqual("[name:[marko], age:[29]]", result[0]);
                }
                catch (Exception)
                {
                    Assert.AreEqual("[age:[29], name:[marko]]", result[0]);
                }
                
            }
        }

        /// <summary>
        /// get_g_V_chooseXhasLabelXpersonX_and_outXcreatedX__outXknowsX__identityX_name()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/branch/ChooseTest.java
        /// Gremlin: g.V().choose(hasLabel("person").and().out("created"), out("knows"), identity()).values("name");
        /// </summary>
        [TestMethod]
        [TestModernCompatible]
        public void get_g_V_chooseXhasLabelXpersonX_and_outXcreatedX__outXknowsX__identityX_name()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = GraphViewCommand.g().V()
                    .Choose(GraphTraversal.__().HasLabel("person").And().Out("created"),
                        GraphTraversal.__().Out("knows"), GraphTraversal.__().Identity()).Values("name");
                var result = traversal.Next();

                CheckUnOrderedResults(new [] { "lop", "ripple", "josh", "vadas", "vadas" }, result);
            }
        }

        /// <summary>
        /// get_g_V_chooseXlabelX_optionXblah__outXknowsXX_optionXbleep__outXcreatedXX_optionXnone__identityX_name()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/branch/ChooseTest.java
        /// Gremlin: g.V().choose(label()).option("blah", out("knows")).option("bleep", out("created")).option(Pick.none, identity()).values("name")
        /// </summary>
        [TestMethod]
        [TestModernCompatible]
        public void get_g_V_chooseXlabelX_optionXblah__outXknowsXX_optionXbleep__outXcreatedXX_optionXnone__identityX_name()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = GraphViewCommand.g().V()
                    .Choose(GraphTraversal.__().Label())
                    .Option("blah", GraphTraversal.__().Out("knows"))
                    .Option("bleep", GraphTraversal.__().Out("created"))
                    .Option(GremlinKeyword.Pick.None, GraphTraversal.__().Identity())
                    .Values("name");
                var result = traversal.Next();

                AbstractGremlinTest.CheckUnOrderedResults(new [] { "marko", "vadas", "peter", "josh", "lop", "ripple" }, result);
            }
        }

        /// <summary>
        /// get_g_V_chooseXoutXknowsX_count_isXgtX0XX__outXknowsXX_name()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/branch/ChooseTest.java
        /// Gremlin: g.V().choose(out("knows").count().is(gt(0)), out("knows")).values("name");
        /// </summary>
        [TestMethod]
        [TestModernCompatible]
        public void get_g_V_chooseXoutXknowsX_count_isXgtX0XX__outXknowsXX_name()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = GraphViewCommand.g().V()
                    .Choose(GraphTraversal.__().Out("knows").Count().Is(Predicate.gt(0)), GraphTraversal.__().Out("knows"))
                    .Values("name");
                var result = traversal.Next();

                CheckUnOrderedResults(new [] { "vadas", "josh", "vadas", "josh", "peter", "lop", "ripple" }, result);
            }
        }
    }
}