using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;

namespace GraphViewUnitTest.Gremlin.ProcessTests
{
    [TestClass]
    public class CustomTest : AbstractGremlinTest
    {
        [TestMethod]
        [TestModernCompatible]
        public void AddVWithNestingProperties()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                // var traversal = command.g().Inject(0).Union(command.g().V().Group(), command.g().E().Group()).Select(GremlinKeyword.Column.Keys).Values("name");
                //var traversal = command.g().V().GroupCount().Unfold().Select(GremlinKeyword.Column.Keys).Values("name");
                // var traversal = command.g().V().Group().By().By(GraphTraversal2.__().Count()).Select(GremlinKeyword.Column.Keys).Unfold().Values("name");
                var traversal = command.g()
                    .V()
                    .Match(
                        GraphTraversal2.__().As("x").Out("dummy").As("z"),
                        GraphTraversal2.__().As("y").Out("dummy").As("z"),
                        GraphTraversal2.__().As("z").Out("dummy").As("a"),
                        GraphTraversal2.__().As("f").Out("dummy").As("h"),
                        GraphTraversal2.__().As("c").Has("age", 29),
                        GraphTraversal2.__().As("b").Has("name", "lop"),
                        GraphTraversal2.__().As("a").Out("created").As("b"),
                        GraphTraversal2.__().As("g").Out("dummy").As("f"),
                        GraphTraversal2.__().As("b").In("created").As("c"),
                        GraphTraversal2.__().As("c").Out("dummy").As("a")
                    )
                    .Select("a", "c")
                    .By("name");

                // var traversal = command.g().V().Match(GraphTraversal2.__().As("a"));
                var result = traversal.Next();
            }
        }

        [TestMethod]
        [TestModernCompatible]
        public void AddPropertyToFormHybridVertex()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection)) {
                var traversal = command.g().V().Has("name", "marko").Property(GremlinKeyword.PropertyCardinality.List, "prop", "value", "m1", "m1v", "m2", "m2v");
                var result = traversal.Next();
                Debugger.Break();
            }
        }

        [TestMethod]
        [TestModernCompatible]
        [ExpectedException(typeof(GraphViewException), "The adding/updating property 'age' already exists as flat.")]
        public void AddPropertyToFormHybridVertexException()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection)) {
                var traversal = command.g().V().Has("name", "marko").Property(GremlinKeyword.PropertyCardinality.List, "age", 2333, "m1", "m1v", "m2", "m2v");
                var result = traversal.Next();
            }
        }

        [TestMethod]
        [TestModernCompatible]
        public void TryDropFlatProperty()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection)) {
                var traversal = command.g().V().Has("name", "marko").Properties("age").Drop();
                var result = traversal.Next();
                Debugger.Break();
            }
        }


        [TestMethod]
        public void NoneReversedEdges()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                var result = command.g().V().BothE().Next();
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
                Debugger.Break();
            }
        }
    }
}
