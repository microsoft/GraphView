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
        public void AddVWithNestingProperties()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                //var traversal = command.g()
                //    .V()
                //    .Match(
                //        GraphTraversal2.__().As("b").In("created").As("c"),
                //        GraphTraversal2.__().As("c").Has("age", 29),
                //        GraphTraversal2.__().As("b").Has("name", "lop"),
                //        GraphTraversal2.__().As("a").Out("created").As("b")
                //    )
                //    .Select("a", "c")
                //    .By("name");

                //var traversal = command.g()
                //    .V()
                //    .Match(
                //        GraphTraversal2.__().As("a").Out().As("b"),
                //        GraphTraversal2.__().Not(GraphTraversal2.__().As("b").In("knows").As("a"))
                //    )
                //    .Select("a", "b")
                //    .By("name");

                //var traversal = command.g()
                //    .V()
                //    .As("a")
                //    .Out()
                //    .As("b")
                //    .Match(
                //        GraphTraversal2.__().As("a").Out().Count().As("c"),
                //        GraphTraversal2.__().Not(GraphTraversal2.__().As("a").In().As("b"))
                //    )
                //    .Select("a", "b", "c")
                //    .By("name")
                //    .By("name")
                //    .By();

                //var traversal = command.g()
                //    .V()
                //    .As("a")
                //    .Out()
                //    .As("b")
                //    .Match(
                //        GraphTraversal2.__().As("a").Out().Count().As("c")
                //    );

                //var traversal = command.g()
                //    .V()
                //    .Match(
                //        GraphTraversal2.__().As("a").Out().As("b"),
                //        GraphTraversal2.__().Not(GraphTraversal2.__().As("b").In("created").As("a"))
                //    );

                //var traversal = command.g()
                //    .V()
                //    .As("a")
                //    .Out()
                //    .As("b")
                //    .Match(
                //        GraphTraversal2.__().As("a").Out().Count().As("c"),
                //        GraphTraversal2.__().Not(GraphTraversal2.__().As("a").In().As("b")),
                //        GraphTraversal2.__()
                //            .Or(
                //                GraphTraversal2.__().As("a").Out("knows").As("b"),
                //                GraphTraversal2.__().As("b").In().Count().As("c")
                //            )
                //    )
                //    .Select("a", "b", "c")
                //    .By("name")
                //    .By("name")
                //    .By();

                //var result = traversal.Next();


                command.CommandText = "g.V().as('a').out().as('b').match(__.as('a').out().count().as('c'),__.not(__.as('a').in().as('b')),__.or(__.as('a').out('knows').as('b'),__.and(__.as('b').in().count().as('c'),__.as('c').is(gt(2))))).select('a','b','c').by('name').by('name').by()";

                var result = command.ExecuteAndGetResults();

                foreach (string r in result)
                {
                    Console.WriteLine(r);
                }
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
