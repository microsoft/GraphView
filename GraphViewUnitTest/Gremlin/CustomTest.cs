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
                var traversal = command.g().Inject(0).AddV("123").Property("name", "456").SideEffect(GraphTraversal.__().AddE("789").To(GraphTraversal.__()));
                var result = traversal.Next();
                foreach (var r in result)
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
                var traversal = command.g().V().Optional(GraphTraversal.__().OutE()).Union().Out();
                var result = traversal.Next();
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
                Debugger.Break();
            }
        }
    }
}
