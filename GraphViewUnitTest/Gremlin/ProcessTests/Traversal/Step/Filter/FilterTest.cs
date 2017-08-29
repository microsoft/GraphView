using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewUnitTest.Gremlin.ProcessTests.Traversal.Step.Filter
{
    [TestClass]
    public class FilterTest : AbstractGremlinTest
    {
        [TestMethod]
        public void AggregateHas()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                GraphTraversal traversal = graphCommand.g().V().Aggregate("x").Has("age").Cap("x").Unfold();
                List<string> result = traversal.Next();
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
                Assert.AreEqual(6, result.Count);
            }
        }

        [TestMethod]
        public void HasAggregate()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                GraphTraversal traversal = graphCommand.g().V().Has("age").Aggregate("x").Cap("x").Unfold();
                List<string> result = traversal.Next();
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
                Assert.AreEqual(4, result.Count);
            }
        }

        [TestMethod]
        public void AggregateWhere()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                GraphTraversal traversal = graphCommand.g().V().Out("created").In("created")
                    .Aggregate("x").Where(GraphTraversal.__().Out("created").Count().Is(Predicate.gt(1))).Values("name")
                    .Cap("x").Unfold();
                List<string> result = traversal.Next();
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
                Assert.AreEqual(10, result.Count);
            }
        }

        [TestMethod]
        public void WhereAggregate()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                GraphTraversal traversal = graphCommand.g().V().Out("created").In("created")
                    .Where(GraphTraversal.__().Out("created").Count().Is(Predicate.gt(1))).Aggregate("x").Values("name")
                    .Cap("x").Unfold();
                List<string> result = traversal.Next();
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
                Assert.AreEqual(4, result.Count);
            }
        }

        [TestMethod]
        public void AggregateIs()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                GraphTraversal traversal = graphCommand.g().V().Values("age").Aggregate("x").Is(Predicate.lte(30)).Cap("x").Unfold();
                List<string> result = traversal.Next();
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
                Assert.AreEqual(4, result.Count);
            }
        }

        [TestMethod]
        public void IsAggregate()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                GraphTraversal traversal = graphCommand.g().V().Values("age").Is(Predicate.lte(30)).Aggregate("x").Cap("x").Unfold();
                List<string> result = traversal.Next();
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
                Assert.AreEqual(2, result.Count);
            }
        }

        [TestMethod]
        public void AggregateNot()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                GraphTraversal traversal = graphCommand.g().V().Aggregate("x").Not(GraphTraversal.__().HasLabel("person")).Cap("x").Unfold();
                List<string> result = traversal.Next();
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
                Assert.AreEqual(6, result.Count);
            }
        }

        [TestMethod]
        public void NotAggregate()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                GraphTraversal traversal = graphCommand.g().V().Not(GraphTraversal.__().HasLabel("person")).Aggregate("x").Cap("x").Unfold();
                List<string> result = traversal.Next();
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
                Assert.AreEqual(2, result.Count);
            }
        }

        [TestMethod]
        public void AggregateAnd()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                GraphTraversal traversal = graphCommand.g().V().Aggregate("x")
                    .And(GraphTraversal.__().OutE("knows"), GraphTraversal.__().Values("age").Is(Predicate.lte(30)))
                    .Cap("x").Unfold();
                List<string> result = traversal.Next();
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
                Assert.AreEqual(6, result.Count);
            }
        }

        [TestMethod]
        public void AndAggregate()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                GraphTraversal traversal = graphCommand.g().V()
                    .And(GraphTraversal.__().OutE("knows"),
                        GraphTraversal.__().Values("age").Is(Predicate.lte(30))).Aggregate("x").Cap("x").Unfold();
                List<string> result = traversal.Next();
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
                Assert.AreEqual(1, result.Count);
            }
        }

        [TestMethod]
        public void AggregateOr()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                GraphTraversal traversal = graphCommand.g().V().Aggregate("x")
                    .Or(GraphTraversal.__().OutE("created"),
                        GraphTraversal.__().InE("created").Count().Is(Predicate.gt(1))).Cap("x").Unfold();
                List<string> result = traversal.Next();
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
                Assert.AreEqual(6, result.Count);
            }
        }

        [TestMethod]
        public void OrAggregate()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                GraphTraversal traversal = graphCommand.g().V()
                    .Or(GraphTraversal.__().OutE("created"),
                        GraphTraversal.__().InE("created").Count().Is(Predicate.gt(1))).Aggregate("x").Cap("x")
                    .Unfold();
                List<string> result = traversal.Next();
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
                Assert.AreEqual(4, result.Count);
            }
        }

        //----

        [TestMethod]
        public void StoreHas()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                GraphTraversal traversal = graphCommand.g().V().Store("x").Has("age").Cap("x").Unfold();
                List<string> result = traversal.Next();
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
                Assert.AreEqual(6, result.Count);
            }
        }

        [TestMethod]
        public void HasStore()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                GraphTraversal traversal = graphCommand.g().V().Has("age").Store("x").Cap("x").Unfold();
                List<string> result = traversal.Next();
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
                Assert.AreEqual(4, result.Count);
            }
        }

        [TestMethod]
        public void StoreWhere()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                GraphTraversal traversal = graphCommand.g().V().Out("created").In("created")
                    .Store("x").Where(GraphTraversal.__().Out("created").Count().Is(Predicate.gt(1))).Values("name")
                    .Cap("x").Unfold();
                List<string> result = traversal.Next();
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
                Assert.AreEqual(10, result.Count);
            }
        }

        [TestMethod]
        public void WhereStore()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                GraphTraversal traversal = graphCommand.g().V().Out("created").In("created")
                    .Where(GraphTraversal.__().Out("created").Count().Is(Predicate.gt(1))).Store("x").Values("name")
                    .Cap("x").Unfold();
                List<string> result = traversal.Next();
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
                Assert.AreEqual(4, result.Count);
            }
        }

        [TestMethod]
        public void StoreIs()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                GraphTraversal traversal = graphCommand.g().V().Values("age").Store("x").Is(Predicate.lte(30)).Cap("x").Unfold();
                List<string> result = traversal.Next();
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
                Assert.AreEqual(4, result.Count);
            }
        }

        [TestMethod]
        public void IsStore()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                GraphTraversal traversal = graphCommand.g().V().Values("age").Is(Predicate.lte(30)).Store("x").Cap("x").Unfold();
                List<string> result = traversal.Next();
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
                Assert.AreEqual(2, result.Count);
            }
        }

        [TestMethod]
        public void StoreNot()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                GraphTraversal traversal = graphCommand.g().V().Store("x").Not(GraphTraversal.__().HasLabel("person")).Cap("x").Unfold();
                List<string> result = traversal.Next();
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
                Assert.AreEqual(6, result.Count);
            }
        }

        [TestMethod]
        public void NotStore()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                GraphTraversal traversal = graphCommand.g().V().Not(GraphTraversal.__().HasLabel("person")).Store("x").Cap("x").Unfold();
                List<string> result = traversal.Next();
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
                Assert.AreEqual(2, result.Count);
            }
        }

        [TestMethod]
        public void StoreAnd()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                GraphTraversal traversal = graphCommand.g().V().Store("x")
                    .And(GraphTraversal.__().OutE("knows"), GraphTraversal.__().Values("age").Is(Predicate.lte(30)))
                    .Cap("x").Unfold();
                List<string> result = traversal.Next();
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
                Assert.AreEqual(6, result.Count);
            }
        }

        [TestMethod]
        public void AndStore()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                GraphTraversal traversal = graphCommand.g().V()
                    .And(GraphTraversal.__().OutE("knows"),
                        GraphTraversal.__().Values("age").Is(Predicate.lte(30))).Store("x").Cap("x").Unfold();
                List<string> result = traversal.Next();
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
                Assert.AreEqual(1, result.Count);
            }
        }

        [TestMethod]
        public void StoreOr()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                GraphTraversal traversal = graphCommand.g().V().Store("x")
                    .Or(GraphTraversal.__().OutE("created"),
                        GraphTraversal.__().InE("created").Count().Is(Predicate.gt(1))).Cap("x").Unfold();
                List<string> result = traversal.Next();
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
                Assert.AreEqual(6, result.Count);
            }
        }

        [TestMethod]
        public void OrStore()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                GraphTraversal traversal = graphCommand.g().V()
                    .Or(GraphTraversal.__().OutE("created"),
                        GraphTraversal.__().InE("created").Count().Is(Predicate.gt(1))).Store("x").Cap("x").Unfold();
                List<string> result = traversal.Next();
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
                Assert.AreEqual(4, result.Count);
            }
        }

        [TestMethod]
        public void FlatMapFilter()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                GraphTraversal traversal = graphCommand.g().V().FlatMap(GraphTraversal.__().Aggregate("x").Has("age")).Cap("x").Unfold();
                List<string> result = traversal.Next();
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
                Assert.AreEqual(6, result.Count);
            }
        }



        [TestMethod]
        public void UnionFilter()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                GraphTraversal traversal = graphCommand.g()
                    .V()
                    .Union(GraphTraversal.__().Aggregate("x").Has("age"), GraphTraversal.__().Has("age").Aggregate("x"))
                    .Cap("x")
                    .Unfold();
                List<string> result = traversal.Next();
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
                Assert.AreEqual(10, result.Count);
            }
        }

        [TestMethod]
        public void FilterPath()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g().V().Aggregate("x").Has("lang").Path();
                var result = traversal.Next();

                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
            }
        }
    }
}
