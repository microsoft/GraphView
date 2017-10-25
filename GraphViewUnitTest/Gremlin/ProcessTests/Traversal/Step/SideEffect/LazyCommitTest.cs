using System;
using System.Linq;
using System.Reflection;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GraphViewUnitTest.Gremlin
{
    [TestClass]
    public class LazyCommitTest : AbstractGremlinTest
    {

        [TestMethod]
        public void Test_Lazy_AddV()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                var traversal = command.g().V().OutE("knows").AddV().Property("name", "nothing").Has("name", "nothing").Commit();
                var result = traversal.Next();
                Assert.IsTrue(result.Count == 2);

                Console.WriteLine("g.V().outE('knows').addV().property('name','nothing').has('name','nothing') :");
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
            }
        }

        [TestMethod]
        public void Test_Lazy_AddE()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                var traversal = command.g().V().Has("name", "marko").As("a").Out("knows")
                    .AddE("newEdge").From("a").Property("year", "2017").Commit().InV().InE("newEdge").Commit().Values("year");
                var result = traversal.Next();
                Assert.IsTrue(result.Count == 2);
                Assert.IsTrue(result[0] == "2017");
                Assert.IsTrue(result[1] == "2017");

                Console.WriteLine("g.V().has('name','marko').as('a').out('knows').addE('newEdge').from('a').property('year', '2017').inV().inE('newEdge').commit().values('year') :");
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
            }
        }

        [TestMethod]
        public void Test_Lazy_AddProperty()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                var traversal = command.g().V().Has("name", "marko").Union(
                    GraphTraversal.__().Property("city", "beijing").Property("age", "20").Commit().ValueMap(),
                    GraphTraversal.__().OutE("created").Property("number", "10").ValueMap()).Commit();
                var result = traversal.Next();
                Assert.IsTrue(result.Count == 2);
                Assert.IsTrue(result[0].Contains("name:[marko]"));
                Assert.IsTrue(result[0].Contains("age:[20]"));
                Assert.IsTrue(result[0].Contains("city:[beijing]"));
                Assert.IsTrue(result[1].Contains("weight:0.4"));
                Assert.IsTrue(result[1].Contains("number:10"));

                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
            }
        }

        [TestMethod]
        public void Test_Lazy_Drop()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                var traversal = command.g().Inject(1).Union(
                    GraphTraversal.__().V().Has("name", "vadas").Drop(),
                    GraphTraversal.__().V().Has("name", "marko").OutE("created").Drop(),
                    GraphTraversal.__().V().Has("name", "marko").Properties("age").Drop(),
                    GraphTraversal.__().V().Has("name", "peter").OutE().Properties("weight").Drop().Commit(),
                    GraphTraversal.__().V().Count(),
                    GraphTraversal.__().V().OutE().Count(),
                    GraphTraversal.__().V().Has("name", "marko").ValueMap(),
                    GraphTraversal.__().V().Has("name", "peter").OutE().ValueMap());
                // =>5
                // =>4
                // =>[name:[marko]]
                // =>[label:created]  or  []

                var result = traversal.Next();
                Assert.IsTrue(result.Count == 4);
                Assert.IsTrue(result[0] == "5");
                Assert.IsTrue(result[1] == "4");
                Assert.IsTrue(result[2] == "[name:[marko]]");
                Assert.IsTrue(!result[3].Contains("weight"));
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
            }
        }

        [TestMethod]
        public void Test_Lazy_Update_Vertex_Properties()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                var traversal = command.g().Inject(1).AddV()
                    .Property("name", "testNode")
                    .Property("grade", "100")
                    .Property("p1", "v1")
                    .Property("fatherProperty", "fatherValue", "name1", "value1", "name2", "value2")
                    .Union(
                        GraphTraversal.__().Properties("p1").Drop(),
                        GraphTraversal.__().Properties("fatherProperty").Properties().HasKey("name1").Drop(),
                        GraphTraversal.__().PropertyMap(),
                        GraphTraversal.__().Properties("fatherProperty").Properties()).Commit();
                    ;

                var result = traversal.Next();

                Assert.IsTrue(result.Count == 2);
                Assert.IsTrue(result[0].Contains("name:[vp[name->testNode]]"));
                Assert.IsTrue(result[0].Contains("grade:[vp[grade->100]]"));
                Assert.IsTrue(result[0].Contains("fatherProperty:[vp[fatherProperty->fatherValue]]"));
                Assert.IsTrue(!result[0].Contains("p1"));
                Assert.IsTrue(result[1].Contains("p[name2->value2]"));

                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
            }
        }


        [TestMethod]
        public void Test_Lazy_Update_Edge_Properties()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                var traversal = command.g().V().Has("name", "peter").OutE()
                    .Property("weight", "1")
                    .Property("grade", "100")
                    .Property("p1", "v1")
                    .Union(
                        GraphTraversal.__().Properties("p1").Drop(),
                        GraphTraversal.__().Property("p1", "v2"),
                        GraphTraversal.__().PropertyMap()).Commit();

                var result = traversal.Next();

                Assert.IsTrue(result.Count == 2);
                Assert.IsTrue(result[1].Contains("weight:p[weight->1]"));
                Assert.IsTrue(result[1].Contains("grade:p[grade->100]"));
                Assert.IsTrue(result[1].Contains("p1:p[p1->v2]"));
                Assert.IsTrue(!result[1].Contains("p1->v1"));

                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
            }
        }

    }
}
