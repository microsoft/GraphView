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
    public class SampleTest : AbstractGremlinTest
    {
        // case: collectionField
        [TestMethod]
        public void Test_Fold_Sample_Local()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                var traversal = command.g().V().Fold().Sample(GremlinKeyword.Scope.Local, 1).Unfold().Values("name");
                var result = traversal.Next();
                Assert.IsTrue(result.Count == 1);

                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
            }
        }

        // case: MapField
        [TestMethod]
        public void Test_Map_Sample_Local()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                var traversal = command.g().V().GroupCount().By("name").Sample(GremlinKeyword.Scope.Local, 1);
                var result = traversal.Next();
                Assert.IsTrue(result.Count == 1);

                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
            }
        }

        [TestMethod]
        public void Test_Aggregate_Sample_Local()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                var traversal = command.g().V().Aggregate("a").Select("a").Sample(GremlinKeyword.Scope.Local, 1);
                var result = traversal.Next();
                Assert.IsTrue(result.Count == 6);
                foreach (string row in result)
                {
                    Assert.IsTrue(!row.Contains(","));
                }
                
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
            }
        }

        // case : PathField
        [TestMethod]
        public void Test_Path_Sample_Local()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                var traversal = command.g().V().Out().Path().Sample(GremlinKeyword.Scope.Local, 1);
                var result = traversal.Next();
                Assert.IsTrue(result.Count == 6);
                foreach (string row in result)
                {
                    Assert.IsTrue(!row.Contains(","));
                }

                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
            }
        }


        [TestMethod]
        public void Test_Inject_Sample_Local()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                var traversal = command.g().Inject(1).Sample(GremlinKeyword.Scope.Local, 1);
                var result = traversal.Next();
                Assert.IsTrue(result.Count == 1);

                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
            }
        }

        [TestMethod]
        public void Test_Sample_By()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                var traversal = command.g().V().OutE("knows").Property("weight", "20");
                var result = traversal.Next();

                traversal = command.g().V().OutE().Sample(2).By("weight");
                result = traversal.Next();
                Assert.IsTrue(result.Count == 2);


                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
            }
        }
    }
}
