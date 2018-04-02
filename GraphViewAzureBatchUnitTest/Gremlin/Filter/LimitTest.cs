using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewAzureBatchUnitTest.Gremlin.Filter
{

    [TestClass]
    public class LimitTest : AbstractAzureBatchGremlinTest
    {
        [TestMethod]
        public void Limit()
        {
            using (GraphViewCommand graphCommand = this.job.Command)
            {
                this.job.Traversal = graphCommand.g().V().Limit(1);
                var result = this.jobManager.TestQuery(this.job);

                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }

                Assert.AreEqual(1, result.Count);
            }
        }

        /// <summary>
        /// The result of this test should be different from single-mode result. And the result is related about parallelism.
        /// </summary>
        [TestMethod]
        public void StoreLimit()
        {
            using (GraphViewCommand graphCommand = this.job.Command)
            {
                this.job.Traversal = graphCommand.g().V().Store("x").Limit(1).Cap("x").Unfold();
                var result = this.jobManager.TestQuery(this.job);

                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
            }
        }

        [TestMethod]
        public void LimitStore()
        {
            using (GraphViewCommand graphCommand = this.job.Command)
            {
                this.job.Traversal = graphCommand.g().V().Limit(1).Store("x").Cap("x").Unfold();
                var result = this.jobManager.TestQuery(this.job);

                Assert.AreEqual(1, result.Count);
            }
        }

        [TestMethod]
        public void AggregateLimit()
        {
            using (GraphViewCommand graphCommand = this.job.Command)
            {
                this.job.Traversal = graphCommand.g().V().Aggregate("x").Limit(1).Cap("x").Unfold();
                var result = this.jobManager.TestQuery(this.job);

                Assert.AreEqual(6, result.Count);
            }
        }

        [TestMethod]
        public void LimitAggregate()
        {
            using (GraphViewCommand graphCommand = this.job.Command)
            {
                this.job.Traversal = graphCommand.g().V().Limit(1).Aggregate("x").Cap("x").Unfold();
                var result = this.jobManager.TestQuery(this.job);

                Assert.AreEqual(1, result.Count);
            }
        }
    }
}
