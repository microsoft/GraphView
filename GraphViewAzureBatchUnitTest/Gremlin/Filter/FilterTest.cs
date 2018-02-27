using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewAzureBatchUnitTest.Gremlin.Filter
{
    [TestClass]
    public class FilterTest : AbstractAzureBatchGremlinTest
    {
        [TestMethod]
        public void AggregateHas()
        {
            string query = "g.V().aggregate('x').has('age').cap('x').unfold()";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.AreEqual(6, results.Count);
        }

        [TestMethod]
        public void HasAggregate()
        {
            string query = "g.V().has('age').aggregate('x').cap('x').unfold()";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.AreEqual(4, results.Count);
        }

        [TestMethod]
        public void AggregateWhere()
        {
            string query = "g.V().out('created').in('created').aggregate('x').where(__.out('created').count().is(gt(1))).values('name').cap('x').unfold()";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.AreEqual(10, results.Count);
        }

        [TestMethod]
        public void WhereAggregate()
        {
            string query = "g.V().out('created').in('created').where(__.out('created').count().is(gt(1))).aggregate('x').values('name').cap('x').unfold()";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.AreEqual(4, results.Count);
        }

        [TestMethod]
        public void AggregateIs()
        {
            string query = "g.V().values('age').aggregate('x').is(lte(30)).cap('x').unfold()";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.AreEqual(4, results.Count);
        }

        [TestMethod]
        public void IsAggregate()
        {
            string query = "g.V().values('age').is(lte(30)).aggregate('x').cap('x').unfold()";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.AreEqual(2, results.Count);
        }

        [TestMethod]
        public void AggregateNot()
        {
            string query = "g.V().aggregate('x').not(__.hasLabel('person')).cap('x').unfold()";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.AreEqual(6, results.Count);
        }

        [TestMethod]
        public void NotAggregate()
        {
            string query = "g.V().not(__.hasLabel('person')).aggregate('x').cap('x').unfold()";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.AreEqual(2, results.Count);
        }

        [TestMethod]
        public void AggregateAnd()
        {
            string query = "g.V().aggregate('x').and(__.outE('knows'), __.values('age').is(lte(30))).cap('x').unfold()";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.AreEqual(6, results.Count);
        }

        [TestMethod]
        public void AndAggregate()
        {
            string query = "g.V().and(__.outE('knows'), __.values('age').is(lte(30))).aggregate('x').cap('x').unfold()";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.AreEqual(1, results.Count);
        }

        [TestMethod]
        public void AggregateOr()
        {
            string query = "g.V().aggregate('x').or(__.outE('created'), __.inE('created').count().is(gt(1))).cap('x').unfold()";
            //string query = "g.V().or(__.outE('created'), __.inE('created').count().is(gt(1)))";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.AreEqual(6, results.Count);
        }

        [TestMethod]
        public void OrAggregate()
        {
            string query = "g.V().or(__.outE('created'), __.inE('created').count().is(gt(1))).aggregate('x').cap('x').unfold()";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.AreEqual(4, results.Count);
        }

        [TestMethod]
        public void StoreHas()
        {
            string query = "g.V().store('x').has('age').cap('x').unfold()";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.AreEqual(6, results.Count);
        }

        [TestMethod]
        public void HasStore()
        {
            string query = "g.V().has('age').store('x').cap('x').unfold()";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.AreEqual(4, results.Count);
        }

        [TestMethod]
        public void StoreWhere()
        {
            string query = "g.V().out('created').in('created').store('x').where(__.out('created').count().is(gt(1))).values('name').cap('x').unfold()";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.AreEqual(10, results.Count);
        }

        [TestMethod]
        public void WhereStore()
        {
            string query = "g.V().out('created').in('created').where(__.out('created').count().is(gt(1))).store('x').values('name').cap('x').unfold()";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.AreEqual(4, results.Count);
        }

        [TestMethod]
        public void StoreIs()
        {
            string query = "g.V().values('age').store('x').is(lte(30)).cap('x').unfold()";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.AreEqual(4, results.Count);
        }

        [TestMethod]
        public void IsStore()
        {
            string query = "g.V().values('age').is(lte(30)).store('x').cap('x').unfold()";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.AreEqual(2, results.Count);
        }

        [TestMethod]
        public void StoreNot()
        {
            string query = "g.V().store('x').not(__.hasLabel('person')).cap('x').unfold()";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.AreEqual(6, results.Count);
        }

        [TestMethod]
        public void NotStore()
        {
            string query = "g.V().not(__.hasLabel('person')).store('x').cap('x').unfold()";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.AreEqual(2, results.Count);
        }

        [TestMethod]
        public void StoreAnd()
        {
            string query = "g.V().store('x').and(__.outE('knows'), __.values('age').is(lte(30))).cap('x').unfold()";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.AreEqual(6, results.Count);
        }

        [TestMethod]
        public void AndStore()
        {
            string query = "g.V().and(__.outE('knows'), __.values('age').is(lte(30))).store('x').cap('x').unfold()";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.AreEqual(1, results.Count);
        }

        [TestMethod]
        public void StoreOr()
        {
            string query = "g.V().store('x').or(__.outE('created'), __.inE('created').count().is(gt(1))).cap('x').unfold()";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.AreEqual(6, results.Count);
        }

        [TestMethod]
        public void OrStore()
        {
            string query = "g.V().or(__.outE('created'), __.inE('created').count().is(gt(1))).store('x').cap('x').unfold()";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.AreEqual(4, results.Count);
        }

        [TestMethod]
        public void FlatMapFilter()
        {
            string query = "g.V().flatMap(__.aggregate('x').has('age')).cap('x').unfold()";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.AreEqual(6, results.Count);
        }



        [TestMethod]
        public void UnionFilter()
        {
            string query = "g.V().union(__.aggregate('x').has('age'), __.has('age').aggregate('x')).cap('x').unfold()";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.AreEqual(10, results.Count);
        }

        [TestMethod]
        public void FilterPath()
        {
            string query = "g.V().aggregate('x').has('lang').path()";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
        }
    }
}
