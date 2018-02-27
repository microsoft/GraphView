using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GraphViewAzureBatchUnitTest.Gremlin.Filter
{
    [TestClass]
    public class AndTest : AbstractAzureBatchGremlinTest
    {
        /// <summary>
        /// g_V_andXhasXage_gt_27X__outE_count_gt_2X_name()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/AndTest.java
        /// Gremlin: g.V().and(has("age", P.gt(27)), outE().count().is(P.gte(2l))).values("name");
        /// </summary>
        [TestMethod]
        public void AndWithParameters()
        {
            string query =
                "g.V().and(__.has('age', gt(27)), __.outE().count().is(gte(2l))).values('name')";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            CheckUnOrderedResults(new string[] { "marko", "josh" }, results);
        }

        /// <summary>
        /// g_V_andXout__hasXlabel_personX_and_hasXage_gte_32XX_name()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/AndTest.java
        /// Gremlin: g.V().and(outE(), has(T.label, "person").and().has("age", P.gte(32))).values("name");
        /// </summary>
        [TestMethod]
        public void AndAsInfixNotation()
        {
            string query =
                "g.V().and(__.outE(), __.hasLabel('person').and().has('age', gte(32))).values('name')";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            CheckUnOrderedResults(new string[] { "josh", "peter" }, results);
        }

        ///// <summary>
        ///// g_V_asXaX_outXknowsX_and_outXcreatedX_inXcreatedX_asXaX_name()
        ///// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/AndTest.java
        ///// Gremlin: g.V().as("a").out("knows").and().out("created").in("created").as("a").values("name");
        ///// </summary>
        [TestMethod]
        public void AndWithAs()
        {
            string query =
                "g.V().as('a').out('knows').and().out('created').in('created').as('a').values('name')";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.IsTrue(results.Count == 1);
            Assert.IsTrue(results[0] == "v[dummy]");
        }

        ///// <summary>
        ///// g_V_asXaX_andXselectXaX_selectXaXX()
        ///// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/AndTest.java
        ///// Gremlin: g.V().as("a").and(select("a"), select("a"));
        ///// </summary>
        [TestMethod]
        public void AndWithSelect()
        {
            string query =
                "g.V().as('a').and(__.select('a'), __.select('a'))";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.AreEqual(6, results.Count);
        }
    }
}
