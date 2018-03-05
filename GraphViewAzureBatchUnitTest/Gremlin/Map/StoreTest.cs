using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GraphViewAzureBatchUnitTest.Gremlin.Map
{
    [TestClass]
    public class StoreTest : AbstractAzureBatchGremlinTest
    {
        /// <summary>
        /// Port of the g_V_storeXa_nameX_out_capXaX UT from org/apache/tinkerpop/gremlin/process/traversal/step/sideEffect/StoreTest.java.
        /// Equivalent gremlin: "g.V().store('a').by('name').out().cap('a')"
        /// </summary>
        [TestMethod]
        public void VerticesStoreAByNameOutCapA()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                this.job.Traversal = graphCommand.g().V().Store("a").By("name").Out().Cap("a");
                var result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job).First().Trim('[', ']').Split(',').Select(r => r.Trim(' '));
                var expectedResult = new List<string> { "marko", "josh", "peter", "lop", "ripple", "vadas" };
                CheckUnOrderedResults(expectedResult, result);
            }
        }

        /// <summary>
        /// Port of the g_VX1X_storeXaX_byXnameX_out_storeXaX_byXnameX_name_capXaX UT from org/apache/tinkerpop/gremlin/process/traversal/step/sideEffect/StoreTest.java.
        /// Equivalent gremlin: "g.V(v1Id).store('a').by('name').out().store('a').by('name').name.cap('a')", "v1Id", v1Id
        /// </summary>
        /// <remarks>
        /// WorkItem: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/40056
        /// </remarks>
        [TestMethod]
        [TestCategory("Ignore")] // P0: regression
        public void VertexWithIdStoreAByNameOutStoreAByNameValuesNameCapA()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                string markoVertexId = this.ConvertToVertexId(graphCommand, "marko");
                this.job.Traversal = graphCommand.g().V(markoVertexId).Store("a").By("name").Out().Store("a").By("name").Values("name").Cap("a");

                var result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job).First().Trim('[', ']').Split(',').Select(r => r.Trim(' '));
                var expectedResult = new List<string> { "marko", "josh", "vadas", "lop" };
                CheckUnOrderedResults(expectedResult, result);
            }
        }

        /// <summary>
        /// Port of the g_V_storeXaX_byXoutEXcreatedX_countX_out_out_storeXaX_byXinEXcreatedX_weight_sumX UT from org/apache/tinkerpop/gremlin/process/traversal/step/sideEffect/StoreTest.java.
        /// Equivalent gremlin: "g.V.store('a').by(__.outE('created').count).out.out.store('a').by(__.inE('created').weight.sum).cap('a')"
        /// </summary>
        /// <remarks>
        /// WorkItem: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/40056
        /// </remarks>
        [TestMethod]
        [TestCategory("Ignore")] // P0: regression
        public void VerticesStoreAByOutECountOutOutStoreAByInECreatedValuesWeightSumCapA()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                this.job.Traversal = graphCommand.g().V().Store("a").By(GraphTraversal.__().OutE("created").Count()).Out().Out().Store("a").By(GraphTraversal.__().InE("created").Values("weight").Sum()).Cap("a");

                var result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job).First().Trim('[', ']').Split(',').Select(r => r.Trim(' '));
                var expectedResult = new List<string> { "2", "1", "1", "1", "1", "0", "0", "0" };
                CheckUnOrderedResults(expectedResult, result);
            }
        }
    }
}
