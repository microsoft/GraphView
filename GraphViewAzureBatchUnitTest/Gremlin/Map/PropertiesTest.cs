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
    public class PropertiesTest : AbstractAzureBatchGremlinTest
    {

        /// <summary>
        /// Port of the g_V_hasXageX_properties_hasXid_nameIdX_value UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/PropertiesTest.java.
        /// Equivalent gremlin: "g.V.has('age').properties().has(T.id, nameId).value()", "nameId", nameId
        /// </summary>
        [TestMethod]
        public void VerticesHasAgePropertiesHasIdNameIdValue()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                string markoNameVertexPropertyId = this.ConvertToPropertyId(graphCommand, "marko", "name", "marko");

                this.job.Traversal = graphCommand.g().V().Has("age").Properties().Has("id", markoNameVertexPropertyId).Value();

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);

                Assert.AreEqual(1, result.Count);
                Assert.AreEqual("marko", result.First());
            }
        }

        /// <summary>
        /// Port of the g_V_hasXageX_propertiesXnameX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/PropertiesTest.java.
        /// Equivalent gremlin: "g.V.has('age').properties('name')"
        /// </summary>
        /// <remarks>
        /// NOTE: original test also does asserts on Vertex Property Ids, but since we do not support Vertex Property Ids, I've skipped doing these asserts.
        /// </remarks>
        [TestMethod]
        public void VerticesHasAgePropertiesName()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                this.job.Traversal = graphCommand.g().V().Has("age").Properties("name");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);

                Assert.AreEqual(4, result.Count);

                List<string> expected = new List<string> { "vp[name->marko]", "vp[name->vadas]", "vp[name->josh]", "vp[name->peter]" };
                CheckUnOrderedResults(expected, result);
            }
        }
    }
}
