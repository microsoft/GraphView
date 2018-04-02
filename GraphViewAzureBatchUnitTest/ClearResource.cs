using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphViewAzureBatchUnitTest.Gremlin;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewAzureBatchUnitTest
{
    // For Debug. Clear AzureBatch resources.
    [TestClass]
    public class ClearResource : AbstractAzureBatchGremlinTest
    {
        // For Debug.
        [TestMethod]
        public void Clear()
        {
            this.jobManager.ClearResource();
        }
    }
}
