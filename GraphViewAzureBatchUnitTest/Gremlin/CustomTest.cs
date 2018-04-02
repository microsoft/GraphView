using System;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewAzureBatchUnitTest.Gremlin
{
    [TestClass]
    public class CustomTest : AbstractAzureBatchGremlinTest
    {
        [TestMethod]
        public void Test1()
        {
            //this.job.Query = "g.V()";
            this.job.Traversal = this.job.Command.g().V();

            List<string> results = this.jobManager.TestQuery(this.job);

            Console.WriteLine("-------------Test Result-------------");
            foreach (string result in results)
            {
                Console.WriteLine(result);
            }
        }
    }
}