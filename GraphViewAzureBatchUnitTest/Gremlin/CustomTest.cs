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
            string query = "g.V().has('name', 'marko').emit(__.has('label', 'person')).repeat(__.out ()).values('name')";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string result in results)
            {
                Console.WriteLine(result);
            }
        }
    }
}