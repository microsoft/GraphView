using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView;

namespace GraphViewUnitTest
{
    [TestClass]
    public class JsonServerTestFirst
    {
        [TestMethod]
        public void InitTest()
        {
            const string CONNECTION_STRING = "Data Source = (local); Initial Catalog = JsonTesting; Integrated Security = true;";
            var connection = new GraphViewConnection(CONNECTION_STRING, "GraphViewCollection", GraphType.GraphAPIOnly, false, null);
            GraphViewCommand command = new GraphViewCommand(connection);
            var traversal = command.g().Inject(0).AddV();
            var result = traversal.Next();
            foreach (string s in result)
            {
                Console.WriteLine(s);
            }
        }
    }
}
