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
            var connection = new GraphViewConnection(CONNECTION_STRING, GraphType.GraphAPIOnly, false, null);
        }
    }
}
