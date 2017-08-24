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
            const string COLLECTION_NAME = "GraphViewCollection";
            var connection = new GraphViewConnection(CONNECTION_STRING, COLLECTION_NAME, GraphType.GraphAPIOnly, false, null);
            GraphViewCommand command = new GraphViewCommand(connection);

//            // Reset collection
//            connection.ResetJsonServerCollection(COLLECTION_NAME);
//
//            // INIT
//            command.g().AddV("person").Property("id", "dummy").Property("name", "marko").Property("age", 29).Next();
//            command.g().AddV("person").Property("id", "特殊符号").Property("name", "vadas").Property("age", 27).Next();
//            command.g().AddV("software").Property("id", "这是一个中文ID").Property("name", "lop").Property("lang", "java").Next();
//            command.g().AddV("person").Property("id", "引号").Property("name", "josh").Property("age", 32).Next();
//            command.g().AddV("software").Property("id", "中文English").Property("name", "ripple").Property("lang", "java").Next();
//            command.g().AddV("person").Property("name", "peter").Property("age", 35).Next();
//            command.g().V().Has("name", "marko").AddE("knows").Property("weight", 0.5d).To(command.g().V().Has("name", "vadas")).Next();
//            command.g().V().Has("name", "marko").AddE("knows").Property("weight", 1.0d).To(command.g().V().Has("name", "josh")).Next();
//            command.g().V().Has("name", "marko").AddE("created").Property("weight", 0.4d).To(command.g().V().Has("name", "lop")).Next();
//            command.g().V().Has("name", "josh").AddE("created").Property("weight", 1.0d).To(command.g().V().Has("name", "ripple")).Next();
//            command.g().V().Has("name", "josh").AddE("created").Property("weight", 0.4d).To(command.g().V().Has("name", "lop")).Next();
//            command.g().V().Has("name", "peter").AddE("created").Property("weight", 0.2d).To(command.g().V().Has("name", "lop")).Next();


            var traversal = command.g().V().Has("name", "josh").OutE("created");
            var result = traversal.Next();
            foreach (string s in result)
            {
                Console.WriteLine(s);
            }
        }
    }
}
