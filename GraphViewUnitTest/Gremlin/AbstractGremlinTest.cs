#define TEST_ON_DOCUMENT_DB
//#define TEST_ON_JSONSERVER

// !!! Important, change the same define in GraphTraversal.cs at the same time.

using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Reflection;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GraphViewUnitTest.Gremlin
{
    [AttributeUsage(AttributeTargets.Method)]
    public class TestModernCompatibleAttribute : Attribute
    {
        public bool InitialFlat { get; }

        public TestModernCompatibleAttribute(bool initialFlat = true)
        {
            this.InitialFlat = initialFlat;
        }
    }
    /// <summary>
    /// Abstract test class that contains helper methods, and common setup/cleanup.
    /// </summary>
    [TestClass]
    public class AbstractGremlinTest
    {
        internal const bool TEST_USE_REVERSE_EDGE = true;

#if TEST_PARTITION_BY_NAME
        internal const string TEST_PARTITION_BY_KEY = "name";
#elif TEST_PARTITION_BY_LABEL
        internal const string TEST_PARTITION_BY_KEY = "label";
#else
        "Can't compile me!"
#endif

        internal const int TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI = 1;

        protected static GraphViewConnection graphConnection;

        public TestContext TestContext { get; set; }

        /// <summary>
        /// Do any necessary setup.
        /// </summary>
#if TEST_ON_DOCUMENT_DB
        [TestInitialize]
        public void Setup()
        {
            //string endpoint = ConfigurationManager.AppSettings["DocDBEndPoint"];
            string endpoint = ConfigurationManager.AppSettings["DocDBEndPointLocal"];
            //string authKey = ConfigurationManager.AppSettings["DocDBKey"];
            string authKey = ConfigurationManager.AppSettings["DocDBKeyLocal"];
            string databaseId = ConfigurationManager.AppSettings["DocDBDatabaseGremlin"];

            // You can switch data on demand. And if you change the collection Id, you also need to change the parameter of 
            // GraphDataLoader.LoadGraphData. You can find these data in GraphDataLoader.cs.

            // ClassicGraphData
            // string collectionId = ConfigurationManager.AppSettings["DocDBCollectionClassic"];

            // ModernGraphData
            string collectionId = ConfigurationManager.AppSettings["DocDBCollectionModern"];

            Type classType = Type.GetType(TestContext.FullyQualifiedTestClassName);
            MethodInfo method = classType.GetMethod(TestContext.TestName, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
            TestModernCompatibleAttribute attr = method.GetCustomAttribute<TestModernCompatibleAttribute>();
            if (attr != null && attr.InitialFlat) {
                Console.WriteLine($"[{TestContext.TestName}] Convert the graph to flat!");

                GraphDataLoader.ResetToCompatibleData_Modern(endpoint, authKey, databaseId, collectionId, TEST_USE_REVERSE_EDGE);

                graphConnection = new GraphViewConnection(
                    endpoint, authKey, databaseId, collectionId,
                    GraphType.CompatibleOnly,
                    edgeSpillThreshold: 1,
                    useReverseEdges: TEST_USE_REVERSE_EDGE,
                    partitionByKeyIfViaGraphAPI: null
                );

            }
            else {
                Console.WriteLine($"[{TestContext.TestName}] Via Graph API!");

                // ClassicGraphData
                // GraphDataLoader.LoadGraphData(GraphData.CLASSIC);

                // ModernGraphData
                GraphDataLoader.LoadGraphData(GraphData.MODERN);

                graphConnection = new GraphViewConnection(
                    endpoint, authKey, databaseId, collectionId,
                    GraphType.GraphAPIOnly,
                    edgeSpillThreshold: AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI,
                    useReverseEdges: TEST_USE_REVERSE_EDGE,
                    partitionByKeyIfViaGraphAPI: TEST_PARTITION_BY_KEY
                );
            }
        }
#elif TEST_ON_JSONSERVER
        [TestInitialize]
        public void Setup()
        {
            const string CONNECTION_STRING = "Data Source = (local); Initial Catalog = JsonTesting; Integrated Security = true;";
            const string COLLECTION_NAME = "GraphViewCollection";

            Type classType = Type.GetType(TestContext.FullyQualifiedTestClassName);
            MethodInfo method = classType.GetMethod(TestContext.TestName, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
            TestModernCompatibleAttribute attr = method.GetCustomAttribute<TestModernCompatibleAttribute>();
            if (attr != null && attr.InitialFlat)
            {
                graphConnection = new GraphViewConnection(CONNECTION_STRING, COLLECTION_NAME, GraphType.GraphAPIOnly, true, 1, "/name", null);
                // Reset collection
                graphConnection.ResetJsonServerCollection(COLLECTION_NAME);

                graphConnection.JsonServerClient.InsertJson("{\"label\":\"person\",\"id\":\"dummy\",\"name\":\"marko\",\"age\":29,\"_etag\":\"_etag_1\",\"_edge\": [\"dummy\"],\"_edgeSpilled\": true,\"_revEdgeSpilled\": true,\"_reverse_edge\": [\"dummy\"]}", COLLECTION_NAME);
                graphConnection.JsonServerClient.InsertJson("{\"label\":\"person\",\"id\":\"特殊符号\",\"name\":\"vadas\",\"age\":27,\"_etag\":\"_etag_2\",\"_edge\": [\"dummy\"],\"_edgeSpilled\": true,\"_revEdgeSpilled\": true,\"_reverse_edge\": [\"dummy\"]}", COLLECTION_NAME);
                graphConnection.JsonServerClient.InsertJson("{\"label\":\"software\",\"id\":\"这是一个中文ID\",\"name\":\"lop\",\"lang\":\"java\",\"_etag\":\"_etag_3\",\"_edge\": [\"dummy\"],\"_edgeSpilled\": true,\"_revEdgeSpilled\": true,\"_reverse_edge\": [\"dummy\"]}", COLLECTION_NAME);
                graphConnection.JsonServerClient.InsertJson("{\"label\":\"person\",\"id\":\"引号\",\"name\":\"josh\",\"age\":32,\"_etag\":\"_etag_4\",\"_edge\": [\"dummy\"],\"_edgeSpilled\": true,\"_revEdgeSpilled\": true,\"_reverse_edge\": [\"dummy\"]}", COLLECTION_NAME);
                graphConnection.JsonServerClient.InsertJson("{\"label\":\"software\",\"id\":\"中文English\",\"name\":\"ripple\",\"lang\":\"java\",\"_etag\":\"_etag_5\",\"_edge\": [\"dummy\"],\"_edgeSpilled\": true,\"_revEdgeSpilled\": true,\"_reverse_edge\": [\"dummy\"]}", COLLECTION_NAME);
                graphConnection.JsonServerClient.InsertJson("{\"label\":\"person\",\"name\":\"peter\",\"age\":35,\"id\":\"ID_1007\",\"_etag\":\"_etag_6\",\"_edge\": [\"dummy\"],\"_edgeSpilled\": true,\"_revEdgeSpilled\": true,\"_reverse_edge\": [\"dummy\"]}", COLLECTION_NAME);

                
                string partitionByKey = AbstractGremlinTest.TEST_PARTITION_BY_KEY;
                if (partitionByKey == "name")
                {
                    if (TEST_USE_REVERSE_EDGE)
                    {
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_1009\",\"_isEdgeDoc\":true,\"_is_reverse\":false,\"_vertex_label\":\"person\",\"_vertex_id\":\"dummy\",\"_edge\":[{\"label\":\"knows\",\"weight\":0.5,\"id\":\"ID_1008\",\"_sinkV\":\"特殊符号\",\"_sinkVLabel\":\"person\",\"_sinkVPartition\":\"vadas\"}],\"name\":\"marko\",\"_etag\":\"_etag_7\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_1010\",\"_isEdgeDoc\":true,\"_is_reverse\":true,\"_vertex_label\":\"person\",\"_vertex_id\":\"特殊符号\",\"_edge\":[{\"label\":\"knows\",\"weight\":0.5,\"id\":\"ID_1008\",\"_srcV\":\"dummy\",\"_srcVLabel\":\"person\",\"_srcVPartition\":\"marko\"}],\"name\":\"vadas\",\"_etag\":\"_etag_8\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_1012\",\"_isEdgeDoc\":true,\"_is_reverse\":false,\"_vertex_label\":\"person\",\"_vertex_id\":\"dummy\",\"_edge\":[{\"label\":\"knows\",\"weight\":1,\"id\":\"ID_1011\",\"_sinkV\":\"引号\",\"_sinkVLabel\":\"person\",\"_sinkVPartition\":\"josh\"}],\"name\":\"marko\",\"_etag\":\"_etag_9\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_1013\",\"_isEdgeDoc\":true,\"_is_reverse\":true,\"_vertex_label\":\"person\",\"_vertex_id\":\"引号\",\"_edge\":[{\"label\":\"knows\",\"weight\":1,\"id\":\"ID_1011\",\"_srcV\":\"dummy\",\"_srcVLabel\":\"person\",\"_srcVPartition\":\"marko\"}],\"name\":\"josh\",\"_etag\":\"_etag_10\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_1015\",\"_isEdgeDoc\":true,\"_is_reverse\":false,\"_vertex_label\":\"person\",\"_vertex_id\":\"dummy\",\"_edge\":[{\"label\":\"created\",\"weight\":0.4,\"id\":\"ID_1014\",\"_sinkV\":\"这是一个中文ID\",\"_sinkVLabel\":\"software\",\"_sinkVPartition\":\"lop\"}],\"name\":\"marko\",\"_etag\":\"_etag_11\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_1016\",\"_isEdgeDoc\":true,\"_is_reverse\":true,\"_vertex_label\":\"software\",\"_vertex_id\":\"这是一个中文ID\",\"_edge\":[{\"label\":\"created\",\"weight\":0.4,\"id\":\"ID_1014\",\"_srcV\":\"dummy\",\"_srcVLabel\":\"person\",\"_srcVPartition\":\"marko\"}],\"name\":\"lop\",\"_etag\":\"_etag_12\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_1018\",\"_isEdgeDoc\":true,\"_is_reverse\":false,\"_vertex_label\":\"person\",\"_vertex_id\":\"引号\",\"_edge\":[{\"label\":\"created\",\"weight\":1,\"id\":\"ID_1017\",\"_sinkV\":\"中文English\",\"_sinkVLabel\":\"software\",\"_sinkVPartition\":\"ripple\"}],\"name\":\"josh\",\"_etag\":\"_etag_13\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_1019\",\"_isEdgeDoc\":true,\"_is_reverse\":true,\"_vertex_label\":\"software\",\"_vertex_id\":\"中文English\",\"_edge\":[{\"label\":\"created\",\"weight\":1,\"id\":\"ID_1017\",\"_srcV\":\"引号\",\"_srcVLabel\":\"person\",\"_srcVPartition\":\"josh\"}],\"name\":\"ripple\",\"_etag\":\"_etag_14\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_1021\",\"_isEdgeDoc\":true,\"_is_reverse\":false,\"_vertex_label\":\"person\",\"_vertex_id\":\"引号\",\"_edge\":[{\"label\":\"created\",\"weight\":0.4,\"id\":\"ID_1020\",\"_sinkV\":\"这是一个中文ID\",\"_sinkVLabel\":\"software\",\"_sinkVPartition\":\"lop\"}],\"name\":\"josh\",\"_etag\":\"_etag_15\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_1022\",\"_isEdgeDoc\":true,\"_is_reverse\":true,\"_vertex_label\":\"software\",\"_vertex_id\":\"这是一个中文ID\",\"_edge\":[{\"label\":\"created\",\"weight\":0.4,\"id\":\"ID_1020\",\"_srcV\":\"引号\",\"_srcVLabel\":\"person\",\"_srcVPartition\":\"josh\"}],\"name\":\"lop\",\"_etag\":\"_etag_16\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_1024\",\"_isEdgeDoc\":true,\"_is_reverse\":false,\"_vertex_label\":\"vertex\",\"_vertex_id\":\"ID_1007\",\"_edge\":[{\"label\":\"created\",\"weight\":0.2,\"id\":\"ID_1023\",\"_sinkV\":\"这是一个中文ID\",\"_sinkVLabel\":\"software\",\"_sinkVPartition\":\"lop\"}],\"name\":\"peter\",\"_etag\":\"_etag_17\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_1025\",\"_isEdgeDoc\":true,\"_is_reverse\":true,\"_vertex_label\":\"software\",\"_vertex_id\":\"这是一个中文ID\",\"_edge\":[{\"label\":\"created\",\"weight\":0.2,\"id\":\"ID_1023\",\"_srcV\":\"ID_1007\",\"_srcVLabel\":\"person\",\"_srcVPartition\":\"peter\"}],\"name\":\"lop\",\"_etag\":\"_etag_18\"}", COLLECTION_NAME);
                    }
                    else
                    {
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_1009\",\"_isEdgeDoc\":true,\"_is_reverse\":false,\"_vertex_label\":\"person\",\"_vertex_id\":\"dummy\",\"_edge\":[{\"label\":\"knows\",\"weight\":0.5,\"id\":\"ID_1008\",\"_sinkV\":\"特殊符号\",\"_sinkVLabel\":\"person\",\"_sinkVPartition\":\"vadas\"}],\"name\":\"marko\",\"_etag\":\"_etag_7\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_1012\",\"_isEdgeDoc\":true,\"_is_reverse\":false,\"_vertex_label\":\"person\",\"_vertex_id\":\"dummy\",\"_edge\":[{\"label\":\"knows\",\"weight\":1,\"id\":\"ID_1011\",\"_sinkV\":\"引号\",\"_sinkVLabel\":\"person\",\"_sinkVPartition\":\"josh\"}],\"name\":\"marko\",\"_etag\":\"_etag_8\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_1015\",\"_isEdgeDoc\":true,\"_is_reverse\":false,\"_vertex_label\":\"person\",\"_vertex_id\":\"dummy\",\"_edge\":[{\"label\":\"created\",\"weight\":0.4,\"id\":\"ID_1014\",\"_sinkV\":\"这是一个中文ID\",\"_sinkVLabel\":\"software\",\"_sinkVPartition\":\"lop\"}],\"name\":\"marko\",\"_etag\":\"_etag_9\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_1018\",\"_isEdgeDoc\":true,\"_is_reverse\":false,\"_vertex_label\":\"person\",\"_vertex_id\":\"引号\",\"_edge\":[{\"label\":\"created\",\"weight\":1,\"id\":\"ID_1017\",\"_sinkV\":\"中文English\",\"_sinkVLabel\":\"software\",\"_sinkVPartition\":\"ripple\"}],\"name\":\"josh\",\"_etag\":\"_etag_10\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_1021\",\"_isEdgeDoc\":true,\"_is_reverse\":false,\"_vertex_label\":\"person\",\"_vertex_id\":\"引号\",\"_edge\":[{\"label\":\"created\",\"weight\":0.4,\"id\":\"ID_1020\",\"_sinkV\":\"这是一个中文ID\",\"_sinkVLabel\":\"software\",\"_sinkVPartition\":\"lop\"}],\"name\":\"josh\",\"_etag\":\"_etag_11\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_1024\",\"_isEdgeDoc\":true,\"_is_reverse\":false,\"_vertex_label\":\"vertex\",\"_vertex_id\":\"ID_1007\",\"_edge\":[{\"label\":\"created\",\"weight\":0.2,\"id\":\"ID_1023\",\"_sinkV\":\"这是一个中文ID\",\"_sinkVLabel\":\"software\",\"_sinkVPartition\":\"lop\"}],\"name\":\"peter\",\"_etag\":\"_etag_12\"}", COLLECTION_NAME);
                    }
                }
                else if (partitionByKey == "label")
                {
                    if (TEST_USE_REVERSE_EDGE)
                    {
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_15\",\"_isEdgeDoc\":true,\"_is_reverse\":false,\"_vertex_label\":\"person\",\"_vertex_id\":\"dummy\",\"_edge\":[{\"label\":\"knows\",\"weight\":0.5,\"id\":\"ID_14\",\"_sinkV\":\"特殊符号\",\"_sinkVLabel\":\"person\",\"_sinkVPartition\":\"person\"}],\"label\":\"person\",\"_etag\":\"_etag_7\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_16\",\"_isEdgeDoc\":true,\"_is_reverse\":true,\"_vertex_label\":\"person\",\"_vertex_id\":\"特殊符号\",\"_edge\":[{\"label\":\"knows\",\"weight\":0.5,\"id\":\"ID_14\",\"_srcV\":\"dummy\",\"_srcVLabel\":\"person\",\"_srcVPartition\":\"person\"}],\"label\":\"person\",\"_etag\":\"_etag_8\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_18\",\"_isEdgeDoc\":true,\"_is_reverse\":false,\"_vertex_label\":\"person\",\"_vertex_id\":\"dummy\",\"_edge\":[{\"label\":\"knows\",\"weight\":1,\"id\":\"ID_17\",\"_sinkV\":\"引号\",\"_sinkVLabel\":\"person\",\"_sinkVPartition\":\"person\"}],\"label\":\"person\",\"_etag\":\"_etag_9\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_19\",\"_isEdgeDoc\":true,\"_is_reverse\":true,\"_vertex_label\":\"person\",\"_vertex_id\":\"引号\",\"_edge\":[{\"label\":\"knows\",\"weight\":1,\"id\":\"ID_17\",\"_srcV\":\"dummy\",\"_srcVLabel\":\"person\",\"_srcVPartition\":\"person\"}],\"label\":\"person\",\"_etag\":\"_etag_10\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_21\",\"_isEdgeDoc\":true,\"_is_reverse\":false,\"_vertex_label\":\"person\",\"_vertex_id\":\"dummy\",\"_edge\":[{\"label\":\"created\",\"weight\":0.4,\"id\":\"ID_20\",\"_sinkV\":\"这是一个中文ID\",\"_sinkVLabel\":\"software\",\"_sinkVPartition\":\"software\"}],\"label\":\"person\",\"_etag\":\"_etag_11\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_22\",\"_isEdgeDoc\":true,\"_is_reverse\":true,\"_vertex_label\":\"software\",\"_vertex_id\":\"这是一个中文ID\",\"_edge\":[{\"label\":\"created\",\"weight\":0.4,\"id\":\"ID_20\",\"_srcV\":\"dummy\",\"_srcVLabel\":\"person\",\"_srcVPartition\":\"person\"}],\"label\":\"software\",\"_etag\":\"_etag_12\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_24\",\"_isEdgeDoc\":true,\"_is_reverse\":false,\"_vertex_label\":\"person\",\"_vertex_id\":\"引号\",\"_edge\":[{\"label\":\"created\",\"weight\":1,\"id\":\"ID_23\",\"_sinkV\":\"中文English\",\"_sinkVLabel\":\"software\",\"_sinkVPartition\":\"software\"}],\"label\":\"person\",\"_etag\":\"_etag_13\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_25\",\"_isEdgeDoc\":true,\"_is_reverse\":true,\"_vertex_label\":\"software\",\"_vertex_id\":\"中文English\",\"_edge\":[{\"label\":\"created\",\"weight\":1,\"id\":\"ID_23\",\"_srcV\":\"引号\",\"_srcVLabel\":\"person\",\"_srcVPartition\":\"person\"}],\"label\":\"software\",\"_etag\":\"_etag_14\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_27\",\"_isEdgeDoc\":true,\"_is_reverse\":false,\"_vertex_label\":\"person\",\"_vertex_id\":\"引号\",\"_edge\":[{\"label\":\"created\",\"weight\":0.4,\"id\":\"ID_26\",\"_sinkV\":\"这是一个中文ID\",\"_sinkVLabel\":\"software\",\"_sinkVPartition\":\"software\"}],\"label\":\"person\",\"_etag\":\"_etag_15\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_28\",\"_isEdgeDoc\":true,\"_is_reverse\":true,\"_vertex_label\":\"software\",\"_vertex_id\":\"这是一个中文ID\",\"_edge\":[{\"label\":\"created\",\"weight\":0.4,\"id\":\"ID_26\",\"_srcV\":\"引号\",\"_srcVLabel\":\"person\",\"_srcVPartition\":\"person\"}],\"label\":\"software\",\"_etag\":\"_etag_16\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_30\",\"_isEdgeDoc\":true,\"_is_reverse\":false,\"_vertex_label\":\"vertex\",\"_vertex_id\":\"ID_1007\",\"_edge\":[{\"label\":\"created\",\"weight\":0.2,\"id\":\"ID_29\",\"_sinkV\":\"这是一个中文ID\",\"_sinkVLabel\":\"software\",\"_sinkVPartition\":\"software\"}],\"label\":\"person\",\"_etag\":\"_etag_17\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_31\",\"_isEdgeDoc\":true,\"_is_reverse\":true,\"_vertex_label\":\"software\",\"_vertex_id\":\"这是一个中文ID\",\"_edge\":[{\"label\":\"created\",\"weight\":0.2,\"id\":\"ID_29\",\"_srcV\":\"ID_1007\",\"_srcVLabel\":\"person\",\"_srcVPartition\":\"person\"}],\"label\":\"software\",\"_etag\":\"_etag_18\"}", COLLECTION_NAME);
                    }
                    else
                    {
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_15\",\"_isEdgeDoc\":true,\"_is_reverse\":false,\"_vertex_label\":\"person\",\"_vertex_id\":\"dummy\",\"_edge\":[{\"label\":\"knows\",\"weight\":0.5,\"id\":\"ID_14\",\"_sinkV\":\"特殊符号\",\"_sinkVLabel\":\"person\",\"_sinkVPartition\":\"person\"}],\"label\":\"person\",\"_etag\":\"_etag_7\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_18\",\"_isEdgeDoc\":true,\"_is_reverse\":false,\"_vertex_label\":\"person\",\"_vertex_id\":\"dummy\",\"_edge\":[{\"label\":\"knows\",\"weight\":1,\"id\":\"ID_17\",\"_sinkV\":\"引号\",\"_sinkVLabel\":\"person\",\"_sinkVPartition\":\"person\"}],\"label\":\"person\",\"_etag\":\"_etag_8\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_21\",\"_isEdgeDoc\":true,\"_is_reverse\":false,\"_vertex_label\":\"person\",\"_vertex_id\":\"dummy\",\"_edge\":[{\"label\":\"created\",\"weight\":0.4,\"id\":\"ID_20\",\"_sinkV\":\"这是一个中文ID\",\"_sinkVLabel\":\"software\",\"_sinkVPartition\":\"software\"}],\"label\":\"person\",\"_etag\":\"_etag_9\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_24\",\"_isEdgeDoc\":true,\"_is_reverse\":false,\"_vertex_label\":\"person\",\"_vertex_id\":\"引号\",\"_edge\":[{\"label\":\"created\",\"weight\":1,\"id\":\"ID_23\",\"_sinkV\":\"中文English\",\"_sinkVLabel\":\"software\",\"_sinkVPartition\":\"software\"}],\"label\":\"person\",\"_etag\":\"_etag_10\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_27\",\"_isEdgeDoc\":true,\"_is_reverse\":false,\"_vertex_label\":\"person\",\"_vertex_id\":\"引号\",\"_edge\":[{\"label\":\"created\",\"weight\":0.4,\"id\":\"ID_26\",\"_sinkV\":\"这是一个中文ID\",\"_sinkVLabel\":\"software\",\"_sinkVPartition\":\"software\"}],\"label\":\"person\",\"_etag\":\"_etag_11\"}", COLLECTION_NAME);
                        graphConnection.JsonServerClient.InsertJson("{\"id\":\"ID_30\",\"_isEdgeDoc\":true,\"_is_reverse\":false,\"_vertex_label\":\"vertex\",\"_vertex_id\":\"ID_1007\",\"_edge\":[{\"label\":\"created\",\"weight\":0.2,\"id\":\"ID_29\",\"_sinkV\":\"这是一个中文ID\",\"_sinkVLabel\":\"software\",\"_sinkVPartition\":\"software\"}],\"label\":\"person\",\"_etag\":\"_etag_12\"}", COLLECTION_NAME);
                    }
                }
                else
                {
                    throw new Exception("Not supported!");
                }
            }
            else
            {
                // TODO: Refactor this partition key.
                graphConnection = new GraphViewConnection(CONNECTION_STRING, COLLECTION_NAME, GraphType.GraphAPIOnly, true, 1, "/_partition", TEST_PARTITION_BY_KEY);
                GraphViewCommand command = new GraphViewCommand(graphConnection);
                // Reset collection
                graphConnection.ResetJsonServerCollection(COLLECTION_NAME);

                // INIT
                command.g().AddV("person").Property("id", "dummy").Property("name", "marko").Property("age", 29).Next();
                command.g().AddV("person").Property("id", "特殊符号").Property("name", "vadas").Property("age", 27).Next();
                command.g().AddV("software").Property("id", "这是一个中文ID").Property("name", "lop").Property("lang", "java").Next();
                command.g().AddV("person").Property("id", "引号").Property("name", "josh").Property("age", 32).Next();
                command.g().AddV("software").Property("id", "中文English").Property("name", "ripple").Property("lang", "java").Next();
                command.g().AddV("person").Property("name", "peter").Property("age", 35).Next();
                command.g().V().Has("name", "marko").AddE("knows").Property("weight", 0.5d).To(command.g().V().Has("name", "vadas")).Next();
                command.g().V().Has("name", "marko").AddE("knows").Property("weight", 1.0d).To(command.g().V().Has("name", "josh")).Next();
                command.g().V().Has("name", "marko").AddE("created").Property("weight", 0.4d).To(command.g().V().Has("name", "lop")).Next();
                command.g().V().Has("name", "josh").AddE("created").Property("weight", 1.0d).To(command.g().V().Has("name", "ripple")).Next();
                command.g().V().Has("name", "josh").AddE("created").Property("weight", 0.4d).To(command.g().V().Has("name", "lop")).Next();
                command.g().V().Has("name", "peter").AddE("created").Property("weight", 0.2d).To(command.g().V().Has("name", "lop")).Next();

            }
        }
#endif


        /// <summary>
        /// Do any necessary cleanup.
        /// </summary>
#if TEST_ON_DOCUMENT_DB
        [TestCleanup]
        public void Cleanup()
        {
            GraphDataLoader.ClearGraphData(GraphData.MODERN);
        }
#elif TEST_ON_JSONSERVER
        [TestCleanup]
        public void Cleanup()
        {
            Console.WriteLine("\n\n[Clean] Jsonserver doesn't need clean-up now.");
        }
#endif

        public string getVertexString(GraphViewCommand GraphViewCommand, string vertexName)
        {
            GraphViewCommand.OutputFormat = OutputFormat.GraphSON;
            return JsonConvert.DeserializeObject<dynamic>(GraphViewCommand.g().V().Has("name", vertexName).Next().FirstOrDefault()).First.ToString();
        }

        public string ConvertToVertexId(GraphViewCommand GraphViewCommand, string vertexName)
        {
            OutputFormat originalFormat = GraphViewCommand.OutputFormat;
            GraphViewCommand.OutputFormat = OutputFormat.Regular;

            string vertexId = GraphViewCommand.g().V().Has("name", vertexName).Id().Next().FirstOrDefault();

            GraphViewCommand.OutputFormat = originalFormat;

            return vertexId;
        }

        public string ConvertToPropertyId(GraphViewCommand GraphViewCommand, string vertexName, string property, string propertyValue)
        {
            OutputFormat originalFormat = GraphViewCommand.OutputFormat;
            GraphViewCommand.OutputFormat = OutputFormat.Regular;

            string propertyId = GraphViewCommand.g().V().Has("name", vertexName).Properties(property).HasValue(propertyValue).Id().Next().FirstOrDefault();

            GraphViewCommand.OutputFormat = originalFormat;

            return propertyId;
        }

        public string ConvertToEdgeId(GraphViewCommand GraphViewCommand, string outVertexName, string edgeLabel, string inVertexName)
        {
            OutputFormat originalFormat = GraphViewCommand.OutputFormat;
            GraphViewCommand.OutputFormat = OutputFormat.Regular;

            string edgeId = GraphViewCommand.g().V().Has("name", outVertexName).OutE(edgeLabel).As("e").InV().Has("name", inVertexName).Select("e").Values("id").Next().FirstOrDefault();

            GraphViewCommand.OutputFormat = originalFormat;

            return edgeId;
        }

        public static void CheckUnOrderedResults<T>(IEnumerable<T> expected, IEnumerable<T> actual)
        {
            CheckUnOrderedResults(expected, actual, EqualityComparer<T>.Default);
        }

        public static void CheckPathResults<T>(IEnumerable<T> expected, IEnumerable<T> actual)
        {
            Assert.AreEqual(expected.Count(), actual.Count());
            List<T> expectedList = new List<T>();
            foreach (var item in expected)
            {
                expectedList.Add(item);
            }
            List<T> actualList = new List<T>();
            foreach (var item in actual)
            {
                actualList.Add(item);
            }
            for (var i = 0; i < expectedList.Count(); i++)
            {
                Assert.AreEqual(expectedList[i], actualList[i]);
            }
        }

        public static List<string> ConvertToList(dynamic result)
        {
            return ((JArray) result).Select(p => p.ToString()).ToList();
        }

        public static void CheckOrderedResults<T>(IEnumerable<T> expected, IEnumerable<T> actual)
        {
            Assert.AreEqual(expected.Count(), actual.Count());
            List<T> expectedList = new List<T>();
            foreach (var item in expected)
            {
                expectedList.Add(item);
            }
            List<T> actualList = new List<T>();
            foreach (var item in actual)
            {
                actualList.Add(item);
            }
            for (var i = 0; i < expectedList.Count(); i++)
            {
                Assert.AreEqual(expectedList[i], actualList[i]);
            }
        }

        public static void CheckUnOrderedResults<T>(IEnumerable<T> expected, IEnumerable<T> actual, IEqualityComparer<T> comparer)
        {
            Assert.AreEqual(expected.Count(), actual.Count());

            Dictionary<T, int> expectedHashMap = GetHashMap(expected, comparer);
            Dictionary<T, int> actualHashMap = GetHashMap(actual, comparer);

            foreach (T key in expectedHashMap.Keys)
            {
                Assert.AreEqual(expectedHashMap[key], actualHashMap[key]);
            }
        }

        private static Dictionary<T, int> GetHashMap<T>(IEnumerable<T> list, IEqualityComparer<T> comparer)
        {
            Dictionary<T, int> hashMap = new Dictionary<T, int>(comparer);
            foreach (T listVal in list)
            {
                if (!hashMap.ContainsKey(listVal))
                {
                    hashMap[listVal] = 0;
                }
                hashMap[listVal]++;
            }
            return hashMap;
        }

        //public class DicionaryEqualityComparer<TKey, TValue> : IEqualityComparer<IDictionary<TKey, TValue>>
        //{
        //    public bool Equals(IDictionary<TKey, TValue> a, IDictionary<TKey, TValue> b)
        //    {
        //        return a.Count == b.Count && !a.Except(b).Any();
        //    }

        //    public int GetHashCode(IDictionary<TKey, TValue> a)
        //    {
        //        return a.Select(kvp => kvp.Key.GetHashCode() ^ kvp.Value.GetHashCode())
        //            .Aggregate(0, (acc, val) => (acc ^ val));
        //    }
        //}

        public int GetVertexCount(GraphViewCommand graph)
        {
            graph.OutputFormat = OutputFormat.Regular;
            int count = JsonConvert.DeserializeObject<int>(graph.g().V().Count().Next().First());
            graph.OutputFormat = OutputFormat.GraphSON;
            return count;
        }

        public int GetEdgeCount(GraphViewCommand graph)
        {
            graph.OutputFormat = OutputFormat.Regular;
            int count = JsonConvert.DeserializeObject<int>(graph.g().E().Count().Next().First());
            graph.OutputFormat = OutputFormat.GraphSON;
            return count;
        }
    }
}
