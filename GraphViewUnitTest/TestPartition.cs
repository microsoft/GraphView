#define LOCALTEST

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using GraphViewUnitTest.Gremlin;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json.Linq;

namespace GraphViewUnitTest
{
    [TestClass]
    public class TestPartition
    {
#if LOCALTEST
        private const string DOCDB_URL = "https://localhost:8081/";
        private const string DOCDB_AUTHKEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
        private const string DOCDB_DATABASE = "Wenbin";
#else
        private const string DOCDB_URL = "https://graphview.documents.azure.com:443/";
        private const string DOCDB_AUTHKEY = "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==";
        private const string DOCDB_DATABASE = "Temperary";
#endif

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static GraphViewConnection CreateConnection(string tips = null)
        {
            StackFrame frame = new StackFrame(1);
            if (!string.IsNullOrEmpty(tips)) {
                tips = $" {tips}";
            }
            string collectionName = $"[{frame.GetMethod().Name}]{tips}";

            GraphViewConnection connection = GraphViewConnection.ResetGraphAPICollection(DOCDB_URL, DOCDB_AUTHKEY, DOCDB_DATABASE, collectionName, AbstractGremlinTest.TEST_USE_REVERSE_EDGE, AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI);

            return connection;
        }




        [TestMethod]
        [Ignore]
        public void TestMultiplePartitionKeyPath()
        {
            DocumentClient client = new DocumentClient(new Uri(DOCDB_URL), DOCDB_AUTHKEY);
            Database database = client.CreateDatabaseAsync(new Database {Id = DOCDB_DATABASE}).Result;
            DocumentCollection collection = client.CreateDocumentCollectionAsync(
                database.SelfLink,
                new DocumentCollection {
                    Id = nameof(this.TestMultiplePartitionKeyPath),
                    PartitionKey = new PartitionKeyDefinition {
                        Paths = new Collection<string> {
                            "/name",
                            "/location"
                        }
                    }
                }
            ).Result;
            
            JObject document = new JObject {
                ["id"] = "TestId",
                ["name"] = "marko",
                ["location"] = "NYC",
            };
            client.CreateDocumentAsync(collection.SelfLink, document).Wait();
        }
    }
}
