using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Threading;
using GraphViewUnitTest.Gremlin;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace GraphViewUnitTest
{
    [TestClass]
    public partial class IoTUnitTest
    {
        private GraphViewCommand graph;

        private class IdTypeEtag
        {
            [JsonProperty("_id")]
            public string id { get; set; }

            [JsonProperty("type")]
            public string type { get; set; }

            [JsonProperty("_etag")]
            public string etag { get; set; }
        }

        public int GetVertexCount()
        {
            return JsonConvert.DeserializeObject<dynamic>(graph.g().V().FirstOrDefault()).Count;
        }

        public int GetEdgeCount()
        {
            var results = graph.g().E().Count().Next();
            return JsonConvert.DeserializeObject<dynamic>(graph.g().E().FirstOrDefault()).Count;
        }

        public void CreateDocs(List<string> docs)
        {
            List<Task> tasks = new List<Task>();
            foreach (string doc in docs)
            {
                tasks.Add(graph.Connection.CreateDocumentAsync(JObject.Parse(doc), graph));
            }
            Task.WaitAll(tasks.ToArray());
        }

        [TestInitialize]
        public void Setup()
        {
            //string endpoint = ConfigurationManager.AppSettings["DocDBEndPoint"];
            string endpoint = ConfigurationManager.AppSettings["DocDBEndPointLocal"];
            //string authKey = ConfigurationManager.AppSettings["DocDBKey"];
            string authKey = ConfigurationManager.AppSettings["DocDBKeyLocal"];
            string databaseId = ConfigurationManager.AppSettings["DocDBDatabaseGremlin"];
            string collectionId = ConfigurationManager.AppSettings["DocDBCollectionModern"];

            GraphViewConnection connection = GraphViewConnection.ResetGraphAPICollection(
                endpoint, authKey, databaseId, collectionId, AbstractGremlinTest.TEST_USE_REVERSE_EDGE, AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, null
                );

            graph = new GraphViewCommand(connection);
            graph.OutputFormat = OutputFormat.GraphSON;
        }

        public dynamic GetVertexPropertyValue(dynamic vertex, string property)
        {
            return vertex.properties[property].First.value;
        }

        public String formatQueryStr(String query)
        {
            String result = "";
            query = query.Replace("\"", "\'");
            result = query;
            return result;
        }

        [TestMethod]
        [Ignore]
        public void preProcessTheQueries()
        {
            int counter = 1;
            string line;
            // Read the file and display it line by line.
            System.IO.StreamReader inFile =
               new System.IO.StreamReader("D:\\gremlin.tsv");
            while ((line = inFile.ReadLine()) != null)
            {
                Console.WriteLine(line);
                var array = line.Split('\t');
                var processedQuery = formatQueryStr(array[0]);
                System.IO.StreamWriter outFile = new System.IO.StreamWriter(@"D:\\" + "HUB_queries_" + counter + ".txt", false);
                outFile.WriteLine(processedQuery);
                outFile.Close();
                counter++;
            }
            inFile.Close();

        }

        [TestMethod]
        [Ignore]
        public void testSingleQuery()
        {
            //runQuery(9);
        }

        [TestMethod]
        [Ignore]
        public void test_IoT_topology_queries()
        {
            graph.OutputFormat = OutputFormat.Regular;
            int count = 1;
            for (; count <= 75; count++)
            {
                if (count == 57) continue;
                if (count == 65) continue;
                if (count == 68) continue;
                Console.WriteLine($"------------------- Query : {count} -------------------");
                try
                {
                    System.IO.StreamReader filePath = new System.IO.StreamReader("D:\\topology-queries_" + count + ".txt");

                    runQuery(filePath);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Query" + (count) + " throw Exception");
                    Console.WriteLine(e.Message);
                    Console.WriteLine(e.StackTrace);
                    throw e;
                }
            }
        }

        [TestMethod]
        [Ignore]
        public void test_IOT_HUB_queries()
        {
            graph.OutputFormat = OutputFormat.Regular;
            int count = 1;
            for (; count <= 64; count++)
            {
                if (count == 46) continue;
                if (count == 54) continue;
                if (count == 57) continue;
                Console.WriteLine($"------------------- Query : {count} -------------------");
                try
                {
                    System.IO.StreamReader filePath = new System.IO.StreamReader("D:\\HUB_queries_" + count + ".txt");
                    runQuery(filePath);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Query" + (count) + " throw Exception");
                    Console.WriteLine(e.Message);
                    Console.WriteLine(e.StackTrace);
                    throw e;
                }
            }
        }

        public void runQuery(System.IO.StreamReader filePath)
        {
            string line = filePath.ReadLine();
            filePath.Close();

            graph.CommandText = line;
            graph.OutputFormat = OutputFormat.GraphSON;
            var results = graph.Execute();
            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void TestAllIoTTest()
        {
            List<string> output;
            List<Dictionary<string, string>> expectedResults;
            int matchCount;
            dynamic results;
            ////===========================================
            output = Get_IoT_test_1();

            results = JsonConvert.DeserializeObject<dynamic>(output.FirstOrDefault()).First;
            Assert.AreEqual("application", (string)results.label);
            Assert.AreEqual("vertex", (string)results.type);
            Assert.AreEqual("test-app", (string)GetVertexPropertyValue(results, "_app"));
            Assert.AreEqual("test-app", (string)GetVertexPropertyValue(results, "__id"));
            Assert.AreEqual(1, (int)GetVertexPropertyValue(results, "_provisioningState"));
            Assert.AreEqual(false, (bool)GetVertexPropertyValue(results, "_deleted"));
            Assert.AreEqual(1, GetVertexCount());
            Assert.AreEqual(0, GetEdgeCount());
            //===========================================
            output = Get_IoT_test_2();
            results = JsonConvert.DeserializeObject<List<IdTypeEtag>>(output.FirstOrDefault());
            Assert.AreEqual(4, results.Count);
            expectedResults = new List<Dictionary<string, string>> {
                new Dictionary<string, string>  { {"id","product:soda-machine"},{"type","product-model"},{"etag","B0vDw1xnS/agXzX9F7wxHg=="}},
                new Dictionary<string, string>  { {"id","uber-product:soda-machine"},{"type","product-model"},{"etag","SkYTpr1hSkCL4NkpsfNwvQ=="}},
                new Dictionary<string, string>  { {"id","device:ice-machine"},{"type","device-model"},{"etag","SWnFiMWDTVGOWUJvcqCbtg=="}},
                new Dictionary<string, string>  { {"id","device:soda-mixer"},{"type","device-model"},{"etag","lsRrd7JWSBqW9kiBVPS7aQ=="}}
                };

            matchCount = 0;
            foreach (var result in results)
            {
                foreach (var expectedResult in expectedResults)
                {
                    if (expectedResult["id"] == result.id)
                    {
                        Assert.AreEqual(expectedResult["type"], result.type);
                        Assert.AreEqual(expectedResult["etag"], result.etag);
                        matchCount++;
                        break;
                    }
                }
            }
            Assert.AreEqual(4, matchCount);
            Assert.AreEqual(26, GetVertexCount());
            Assert.AreEqual(27, GetEdgeCount());
            //===========================================
            output = Get_IoT_test_3_prefetch();

            //foreach (var result in results)
            //{
            //    Console.WriteLine(result);
            //}

            //===========================================

            output = Get_IoT_test_3();
            results = JsonConvert.DeserializeObject<List<IdTypeEtag>>(output.FirstOrDefault());
            Assert.AreEqual(16, results.Count);
            expectedResults = new List<Dictionary<string, string>> {
                new Dictionary<string, string>  { {"id","product:soda-machine:shop-1"},{"type","product"},{"etag","gtxVWBOYROCC4We9TdK8yA=="}},
                new Dictionary<string, string>  { { "id", "product:soda-machine:shop-2"},{"type","product"},{ "etag","XVALE7oMRR63jfS4biDS9w=="}},
                new Dictionary<string, string>  { {"id","product:soda-machine:shop-3.1"},{"type","product"},{"etag","WJAjOSurTmGZ6CnfBELyUA=="}},
             new Dictionary<string, string>  { {"id","product:soda-machine:shop-3.2"},{"type","product"},{"etag","3pO/jDqlR0mfoDy1csN+Yw=="}},
             new Dictionary<string, string>  { {"id","uber-product:soda-machine:shop-3"},{"type","product"},{"etag","TMaJk/CGRyurJIle/FncMA=="}},
             new Dictionary<string, string>  { {"id","device:ice-machine:shop-1"},{"type","device"},{"etag","wPY/iDq7RiqmokdVPeENcQ=="}},
             new Dictionary<string, string>  { {"id","device:soda-mixer:shop-1"},{"type","device"},{"etag","uA54hXcmQmyaRwOAkQWcWQ=="}},
             new Dictionary<string, string>  { {"id","device:ice-machine:shop-2"},{"type","device"},{"etag","FBYA/q6dTE6Ny7/v3iTNQg=="}},
             new Dictionary<string, string>  { {"id","device:cola-mixer:shop-2"},{"type","device"},{"etag","oqielLa9QWeVjd2p9lWZPQ=="}},
             new Dictionary<string, string>  { {"id","device:root-beer-mixer:shop-2"},{"type","device"},{"etag","4u7k7lAaSKuUUL2iHbBcRQ=="}},
             new Dictionary<string, string>  { {"id","device:lemonade-mixer:shop-2"},{"type","device"},{"etag","kkLGbSdzSbiCi7w7VM12gw=="}},
             new Dictionary<string, string>  { {"id","device:ice-machine:shop-3.1"},{"type","device"},{"etag","cWI7zlmBSNei70b7zoqghw=="}},
             new Dictionary<string, string>  { {"id","device:soda-mixer:shop-3.1"},{"type","device"},{"etag","yOXsJu84SJW6Amtm9FF9ug=="}},
             new Dictionary<string, string>  { {"id","device:ice-machine:shop-3.2"},{"type","device"},{"etag","XTb4lY83SLes2c+gZZ6vfA=="}},
             new Dictionary<string, string>  { {"id","device:cola-mixer:shop-3.2"},{"type","device"},{"etag","G1lCXUnhRSCqohWUaZza8w=="}},
             new Dictionary<string, string>  { {"id","device:kool-aid-mixer:shop-3.2"},{"type","device"},{"etag","E5h6wBBpRjuDWkVaJ/Ud+Q=="}},
                };

            matchCount = 0;
            foreach (var result in results)
            {
                foreach (var expectedResult in expectedResults)
                {
                    if (expectedResult["id"] == result.id)
                    {
                        Assert.AreEqual(expectedResult["type"], result.type);
                        Assert.AreEqual(expectedResult["etag"], result.etag);
                        matchCount++;
                        break;
                    }
                }
            }
            Assert.AreEqual(16, matchCount);
            Assert.AreEqual(42, GetVertexCount());
            Assert.AreEqual(56, GetEdgeCount());
            //===========================================
            output = Get_IoT_test_4();

            Assert.AreEqual(42, GetVertexCount());
            Assert.AreEqual(56, GetEdgeCount());
            //===========================================
            output = Get_IoT_test_5();
            Assert.AreEqual(42, GetVertexCount());
            Assert.AreEqual(56, GetEdgeCount());
            //===========================================
            output = Get_IoT_test_6();
            Assert.AreEqual(42, GetVertexCount());
            Assert.AreEqual(56, GetEdgeCount());
            //===========================================
            output = Get_IoT_test_7();
            Assert.AreEqual(42, GetVertexCount());
            Assert.AreEqual(56, GetEdgeCount());
            //===========================================
            output = Get_IoT_test_8();
            results = JsonConvert.DeserializeObject<List<IdTypeEtag>>(output.FirstOrDefault());
            Assert.AreEqual(2, results.Count);
            expectedResults = new List<Dictionary<string, string>> {
            new Dictionary<string, string>  { {"id", "uber-product:soda-machine" },{"type", "product-model" },{"etag", "0Ro9MX91RYWT3ZWuot53FA==" } },
            new Dictionary<string, string>  { {"id", "product:soda-machine" },{"type", "product-model" },{"etag", "iBuelvJFQuSGRQfEvvzPrA==" } }
            };
            matchCount = 0;
            foreach (var result in results)
            {
                foreach (var expectedResult in expectedResults)
                {
                    if (expectedResult["id"] == result.id)
                    {
                        Assert.AreEqual(expectedResult["type"], result.type);
                        Assert.AreEqual(expectedResult["etag"], result.etag);
                        matchCount++;
                        break;
                    }
                }
            }
            Assert.AreEqual(2, matchCount);
            Assert.AreEqual(44, GetVertexCount());
            Assert.AreEqual(59, GetEdgeCount());
            //===========================================
            output = Get_IoT_test_9_prefetch();

            //===========================================
            output = Get_IoT_test_9();
            results = JsonConvert.DeserializeObject<List<IdTypeEtag>>(output.FirstOrDefault());
            Assert.AreEqual(2, results.Count);
            expectedResults = new List<Dictionary<string, string>> {
            new Dictionary<string, string>  { {"id", "product:soda-machine:shop-2" },{"type", "product" },{"etag", "2dGQ3DDwSUKY2Jv+9K9t3A==" } },
            new Dictionary<string, string>  { {"id", "device:soda-mixer:shop-1" },{"type", "device" },{"etag", "LmyeSEx1RL+cIZRKKvFPvA==" } }
            };
            matchCount = 0;
            foreach (var result in results)
            {
                foreach (var expectedResult in expectedResults)
                {
                    if (expectedResult["id"] == result.id)
                    {
                        Assert.AreEqual(expectedResult["type"], result.type);
                        Assert.AreEqual(expectedResult["etag"], result.etag);
                        matchCount++;
                        break;
                    }
                }
            }
            Assert.AreEqual(2, matchCount);
            Assert.AreEqual(44, GetVertexCount());
            Assert.AreEqual(60, GetEdgeCount());
            //===========================================
            output = Get_IoT_test_10_prefetch();

            //===========================================
            output = Get_IoT_test_10();
            results = JsonConvert.DeserializeObject<List<IdTypeEtag>>(output.FirstOrDefault());
            Assert.AreEqual(1, results.Count);
            expectedResults = new List<Dictionary<string, string>> {
            new Dictionary<string, string>  { {"id", "uber-product:soda-machine:shop-3" },{"type", "product" },{"etag", "lunRO6wJQg6WMNq/CGr7QA==" } }
            };

            matchCount = 0;
            foreach (var result in results)
            {
                foreach (var expectedResult in expectedResults)
                {
                    if (expectedResult["id"] == result.id)
                    {
                        Assert.AreEqual(expectedResult["type"], result.type);
                        Assert.AreEqual(expectedResult["etag"], result.etag);
                        matchCount++;
                        break;
                    }
                }
            }
            Assert.AreEqual(1, matchCount);
            Assert.AreEqual(44, GetVertexCount());
            Assert.AreEqual(60, GetEdgeCount());
            //===========================================
            output = Get_IoT_test_11_prefetch();

            //===========================================
            output = Get_IoT_test_11();
            results = JsonConvert.DeserializeObject<List<IdTypeEtag>>(output.FirstOrDefault());
            Assert.AreEqual(6, results.Count);
            expectedResults = new List<Dictionary<string, string>> {
                new Dictionary<string, string>  { {"id","uber-product:soda-machine:shop-3"},{"type","product"},{"etag","yzm2GRluTOim/fvMmuxh2g=="}},
                new Dictionary<string, string>  { {"id","device:cola-mixer:shop-3.2"},{"type","device"},{"etag","aj+sec3TRnCF1mwWDErzqA=="}},
                new Dictionary<string, string>  { {"id","device:kool-aid-mixer:shop-3.2"},{"type","device"},{"etag","k0maOZ1/QF+d9fn7WR8YWQ=="}},
                new Dictionary<string, string>  { {"id","device:soda-mixer:shop-3.1"},{"type","device"},{"etag","OP8/P5nKSUyWscCtNgGstw=="}},
                new Dictionary<string, string>  { {"id","product:soda-machine:shop-3.2"},{"type","product"},{"etag","3pO/jDqlR0mfoDy1csN+Yw=="}},
                new Dictionary<string, string>  { {"id","product:soda-machine:shop-3.1"},{"type","product"},{"etag","WJAjOSurTmGZ6CnfBELyUA=="}}};
            matchCount = 0;
            foreach (var result in results)
            {
                foreach (var expectedResult in expectedResults)
                {
                    if (expectedResult["id"] == result.id)
                    {
                        Assert.AreEqual(expectedResult["type"], result.type);
                        Assert.AreEqual(expectedResult["etag"], result.etag);
                        matchCount++;
                        break;
                    }
                }
            }
            Assert.AreEqual(6, matchCount);
            Assert.AreEqual(50, GetVertexCount());
            Assert.AreEqual(66, GetEdgeCount());
            //===========================================
            output = Get_IoT_test_12();
            Assert.AreEqual(50, GetVertexCount());
            Assert.AreEqual(66, GetEdgeCount());
            //===========================================
            output = Get_IoT_test_13();
            Assert.AreEqual(50, GetVertexCount());
            Assert.AreEqual(66, GetEdgeCount());
            //===========================================
            output = Get_IoT_test_14();
            Assert.AreEqual(49, GetVertexCount());
            Assert.AreEqual(63, GetEdgeCount());
            //===========================================
            output = Get_IoT_test_15();
            Assert.AreEqual(41, GetVertexCount());
            Assert.AreEqual(53, GetEdgeCount());
            ////===========================================
            output = Get_IoT_test_16();
            Assert.AreEqual(41, GetVertexCount());
            Assert.AreEqual(53, GetEdgeCount());
            ////===========================================
            output = Get_IoT_test_17();
            Assert.AreEqual(0, GetVertexCount());
            Assert.AreEqual(0, GetEdgeCount());
            //===========================================
        }


        public List<string> Get_IoT_test_1()
        {
            graph.CommandText = "g" +
                                ".addV('application')" +
                                ".property('_app', 'test-app')" +
                                ".property('__id', 'test-app')" +
                                ".property('_provisioningState', 1)" +
                                ".property('_deleted', false)";
            return graph.ExecuteAndGetResults();
        }

        public List<string> Get_IoT_test_2()
        {
            graph.CommandText = "g.inject(0).coalesce(__.union(__.not(__.V().has('_app','test-app').has('__id','test-app').hasLabel('application')).constant('~0'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',0).constant('~1'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',2).constant('~2'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_deleted',true).constant('~3'),__.V().has('_app','test-app').has('__id','product:soda-machine').hasLabel('product-model').constant('~4'),__.V().has('_app','test-app').has('__id','uber-product:soda-machine').hasLabel('product-model').constant('~5'),__.V().has('_app','test-app').has('__id','device:ice-machine').hasLabel('device-model').constant('~6'),__.V().has('_app','test-app').has('__id','device:soda-mixer').hasLabel('device-model').constant('~7')),__.project('#v0','#v1','#v2','#v3').by(__.addV('product-model').property('_app','test-app').property('__id','product:soda-machine').property('__etag','B0vDw1xnS/agXzX9F7wxHg==').sideEffect(__.union(__.property('_name','Soda Machine'),__.sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','_properties').property('_ary',true).inV().sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','0').property('_ary',false).inV().property('_id','location').property('name','Soda machine location').property('kind','property').property('type','string')).sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','1').property('_ary',false).inV().property('_id','installer').property('name','Soda machine installer').property('kind','property').property('type','string')).sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','2').property('_ary',false).inV().property('_id','syrup_level').property('name','Syrup level').property('kind','reference').sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','target').property('_ary',false).inV().property('_id','device:soda-mixer').property('type','device'))).sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','3').property('_ary',false).inV().property('_id','ice_level').property('name','Ice level').property('kind','reference').sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','target').property('_ary',false).inV().property('_id','device:ice-machine').property('type','device'))))))).by(__.addV('product-model').property('_app','test-app').property('__id','uber-product:soda-machine').property('__etag','SkYTpr1hSkCL4NkpsfNwvQ==').sideEffect(__.union(__.property('_name','Uber Soda Machine'),__.sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','_properties').property('_ary',true).inV().sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','0').property('_ary',false).inV().property('_id','location').property('name','Soda machine location').property('kind','property').property('type','string')).sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','1').property('_ary',false).inV().property('_id','installer').property('name','Soda machine installer').property('kind','property').property('type','string')).sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','2').property('_ary',false).inV().property('_id','syrup_level').property('name','Syrup Level').property('kind','reference').sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','target').property('_ary',false).inV().property('_id','product:soda-machine').property('type','product'))))))).by(__.addV('device-model').property('_app','test-app').property('__id','device:ice-machine').property('__etag','SWnFiMWDTVGOWUJvcqCbtg==').sideEffect(__.union(__.property('_name','Ice Machine'),__.sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','_properties').property('_ary',true).inV().sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','0').property('_ary',false).inV().property('_id','firmware_version').property('name','Firmware Version').property('kind','desired').property('type','string').property('path','/firmware_version')).sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','1').property('_ary',false).inV().property('_id','serial_number').property('name','Serial Number').property('kind','desired').property('type','string').property('path','/serial_number')).sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','2').property('_ary',false).inV().property('_id','ice_level').property('name','Ice Level').property('kind','reported').property('type','number').property('path','/ice_level')))))).by(__.addV('device-model').property('_app','test-app').property('__id','device:soda-mixer').property('__etag','lsRrd7JWSBqW9kiBVPS7aQ==').sideEffect(__.union(__.property('_name','Soda Mixer'),__.sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','_properties').property('_ary',true).inV().sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','0').property('_ary',false).inV().property('_id','firmware_version').property('name','Firmware Version').property('kind','desired').property('type','string').property('path','/firmware_version')).sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','1').property('_ary',false).inV().property('_id','serial_number').property('name','Serial Number').property('kind','desired').property('type','string').property('path','/serial_number')).sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','2').property('_ary',false).inV().property('_id','co2_level').property('name','CO2 Level').property('kind','reported').property('type','number').property('path','/co2_level')).sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','3').property('_ary',false).inV().property('_id','syrup_level').property('name','Syrup Level').property('kind','reported').property('type','number').property('path','/syrup_level')))))).as('#v').project('#e0','#e1','#e2','#e3','#e4','#e5').by(__.select('#v2').addE('device-product').to(__.select('#v').select('#v0'))).by(__.select('#v3').addE('device-product').to(__.select('#v').select('#v0'))).by(__.select('#v0').addE('product-product').to(__.select('#v').select('#v1'))).by(__.select('#v0').addE('ref').to(__.select('#v').select('#v3')).property('_key','syrup_level').property('_ref','syrup_level')).by(__.select('#v0').addE('ref').to(__.select('#v').select('#v2')).property('_key','ice_level').property('_ref','ice_level')).by(__.select('#v1').addE('ref').to(__.select('#v').select('#v0')).property('_key','syrup_level').property('_ref','syrup_level')).as('#e').union(__.select('#v').union(__.select('#v0').as('#a').constant(['_name','_properties']),__.select('#v1').as('#a').constant(['_name','_properties']),__.select('#v2').as('#a').constant(['_name','_properties']),__.select('#v3').as('#a').constant(['_name','_properties'])).as('#p'),__.select('#e').union(__.select('#e0'),__.select('#e1'),__.select('#e2'),__.select('#e3'),__.select('#e4'),__.select('#e5')).as('#f').union(__.inV().as('#a').select('#f').outV(),__.outV().as('#a').select('#f').inV()).map(__.optional(__.out('mdl')).as('#m').select('#a').optional(__.out('mdl')).inE('ref').and(__.outV().where(eq('#m'))).values('_key').fold()).as('#p')).select('#a').union(__.identity(),__.as('@v').flatMap(__.optional(__.out('mdl')).inE('ref').and(__.values('_key').where(within('#p')))).repeat(__.as('@e').flatMap(__.outV().as('mdl').select(last,'@v').both().dedup().and(__.optional(__.out('mdl')).where(eq('mdl')))).as('@v').optional(__.flatMap(__.select(last,'@e').values('_key').as('key').select(last,'@v').optional(__.out('mdl')).inE('ref').and(__.values('_ref').where(eq('key')))))).until(__.flatMap(__.as('res').select(last,'@v').where(eq('res')))).select('@v').unfold()).dedup().project('_id','type','_etag').by(__.values('__id')).by(__.label()).by(__.values('__etag')))";
            // graph.CommandText =
            //    "g.inject(0).coalesce(__.project('#v0').by(__.addV('product-model').property('_app','test-app').property('__id','product:soda-machine').property('__etag','B0vDw1xnS/agXzX9F7wxHg==').sideEffect(__.union(__.property('_name','SodaMachine'),__.sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','_properties').property('_ary',true).inV().sideEffect(__.constant(0)))))))";
            return graph.ExecuteAndGetResults();
        }

        public List<string> Get_IoT_test_3_prefetch()
        {
            graph.CommandText =
                "g.V().has('_app','test-app').has('__id','test-app').hasLabel('application').coalesce(__.union(__.not(__.V().has('_app','test-app').has('__id','test-app').hasLabel('application')).constant('~0'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',0).constant('~1'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',2).constant('~2'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_deleted',true).constant('~3')),__.flatMap(__.project('nodes','edges').by(__.union(__.V().has('_app','test-app').has('__id','product:soda-machine').hasLabel('product-model'),__.V().has('_app','test-app').has('__id','uber-product:soda-machine').hasLabel('product-model'),__.V().has('_app','test-app').has('__id','device:ice-machine').hasLabel('device-model'),__.V().has('_app','test-app').has('__id','device:soda-mixer').hasLabel('device-model')).fold()).by(__.union(__.V().has('_app','test-app').has('__id','device:ice-machine').hasLabel('device-model').flatMap(__.as('src').flatMap(__.V().has('_app','test-app').has('__id','product:soda-machine').hasLabel('product-model')).as('tgt').select('src').outE('device-product').and(__.inV().where(eq('tgt')))),__.V().has('_app','test-app').has('__id','device:soda-mixer').hasLabel('device-model').flatMap(__.as('src').flatMap(__.V().has('_app','test-app').has('__id','product:soda-machine').hasLabel('product-model')).as('tgt').select('src').outE('device-product').and(__.inV().where(eq('tgt')))),__.V().has('_app','test-app').has('__id','product:soda-machine').hasLabel('product-model').flatMap(__.as('src').flatMap(__.V().has('_app','test-app').has('__id','uber-product:soda-machine').hasLabel('product-model')).as('tgt').select('src').outE('product-product').and(__.inV().where(eq('tgt'))))).fold()).sideEffect(__.select('edges').unfold().project('name','source','target','properties').by(__.label()).by(__.outV().project('_id','type','_etag').by(__.values('__id')).by(__.label()).by(__.values('__etag'))).by(__.inV().project('_id','type','_etag').by(__.values('__id')).by(__.label()).by(__.values('__etag'))).by(__.properties().group().by(__.key()).by(__.value())).store('^edges')).select('nodes').unfold().union(__.identity().sideEffect(__.id().store('^ids')),__.as('@v').flatMap(__.optional(__.out('mdl')).outE('ref')).repeat(__.as('@e').flatMap(__.inV().as('mdl').select(last,'@v').both().dedup().and(__.optional(__.out('mdl')).where(eq('mdl')))).as('@v').optional(__.flatMap(__.select(last,'@e').values('_ref').as('key').select(last,'@v').optional(__.out('mdl')).outE('ref').and(__.values('_key').where(eq('key')))))).until(__.flatMap(__.as('res').select(last,'@v').where(eq('res')))).sideEffect(__.project('data','info').by(__.select('@e').unfold().project('key','ref').by(__.values('_key')).by(__.values('_ref')).fold()).by(__.select('@v').unfold().project('_id','type','_etag').by(__.values('__id')).by(__.label()).by(__.values('__etag')).fold()).store('^refs'))).dedup().union(__.identity().sideEffect(__.group('^mdls').by(__.id()).by(__.coalesce(__.out('mdl').values('__id'),__.constant('')))),__.out('mdl')).dedup()).union(__.emit().repeat(__.outE('_val').as('_').inV()).tree(),__.cap('^ids'),__.cap('^mdls'),__.cap('^refs')).fold().union(__.identity(),__.cap('^edges')))";
            return graph.ExecuteAndGetResults();
        }

        public List<string> Get_IoT_test_3()
        {
            graph.CommandText =
                "g.inject(0).coalesce(__.union(__.not(__.V().has('_app','test-app').has('__id','test-app').hasLabel('application')).constant('~0'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',0).constant('~1'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',2).constant('~2'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_deleted',true).constant('~3'),__.V().has('_app','test-app').has('__id','product:soda-machine:shop-1').hasLabel('product').constant('~4'),__.not(__.V().has('_app','test-app').has('__id','product:soda-machine').hasLabel('product-model').has('__etag','B0vDw1xnS/agXzX9F7wxHg==')).constant('~5'),__.V().has('_app','test-app').has('__id','product:soda-machine:shop-2').hasLabel('product').constant('~6'),__.V().has('_app','test-app').has('__id','product:soda-machine:shop-3.1').hasLabel('product').constant('~7'),__.V().has('_app','test-app').has('__id','product:soda-machine:shop-3.2').hasLabel('product').constant('~8'),__.V().has('_app','test-app').has('__id','uber-product:soda-machine:shop-3').hasLabel('product').constant('~9'),__.not(__.V().has('_app','test-app').has('__id','uber-product:soda-machine').hasLabel('product-model').has('__etag','SkYTpr1hSkCL4NkpsfNwvQ==')).constant('~10'),__.V().has('_app','test-app').has('__id','device:ice-machine:shop-1').hasLabel('device').constant('~11'),__.not(__.V().has('_app','test-app').has('__id','device:ice-machine').hasLabel('device-model').has('__etag','SWnFiMWDTVGOWUJvcqCbtg==')).constant('~12'),__.V().has('_app','test-app').has('__id','device:soda-mixer:shop-1').hasLabel('device').constant('~13'),__.not(__.V().has('_app','test-app').has('__id','device:soda-mixer').hasLabel('device-model').has('__etag','lsRrd7JWSBqW9kiBVPS7aQ==')).constant('~14'),__.V().has('_app','test-app').has('__id','device:ice-machine:shop-2').hasLabel('device').constant('~15'),__.V().has('_app','test-app').has('__id','device:cola-mixer:shop-2').hasLabel('device').constant('~16'),__.V().has('_app','test-app').has('__id','device:root-beer-mixer:shop-2').hasLabel('device').constant('~17'),__.V().has('_app','test-app').has('__id','device:lemonade-mixer:shop-2').hasLabel('device').constant('~18'),__.V().has('_app','test-app').has('__id','device:ice-machine:shop-3.1').hasLabel('device').constant('~19'),__.V().has('_app','test-app').has('__id','device:soda-mixer:shop-3.1').hasLabel('device').constant('~20'),__.V().has('_app','test-app').has('__id','device:ice-machine:shop-3.2').hasLabel('device').constant('~21'),__.V().has('_app','test-app').has('__id','device:cola-mixer:shop-3.2').hasLabel('device').constant('~22'),__.V().has('_app','test-app').has('__id','device:kool-aid-mixer:shop-3.2').hasLabel('device').constant('~23')),__.project('#v0','#v1','#v2','#v3','#v4','#v5','#v6','#v7','#v8','#v9','#v10','#v11','#v12','#v13','#v14','#v15').by(__.addV('product').property('_app','test-app').property('__id','product:soda-machine:shop-1').property('__etag','gtxVWBOYROCC4We9TdK8yA==').sideEffect(__.union(__.property('name','Soda Machine #1'),__.property('location','Building 43 - Garage'),__.property('installer','Jack Brown'),__.addE('mdl').to(__.V().has('_app','test-app').has('__id','product:soda-machine').hasLabel('product-model'))))).by(__.addV('product').property('_app','test-app').property('__id','product:soda-machine:shop-2').property('__etag','XVALE7oMRR63jfS4biDS9w==').sideEffect(__.union(__.property('name','Soda Machine #2'),__.property('location','Building 44 - Cafe'),__.property('installer','Jim Johns'),__.addE('mdl').to(__.V().has('_app','test-app').has('__id','product:soda-machine').hasLabel('product-model'))))).by(__.addV('product').property('_app','test-app').property('__id','product:soda-machine:shop-3.1').property('__etag','WJAjOSurTmGZ6CnfBELyUA==').sideEffect(__.union(__.property('name','Soda Machine #3.1'),__.property('location','Microsoft Visitor Center - Ground Floor'),__.property('installer','Eva Green'),__.addE('mdl').to(__.V().has('_app','test-app').has('__id','product:soda-machine').hasLabel('product-model'))))).by(__.addV('product').property('_app','test-app').property('__id','product:soda-machine:shop-3.2').property('__etag','3pO/jDqlR0mfoDy1csN+Yw==').sideEffect(__.union(__.property('name','Soda Machine #3.2'),__.property('location','Building 43 - Second Floor'),__.property('installer','Ronnie Wood'),__.addE('mdl').to(__.V().has('_app','test-app').has('__id','product:soda-machine').hasLabel('product-model'))))).by(__.addV('product').property('_app','test-app').property('__id','uber-product:soda-machine:shop-3').property('__etag','TMaJk/CGRyurJIle/FncMA==').sideEffect(__.union(__.property('name','Uber Soda Machine #3'),__.property('location','Building 43 - Third Floor'),__.property('installer','Albert Sims'),__.addE('mdl').to(__.V().has('_app','test-app').has('__id','uber-product:soda-machine').hasLabel('product-model'))))).by(__.addV('device').property('_app','test-app').property('__id','device:ice-machine:shop-1').property('__etag','wPY/iDq7RiqmokdVPeENcQ==').sideEffect(__.union(__.property('name','Ice Machine #456789'),__.property('serial_number','3333-456789'),__.property('firmware_version','1.0.0'),__.property('ice_level',1.2),__.addE('mdl').to(__.V().has('_app','test-app').has('__id','device:ice-machine').hasLabel('device-model'))))).by(__.addV('device').property('_app','test-app').property('__id','device:soda-mixer:shop-1').property('__etag','uA54hXcmQmyaRwOAkQWcWQ==').sideEffect(__.union(__.property('name','Soda Mixer #123456'),__.property('serial_number','4444-123456'),__.property('firmware_version','1.1.0'),__.property('co2_level',0.1),__.property('syrup_level',0.1),__.addE('mdl').to(__.V().has('_app','test-app').has('__id','device:soda-mixer').hasLabel('device-model'))))).by(__.addV('device').property('_app','test-app').property('__id','device:ice-machine:shop-2').property('__etag','FBYA/q6dTE6Ny7/v3iTNQg==').sideEffect(__.union(__.property('name','Ice Machine #456123'),__.property('serial_number','3333-456123'),__.property('firmware_version','1.1.0'),__.property('ice_level',2.4),__.addE('mdl').to(__.V().has('_app','test-app').has('__id','device:ice-machine').hasLabel('device-model'))))).by(__.addV('device').property('_app','test-app').property('__id','device:cola-mixer:shop-2').property('__etag','oqielLa9QWeVjd2p9lWZPQ==').sideEffect(__.union(__.property('name','Cola Mixer #789123'),__.property('serial_number','4444-789123'),__.property('firmware_version','1.0.1'),__.property('co2_level',0.2),__.property('syrup_level',0.2),__.addE('mdl').to(__.V().has('_app','test-app').has('__id','device:soda-mixer').hasLabel('device-model'))))).by(__.addV('device').property('_app','test-app').property('__id','device:root-beer-mixer:shop-2').property('__etag','4u7k7lAaSKuUUL2iHbBcRQ==').sideEffect(__.union(__.property('name','Root Beer Mixer #654123'),__.property('serial_number','4444-654123'),__.property('firmware_version','1.0.0'),__.property('co2_level',0.3),__.property('syrup_level',0.3),__.addE('mdl').to(__.V().has('_app','test-app').has('__id','device:soda-mixer').hasLabel('device-model'))))).by(__.addV('device').property('_app','test-app').property('__id','device:lemonade-mixer:shop-2').property('__etag','kkLGbSdzSbiCi7w7VM12gw==').sideEffect(__.union(__.property('name','Lemonade Mixer #654122'),__.property('serial_number','4444-654122'),__.property('firmware_version','1.0.1'),__.property('co2_level',0.4),__.property('syrup_level',0.4),__.addE('mdl').to(__.V().has('_app','test-app').has('__id','device:soda-mixer').hasLabel('device-model'))))).by(__.addV('device').property('_app','test-app').property('__id','device:ice-machine:shop-3.1').property('__etag','cWI7zlmBSNei70b7zoqghw==').sideEffect(__.union(__.property('name','Ice Machine #654111'),__.property('serial_number','3333-654111'),__.property('firmware_version','1.1.1'),__.property('ice_level',3.6),__.addE('mdl').to(__.V().has('_app','test-app').has('__id','device:ice-machine').hasLabel('device-model'))))).by(__.addV('device').property('_app','test-app').property('__id','device:soda-mixer:shop-3.1').property('__etag','yOXsJu84SJW6Amtm9FF9ug==').sideEffect(__.union(__.property('name','Soda Mixer #987456'),__.property('serial_number','4444-987456'),__.property('firmware_version','1.1.2'),__.property('co2_level',0.5),__.property('syrup_level',0.5),__.addE('mdl').to(__.V().has('_app','test-app').has('__id','device:soda-mixer').hasLabel('device-model'))))).by(__.addV('device').property('_app','test-app').property('__id','device:ice-machine:shop-3.2').property('__etag','XTb4lY83SLes2c+gZZ6vfA==').sideEffect(__.union(__.property('name','Ice Machine #555444'),__.property('serial_number','3333-555444'),__.property('firmware_version','1.0.0'),__.property('ice_level',4.8),__.addE('mdl').to(__.V().has('_app','test-app').has('__id','device:ice-machine').hasLabel('device-model'))))).by(__.addV('device').property('_app','test-app').property('__id','device:cola-mixer:shop-3.2').property('__etag','G1lCXUnhRSCqohWUaZza8w==').sideEffect(__.union(__.property('name','Cola Mixer #111222'),__.property('serial_number','4444-111222'),__.property('firmware_version','1.0.0'),__.property('co2_level',0.6),__.property('syrup_level',0.6),__.addE('mdl').to(__.V().has('_app','test-app').has('__id','device:soda-mixer').hasLabel('device-model'))))).by(__.addV('device').property('_app','test-app').property('__id','device:kool-aid-mixer:shop-3.2').property('__etag','E5h6wBBpRjuDWkVaJ/Ud+Q==').sideEffect(__.union(__.property('name','Kool Aid Mixer #999888'),__.property('serial_number','4444-999888'),__.property('firmware_version','1.0.2'),__.property('co2_level',0.7),__.property('syrup_level',0.7),__.addE('mdl').to(__.V().has('_app','test-app').has('__id','device:soda-mixer').hasLabel('device-model'))))).as('#v').project('#e0','#e1','#e2','#e3','#e4','#e5','#e6','#e7','#e8','#e9','#e10','#e11','#e12').by(__.select('#v5').addE('device-product').to(__.select('#v').select('#v0'))).by(__.select('#v6').addE('device-product').to(__.select('#v').select('#v0'))).by(__.select('#v7').addE('device-product').to(__.select('#v').select('#v1'))).by(__.select('#v8').addE('device-product').to(__.select('#v').select('#v1'))).by(__.select('#v9').addE('device-product').to(__.select('#v').select('#v1'))).by(__.select('#v10').addE('device-product').to(__.select('#v').select('#v1'))).by(__.select('#v11').addE('device-product').to(__.select('#v').select('#v2'))).by(__.select('#v12').addE('device-product').to(__.select('#v').select('#v2'))).by(__.select('#v13').addE('device-product').to(__.select('#v').select('#v3'))).by(__.select('#v14').addE('device-product').to(__.select('#v').select('#v3'))).by(__.select('#v15').addE('device-product').to(__.select('#v').select('#v3'))).by(__.select('#v2').addE('product-product').to(__.select('#v').select('#v4'))).by(__.select('#v3').addE('product-product').to(__.select('#v').select('#v4'))).as('#e').union(__.select('#v').union(__.select('#v0').as('#a').constant(['name','location','installer']),__.select('#v1').as('#a').constant(['name','location','installer']),__.select('#v2').as('#a').constant(['name','location','installer']),__.select('#v3').as('#a').constant(['name','location','installer']),__.select('#v4').as('#a').constant(['name','location','installer']),__.select('#v5').as('#a').constant(['name','serial_number','firmware_version','ice_level']),__.select('#v6').as('#a').constant(['name','serial_number','firmware_version','co2_level','syrup_level']),__.select('#v7').as('#a').constant(['name','serial_number','firmware_version','ice_level']),__.select('#v8').as('#a').constant(['name','serial_number','firmware_version','co2_level','syrup_level']),__.select('#v9').as('#a').constant(['name','serial_number','firmware_version','co2_level','syrup_level']),__.select('#v10').as('#a').constant(['name','serial_number','firmware_version','co2_level','syrup_level']),__.select('#v11').as('#a').constant(['name','serial_number','firmware_version','ice_level']),__.select('#v12').as('#a').constant(['name','serial_number','firmware_version','co2_level','syrup_level']),__.select('#v13').as('#a').constant(['name','serial_number','firmware_version','ice_level']),__.select('#v14').as('#a').constant(['name','serial_number','firmware_version','co2_level','syrup_level']),__.select('#v15').as('#a').constant(['name','serial_number','firmware_version','co2_level','syrup_level'])).as('#p'),__.select('#e').union(__.select('#e0'),__.select('#e1'),__.select('#e2'),__.select('#e3'),__.select('#e4'),__.select('#e5'),__.select('#e6'),__.select('#e7'),__.select('#e8'),__.select('#e9'),__.select('#e10'),__.select('#e11'),__.select('#e12')).as('#f').union(__.inV().as('#a').select('#f').outV(),__.outV().as('#a').select('#f').inV()).map(__.optional(__.out('mdl')).as('#m').select('#a').optional(__.out('mdl')).inE('ref').and(__.outV().where(eq('#m'))).values('_key').fold()).as('#p')).select('#a').union(__.identity(),__.as('@v').flatMap(__.optional(__.out('mdl')).inE('ref').and(__.values('_key').where(within('#p')))).repeat(__.as('@e').flatMap(__.outV().as('mdl').select(last,'@v').both().dedup().and(__.optional(__.out('mdl')).where(eq('mdl')))).as('@v').optional(__.flatMap(__.select(last,'@e').values('_key').as('key').select(last,'@v').optional(__.out('mdl')).inE('ref').and(__.values('_ref').where(eq('key')))))).until(__.flatMap(__.as('res').select(last,'@v').where(eq('res')))).select('@v').unfold()).dedup().project('_id','type','_etag').by(__.values('__id')).by(__.label()).by(__.values('__etag')))";
            return graph.ExecuteAndGetResults();
        }

        public List<string> Get_IoT_test_4()
        {
            graph.CommandText =
                "g.V().has('_app','test-app').has('__id','test-app').hasLabel('application').coalesce(__.union(__.not(__.V().has('_app','test-app').has('__id','test-app').hasLabel('application')).constant('~0'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',0).constant('~1'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',2).constant('~2'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_deleted',true).constant('~3')),__.flatMap(__.V().has('_app','test-app').hasLabel('product').range(0,100).union(__.identity().sideEffect(__.id().store('^ids')),__.as('@v').flatMap(__.optional(__.out('mdl')).outE('ref')).repeat(__.as('@e').flatMap(__.inV().as('mdl').select(last,'@v').both().dedup().and(__.optional(__.out('mdl')).where(eq('mdl')))).as('@v').optional(__.flatMap(__.select(last,'@e').values('_ref').as('key').select(last,'@v').optional(__.out('mdl')).outE('ref').and(__.values('_key').where(eq('key')))))).until(__.flatMap(__.as('res').select(last,'@v').where(eq('res')))).sideEffect(__.project('data','info').by(__.select('@e').unfold().project('key','ref').by(__.values('_key')).by(__.values('_ref')).fold()).by(__.select('@v').unfold().project('_id','type','_etag').by(__.values('__id')).by(__.label()).by(__.values('__etag')).fold()).store('^refs'))).dedup().union(__.identity().sideEffect(__.group('^mdls').by(__.id()).by(__.coalesce(__.out('mdl').values('__id'),__.constant('')))),__.out('mdl')).dedup()).union(__.emit().repeat(__.outE('_val').as('_').inV()).tree(),__.cap('^ids'),__.cap('^mdls'),__.cap('^refs')).fold())";
            return graph.ExecuteAndGetResults();
        }

        public List<string> Get_IoT_test_5()
        {
            graph.CommandText =
                "g.V().has('_app','test-app').has('__id','test-app').hasLabel('application').coalesce(__.union(__.not(__.V().has('_app','test-app').has('__id','test-app').hasLabel('application')).constant('~0'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',0).constant('~1'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',2).constant('~2'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_deleted',true).constant('~3')),__.flatMap(__.V().has('_app','test-app').has('__id','uber-product:soda-machine:shop-3').hasLabel('product').range(0,100).union(__.identity().sideEffect(__.id().store('^ids')),__.as('@v').flatMap(__.optional(__.out('mdl')).outE('ref')).repeat(__.as('@e').flatMap(__.inV().as('mdl').select(last,'@v').both().dedup().and(__.optional(__.out('mdl')).where(eq('mdl')))).as('@v').optional(__.flatMap(__.select(last,'@e').values('_ref').as('key').select(last,'@v').optional(__.out('mdl')).outE('ref').and(__.values('_key').where(eq('key')))))).until(__.flatMap(__.as('res').select(last,'@v').where(eq('res')))).sideEffect(__.project('data','info').by(__.select('@e').unfold().project('key','ref').by(__.values('_key')).by(__.values('_ref')).fold()).by(__.select('@v').unfold().project('_id','type','_etag').by(__.values('__id')).by(__.label()).by(__.values('__etag')).fold()).store('^refs'))).dedup().union(__.identity().sideEffect(__.group('^mdls').by(__.id()).by(__.coalesce(__.out('mdl').values('__id'),__.constant('')))),__.out('mdl')).dedup()).union(__.emit().repeat(__.outE('_val').as('_').inV()).tree(),__.cap('^ids'),__.cap('^mdls'),__.cap('^refs')).fold())";
            return graph.ExecuteAndGetResults();
        }

        public List<string> Get_IoT_test_6()
        {
            graph.CommandText =
                "g.V().has('_app','test-app').has('__id','test-app').hasLabel('application').coalesce(__.union(__.not(__.V().has('_app','test-app').has('__id','test-app').hasLabel('application')).constant('~0'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',0).constant('~1'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',2).constant('~2'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_deleted',true).constant('~3')),__.flatMap(__.V().has('_app','test-app').has('__id','uber-product:soda-machine').hasLabel('product-model').range(0,100).union(__.identity().sideEffect(__.id().store('^ids')),__.as('@v').flatMap(__.optional(__.out('mdl')).outE('ref')).repeat(__.as('@e').flatMap(__.inV().as('mdl').select(last,'@v').both().dedup().and(__.optional(__.out('mdl')).where(eq('mdl')))).as('@v').optional(__.flatMap(__.select(last,'@e').values('_ref').as('key').select(last,'@v').optional(__.out('mdl')).outE('ref').and(__.values('_key').where(eq('key')))))).until(__.flatMap(__.as('res').select(last,'@v').where(eq('res')))).sideEffect(__.project('data','info').by(__.select('@e').unfold().project('key','ref').by(__.values('_key')).by(__.values('_ref')).fold()).by(__.select('@v').unfold().project('_id','type','_etag').by(__.values('__id')).by(__.label()).by(__.values('__etag')).fold()).store('^refs'))).dedup().union(__.identity().sideEffect(__.group('^mdls').by(__.id()).by(__.coalesce(__.out('mdl').values('__id'),__.constant('')))),__.out('mdl')).dedup()).union(__.emit().repeat(__.outE('_val').as('_').inV()).tree(),__.cap('^ids'),__.cap('^mdls'),__.cap('^refs')).fold())";
            return graph.ExecuteAndGetResults();
        }

        public List<string> Get_IoT_test_7()
        {
            graph.CommandText =
                "g.V().has('_app','test-app').has('__id','test-app').hasLabel('application').coalesce(__.union(__.not(__.V().has('_app','test-app').has('__id','test-app').hasLabel('application')).constant('~0'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',0).constant('~1'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',2).constant('~2'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_deleted',true).constant('~3')),__.flatMap(__.V().has('_app','test-app').has('__id','product:soda-machine:shop-2').hasLabel('product').inE('device-product').as('_').outV().dedup().range(0,100)).tree().fold())";
            return graph.ExecuteAndGetResults();
        }

        public List<string> Get_IoT_test_8()
        {
            graph.CommandText =
                "g.inject(0).coalesce(__.union(__.not(__.V().has('_app','test-app').has('__id','test-app').hasLabel('application')).constant('~0'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',0).constant('~1'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',2).constant('~2'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_deleted',true).constant('~3'),__.not(__.V().has('_app','test-app').has('__id','uber-product:soda-machine').hasLabel('product-model').has('__etag','SkYTpr1hSkCL4NkpsfNwvQ==')).constant('~4'),__.V().has('_app','test-app').has('__id','uber-product:soda-machine').hasLabel('product-model').in('mdl').local(__.union(__.properties().key(),__.outE('_val').values('_key')).fold()).as('key').flatMap(__.constant(['location','installer']).unfold()).where(without('key')).dedup().constant('~5'),__.not(__.V().has('_app','test-app').has('__id','product:soda-machine').hasLabel('product-model')).constant('~6'),__.not(__.V().has('_app','test-app').has('__id','uber-product:soda-machine').hasLabel('product-model').both().has('_app','test-app').has('__id','product:soda-machine').hasLabel('product-model')).constant('~7')),__.project('#v0','#v1').by(__.V().has('_app','test-app').has('__id','uber-product:soda-machine').hasLabel('product-model').property('__etag','0Ro9MX91RYWT3ZWuot53FA==').sideEffect(__.union(__.sideEffect(__.properties('_properties').drop()).sideEffect(__.sideEffect(__.outE('_val').has('_key','_properties').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','_properties').property('_ary',true).inV().sideEffect(__.properties('0').drop()).sideEffect(__.coalesce(__.outE('_val').has('_key','0').has('_ary',false),__.sideEffect(__.outE('_val').has('_key','0').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','0').property('_ary',false)).inV().sideEffect(__.outE('_val').has('_key','kind').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('kind','property').sideEffect(__.outE('_val').has('_key','name').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('name','Soda machine location').sideEffect(__.outE('_val').has('_key','_id').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('_id','location').sideEffect(__.outE('_val').has('_key','type').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('type','string')).sideEffect(__.properties('1').drop()).sideEffect(__.coalesce(__.outE('_val').has('_key','1').has('_ary',false),__.sideEffect(__.outE('_val').has('_key','1').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','1').property('_ary',false)).inV().sideEffect(__.outE('_val').has('_key','kind').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('kind','property').sideEffect(__.outE('_val').has('_key','name').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('name','Soda machine installer').sideEffect(__.outE('_val').has('_key','_id').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('_id','installer').sideEffect(__.outE('_val').has('_key','type').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('type','string')).sideEffect(__.properties('2').drop()).sideEffect(__.coalesce(__.outE('_val').has('_key','2').has('_ary',false),__.sideEffect(__.outE('_val').has('_key','2').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','2').property('_ary',false)).inV().sideEffect(__.outE('_val').has('_key','kind').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('kind','reference').sideEffect(__.outE('_val').has('_key','name').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('name','Syrup Level').sideEffect(__.outE('_val').has('_key','_id').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('_id','syrup_level').sideEffect(__.properties('target').drop()).sideEffect(__.coalesce(__.outE('_val').has('_key','target').has('_ary',false),__.sideEffect(__.outE('_val').has('_key','target').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','target').property('_ary',false)).inV().sideEffect(__.outE('_val').has('_key','_id').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('_id','product:soda-machine').sideEffect(__.outE('_val').has('_key','type').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('type','product'))).sideEffect(__.properties('3').drop()).sideEffect(__.coalesce(__.outE('_val').has('_key','3').has('_ary',false),__.sideEffect(__.outE('_val').has('_key','3').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','3').property('_ary',false)).inV().sideEffect(__.outE('_val').has('_key','kind').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('kind','reference').sideEffect(__.outE('_val').has('_key','name').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('name','Ice Level').sideEffect(__.outE('_val').has('_key','_id').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('_id','ice_level').sideEffect(__.properties('target').drop()).sideEffect(__.coalesce(__.outE('_val').has('_key','target').has('_ary',false),__.sideEffect(__.outE('_val').has('_key','target').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','target').property('_ary',false)).inV().sideEffect(__.outE('_val').has('_key','_id').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('_id','product:soda-machine').sideEffect(__.outE('_val').has('_key','type').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('type','product'))))))).by(__.V().has('_app','test-app').has('__id','product:soda-machine').hasLabel('product-model').property('__etag','iBuelvJFQuSGRQfEvvzPrA==')).as('#v').project('#e0','#e1').by(__.select('#v0').flatMap(__.as('src').flatMap(__.select('#v').select('#v1')).as('tgt').select('src').coalesce(__.outE('ref').and(__.inV().where(eq('tgt'))).has('_key','syrup_level').has('_ref','syrup_level'),__.addE('ref').to('tgt').property('_key','syrup_level').property('_ref','syrup_level')))).by(__.select('#v0').flatMap(__.as('src').flatMap(__.select('#v').select('#v1')).as('tgt').select('src').coalesce(__.outE('ref').and(__.inV().where(eq('tgt'))).has('_key','ice_level').has('_ref','ice_level'),__.addE('ref').to('tgt').property('_key','ice_level').property('_ref','ice_level')))).as('#e').union(__.select('#v').union(__.select('#v0').as('#a').constant(['_properties'])).as('#p'),__.select('#e').union(__.select('#e0'),__.select('#e1')).as('#f').union(__.inV().as('#a').select('#f').outV(),__.outV().as('#a').select('#f').inV()).map(__.optional(__.out('mdl')).as('#m').select('#a').optional(__.out('mdl')).inE('ref').and(__.outV().where(eq('#m'))).values('_key').fold()).as('#p')).select('#a').union(__.identity(),__.as('@v').flatMap(__.optional(__.out('mdl')).inE('ref').and(__.values('_key').where(within('#p')))).repeat(__.as('@e').flatMap(__.outV().as('mdl').select(last,'@v').both().dedup().and(__.optional(__.out('mdl')).where(eq('mdl')))).as('@v').optional(__.flatMap(__.select(last,'@e').values('_key').as('key').select(last,'@v').optional(__.out('mdl')).inE('ref').and(__.values('_ref').where(eq('key')))))).until(__.flatMap(__.as('res').select(last,'@v').where(eq('res')))).select('@v').unfold()).dedup().project('_id','type','_etag').by(__.values('__id')).by(__.label()).by(__.values('__etag')))";
            return graph.ExecuteAndGetResults();
        }

        public List<string> Get_IoT_test_9_prefetch()
        {
            graph.CommandText =
                "g.V().has('_app','test-app').has('__id','test-app').hasLabel('application').coalesce(__.union(__.not(__.V().has('_app','test-app').has('__id','test-app').hasLabel('application')).constant('~0'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',0).constant('~1'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',2).constant('~2'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_deleted',true).constant('~3')),__.flatMap(__.project('nodes','edges').by(__.union(__.V().has('_app','test-app').has('__id','device:soda-mixer:shop-1').hasLabel('device'),__.V().has('_app','test-app').has('__id','product:soda-machine:shop-2').hasLabel('product')).fold()).by(__.union(__.V().has('_app','test-app').has('__id','device:soda-mixer:shop-1').hasLabel('device').out('mdl').flatMap(__.as('src').flatMap(__.V().has('_app','test-app').has('__id','product:soda-machine:shop-2').hasLabel('product').out('mdl')).as('tgt').select('src').outE('device-product').and(__.inV().where(eq('tgt'))))).fold()).sideEffect(__.select('edges').unfold().project('name','source','target','properties').by(__.label()).by(__.outV().project('_id','type','_etag').by(__.values('__id')).by(__.label()).by(__.values('__etag'))).by(__.inV().project('_id','type','_etag').by(__.values('__id')).by(__.label()).by(__.values('__etag'))).by(__.properties().group().by(__.key()).by(__.value())).store('^edges')).select('nodes').unfold().union(__.identity().sideEffect(__.id().store('^ids')),__.as('@v').flatMap(__.optional(__.out('mdl')).outE('ref')).repeat(__.as('@e').flatMap(__.inV().as('mdl').select(last,'@v').both().dedup().and(__.optional(__.out('mdl')).where(eq('mdl')))).as('@v').optional(__.flatMap(__.select(last,'@e').values('_ref').as('key').select(last,'@v').optional(__.out('mdl')).outE('ref').and(__.values('_key').where(eq('key')))))).until(__.flatMap(__.as('res').select(last,'@v').where(eq('res')))).sideEffect(__.project('data','info').by(__.select('@e').unfold().project('key','ref').by(__.values('_key')).by(__.values('_ref')).fold()).by(__.select('@v').unfold().project('_id','type','_etag').by(__.values('__id')).by(__.label()).by(__.values('__etag')).fold()).store('^refs'))).dedup().union(__.identity().sideEffect(__.group('^mdls').by(__.id()).by(__.coalesce(__.out('mdl').values('__id'),__.constant('')))),__.out('mdl')).dedup()).union(__.emit().repeat(__.outE('_val').as('_').inV()).tree(),__.cap('^ids'),__.cap('^mdls'),__.cap('^refs')).fold().union(__.identity(),__.cap('^edges')))";
            return graph.ExecuteAndGetResults();
        }

        public List<string> Get_IoT_test_9()
        {
            graph.CommandText =
                "g.inject(0).coalesce(__.union(__.not(__.V().has('_app','test-app').has('__id','test-app').hasLabel('application')).constant('~0'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',0).constant('~1'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',2).constant('~2'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_deleted',true).constant('~3'),__.not(__.V().has('_app','test-app').has('__id','device:soda-mixer:shop-1').hasLabel('device')).constant('~4'),__.not(__.V().has('_app','test-app').has('__id','product:soda-machine:shop-2').hasLabel('product')).constant('~5')),__.project('#v0','#v1').by(__.V().has('_app','test-app').has('__id','device:soda-mixer:shop-1').hasLabel('device').property('__etag','LmyeSEx1RL+cIZRKKvFPvA==')).by(__.V().has('_app','test-app').has('__id','product:soda-machine:shop-2').hasLabel('product').property('__etag','2dGQ3DDwSUKY2Jv+9K9t3A==')).as('#v').project('#e0').by(__.select('#v0').flatMap(__.as('src').flatMap(__.select('#v').select('#v1')).as('tgt').select('src').coalesce(__.outE('device-product').and(__.inV().where(eq('tgt'))),__.addE('device-product').to('tgt')))).as('#e').union(__.select('#e').union(__.select('#e0')).as('#f').union(__.inV().as('#a').select('#f').outV(),__.outV().as('#a').select('#f').inV()).map(__.optional(__.out('mdl')).as('#m').select('#a').optional(__.out('mdl')).inE('ref').and(__.outV().where(eq('#m'))).values('_key').fold()).as('#p')).select('#a').union(__.identity(),__.as('@v').flatMap(__.optional(__.out('mdl')).inE('ref').and(__.values('_key').where(within('#p')))).repeat(__.as('@e').flatMap(__.outV().as('mdl').select(last,'@v').both().dedup().and(__.optional(__.out('mdl')).where(eq('mdl')))).as('@v').optional(__.flatMap(__.select(last,'@e').values('_key').as('key').select(last,'@v').optional(__.out('mdl')).inE('ref').and(__.values('_ref').where(eq('key')))))).until(__.flatMap(__.as('res').select(last,'@v').where(eq('res')))).select('@v').unfold()).dedup().project('_id','type','_etag').by(__.values('__id')).by(__.label()).by(__.values('__etag')))";
            return graph.ExecuteAndGetResults();
        }

        public List<string> Get_IoT_test_10_prefetch()
        {
            graph.CommandText =
                "g.V().has('_app','test-app').has('__id','test-app').hasLabel('application').coalesce(__.union(__.not(__.V().has('_app','test-app').has('__id','test-app').hasLabel('application')).constant('~0'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',0).constant('~1'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',2).constant('~2'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_deleted',true).constant('~3')),__.flatMap(__.project('nodes','edges').by(__.union(__.V().has('_app','test-app').has('__id','uber-product:soda-machine:shop-3').hasLabel('product')).fold()).by(__.union().fold()).sideEffect(__.select('edges').unfold().project('name','source','target','properties').by(__.label()).by(__.outV().project('_id','type','_etag').by(__.values('__id')).by(__.label()).by(__.values('__etag'))).by(__.inV().project('_id','type','_etag').by(__.values('__id')).by(__.label()).by(__.values('__etag'))).by(__.properties().group().by(__.key()).by(__.value())).store('^edges')).select('nodes').unfold().union(__.identity().sideEffect(__.id().store('^ids')),__.as('@v').flatMap(__.optional(__.out('mdl')).outE('ref')).repeat(__.as('@e').flatMap(__.inV().as('mdl').select(last,'@v').both().dedup().and(__.optional(__.out('mdl')).where(eq('mdl')))).as('@v').optional(__.flatMap(__.select(last,'@e').values('_ref').as('key').select(last,'@v').optional(__.out('mdl')).outE('ref').and(__.values('_key').where(eq('key')))))).until(__.flatMap(__.as('res').select(last,'@v').where(eq('res')))).sideEffect(__.project('data','info').by(__.select('@e').unfold().project('key','ref').by(__.values('_key')).by(__.values('_ref')).fold()).by(__.select('@v').unfold().project('_id','type','_etag').by(__.values('__id')).by(__.label()).by(__.values('__etag')).fold()).store('^refs'))).dedup().union(__.identity().sideEffect(__.group('^mdls').by(__.id()).by(__.coalesce(__.out('mdl').values('__id'),__.constant('')))),__.out('mdl')).dedup()).union(__.emit().repeat(__.outE('_val').as('_').inV()).tree(),__.cap('^ids'),__.cap('^mdls'),__.cap('^refs')).fold().union(__.identity(),__.cap('^edges')))";
            return graph.ExecuteAndGetResults();
        }

        public List<string> Get_IoT_test_10()
        {
            graph.CommandText =
                "g.inject(0).coalesce(__.union(__.not(__.V().has('_app','test-app').has('__id','test-app').hasLabel('application')).constant('~0'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',0).constant('~1'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',2).constant('~2'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_deleted',true).constant('~3'),__.not(__.V().has('_app','test-app').has('__id','uber-product:soda-machine:shop-3').hasLabel('product').has('__etag','TMaJk/CGRyurJIle/FncMA==')).constant('~4'),__.not(__.V().has('_app','test-app').has('__id','uber-product:soda-machine').hasLabel('product-model').has('__etag','0Ro9MX91RYWT3ZWuot53FA==')).constant('~5'),__.not(__.V().has('_app','test-app').has('__id','device:cola-mixer:shop-3.2').hasLabel('device').has('__etag','G1lCXUnhRSCqohWUaZza8w==')).constant('~6'),__.not(__.V().has('_app','test-app').has('__id','device:kool-aid-mixer:shop-3.2').hasLabel('device').has('__etag','E5h6wBBpRjuDWkVaJ/Ud+Q==')).constant('~7'),__.not(__.V().has('_app','test-app').has('__id','device:soda-mixer:shop-3.1').hasLabel('device').has('__etag','yOXsJu84SJW6Amtm9FF9ug==')).constant('~8'),__.not(__.V().has('_app','test-app').has('__id','device:ice-machine:shop-3.2').hasLabel('device').has('__etag','XTb4lY83SLes2c+gZZ6vfA==')).constant('~9'),__.not(__.V().has('_app','test-app').has('__id','device:ice-machine:shop-3.1').hasLabel('device').has('__etag','cWI7zlmBSNei70b7zoqghw==')).constant('~10')),__.project('#v0').by(__.V().has('_app','test-app').has('__id','uber-product:soda-machine:shop-3').hasLabel('product').property('__etag','lunRO6wJQg6WMNq/CGr7QA==').sideEffect(__.union(__.sideEffect(__.outE('_val').has('_key','name').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('name','Uber Soda Machine #3 - New Name')))).as('#v').union(__.select('#v').union(__.select('#v0').as('#a').constant(['name'])).as('#p')).select('#a').union(__.identity(),__.as('@v').flatMap(__.optional(__.out('mdl')).inE('ref').and(__.values('_key').where(within('#p')))).repeat(__.as('@e').flatMap(__.outV().as('mdl').select(last,'@v').both().dedup().and(__.optional(__.out('mdl')).where(eq('mdl')))).as('@v').optional(__.flatMap(__.select(last,'@e').values('_key').as('key').select(last,'@v').optional(__.out('mdl')).inE('ref').and(__.values('_ref').where(eq('key')))))).until(__.flatMap(__.as('res').select(last,'@v').where(eq('res')))).select('@v').unfold()).dedup().project('_id','type','_etag').by(__.values('__id')).by(__.label()).by(__.values('__etag')))";
            return graph.ExecuteAndGetResults();
        }

        public List<string> Get_IoT_test_11_prefetch()
        {
            graph.CommandText =
                "g.V().has('_app','test-app').has('__id','test-app').hasLabel('application').coalesce(__.union(__.not(__.V().has('_app','test-app').has('__id','test-app').hasLabel('application')).constant('~0'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',0).constant('~1'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',2).constant('~2'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_deleted',true).constant('~3')),__.flatMap(__.project('nodes','edges').by(__.union(__.V().has('_app','test-app').has('__id','uber-product:soda-machine:shop-3').hasLabel('product')).fold()).by(__.union().fold()).sideEffect(__.select('edges').unfold().project('name','source','target','properties').by(__.label()).by(__.outV().project('_id','type','_etag').by(__.values('__id')).by(__.label()).by(__.values('__etag'))).by(__.inV().project('_id','type','_etag').by(__.values('__id')).by(__.label()).by(__.values('__etag'))).by(__.properties().group().by(__.key()).by(__.value())).store('^edges')).select('nodes').unfold().union(__.identity().sideEffect(__.id().store('^ids')),__.as('@v').flatMap(__.optional(__.out('mdl')).outE('ref')).repeat(__.as('@e').flatMap(__.inV().as('mdl').select(last,'@v').both().dedup().and(__.optional(__.out('mdl')).where(eq('mdl')))).as('@v').optional(__.flatMap(__.select(last,'@e').values('_ref').as('key').select(last,'@v').optional(__.out('mdl')).outE('ref').and(__.values('_key').where(eq('key')))))).until(__.flatMap(__.as('res').select(last,'@v').where(eq('res')))).sideEffect(__.project('data','info').by(__.select('@e').unfold().project('key','ref').by(__.values('_key')).by(__.values('_ref')).fold()).by(__.select('@v').unfold().project('_id','type','_etag').by(__.values('__id')).by(__.label()).by(__.values('__etag')).fold()).store('^refs'))).dedup().union(__.identity().sideEffect(__.group('^mdls').by(__.id()).by(__.coalesce(__.out('mdl').values('__id'),__.constant('')))),__.out('mdl')).dedup()).union(__.emit().repeat(__.outE('_val').as('_').inV()).tree(),__.cap('^ids'),__.cap('^mdls'),__.cap('^refs')).fold().union(__.identity(),__.cap('^edges')))";
            return graph.ExecuteAndGetResults();
        }

        public List<string> Get_IoT_test_11()
        {
            graph.CommandText =
                "g.inject(0).coalesce(__.union(__.not(__.V().has('_app','test-app').has('__id','test-app').hasLabel('application')).constant('~0'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',0).constant('~1'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',2).constant('~2'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_deleted',true).constant('~3'),__.not(__.V().has('_app','test-app').has('__id','uber-product:soda-machine:shop-3').hasLabel('product').has('__etag','lunRO6wJQg6WMNq/CGr7QA==')).constant('~4'),__.not(__.V().has('_app','test-app').has('__id','uber-product:soda-machine').hasLabel('product-model').has('__etag','0Ro9MX91RYWT3ZWuot53FA==')).constant('~5'),__.not(__.V().has('_app','test-app').has('__id','device:cola-mixer:shop-3.2').hasLabel('device').has('__etag','G1lCXUnhRSCqohWUaZza8w==')).constant('~6'),__.not(__.V().has('_app','test-app').has('__id','device:kool-aid-mixer:shop-3.2').hasLabel('device').has('__etag','E5h6wBBpRjuDWkVaJ/Ud+Q==')).constant('~7'),__.not(__.V().has('_app','test-app').has('__id','device:soda-mixer:shop-3.1').hasLabel('device').has('__etag','yOXsJu84SJW6Amtm9FF9ug==')).constant('~8'),__.not(__.V().has('_app','test-app').has('__id','device:ice-machine:shop-3.2').hasLabel('device').has('__etag','XTb4lY83SLes2c+gZZ6vfA==')).constant('~9'),__.not(__.V().has('_app','test-app').has('__id','device:ice-machine:shop-3.1').hasLabel('device').has('__etag','cWI7zlmBSNei70b7zoqghw==')).constant('~10'),__.not(__.V().has('_app','test-app').has('__id','device:soda-mixer').hasLabel('device-model').has('__etag','lsRrd7JWSBqW9kiBVPS7aQ==')).constant('~11')),__.project('#v0','#v1','#v2','#v3').by(__.V().has('_app','test-app').has('__id','uber-product:soda-machine:shop-3').hasLabel('product').property('__etag','yzm2GRluTOim/fvMmuxh2g==')).by(__.V().has('_app','test-app').has('__id','device:cola-mixer:shop-3.2').hasLabel('device').property('__etag','aj+sec3TRnCF1mwWDErzqA==').sideEffect(__.union(__.sideEffect(__.properties('_twin').drop()).sideEffect(__.coalesce(__.outE('_val').has('_key','_twin').has('_ary',false),__.sideEffect(__.outE('_val').has('_key','_twin').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','_twin').property('_ary',false)).inV().sideEffect(__.properties('reported').drop()).sideEffect(__.coalesce(__.outE('_val').has('_key','reported').has('_ary',false),__.sideEffect(__.outE('_val').has('_key','reported').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','reported').property('_ary',false)).inV().sideEffect(__.outE('_val').has('_key','syrup_level').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('syrup_level',2.3))),__.sideEffect(__.outE('_val').has('_key','co2_level').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('co2_level',0.6),__.sideEffect(__.outE('_val').has('_key','name').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('name','Cola Mixer #111222'),__.sideEffect(__.outE('_val').has('_key','serial_number').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('serial_number','4444-111222'),__.sideEffect(__.outE('_val').has('_key','firmware_version').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('firmware_version','1.0.0'),__.sideEffect(__.outE('mdl').drop()).addE('mdl').to(__.V().has('_app','test-app').has('__id','device:soda-mixer').hasLabel('device-model'))))).by(__.V().has('_app','test-app').has('__id','device:kool-aid-mixer:shop-3.2').hasLabel('device').property('__etag','k0maOZ1/QF+d9fn7WR8YWQ==').sideEffect(__.union(__.sideEffect(__.properties('_twin').drop()).sideEffect(__.coalesce(__.outE('_val').has('_key','_twin').has('_ary',false),__.sideEffect(__.outE('_val').has('_key','_twin').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','_twin').property('_ary',false)).inV().sideEffect(__.properties('reported').drop()).sideEffect(__.coalesce(__.outE('_val').has('_key','reported').has('_ary',false),__.sideEffect(__.outE('_val').has('_key','reported').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','reported').property('_ary',false)).inV().sideEffect(__.outE('_val').has('_key','syrup_level').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('syrup_level',2.3))),__.sideEffect(__.outE('_val').has('_key','co2_level').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('co2_level',0.7),__.sideEffect(__.outE('_val').has('_key','name').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('name','Kool Aid Mixer #999888'),__.sideEffect(__.outE('_val').has('_key','serial_number').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('serial_number','4444-999888'),__.sideEffect(__.outE('_val').has('_key','firmware_version').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('firmware_version','1.0.2'),__.sideEffect(__.outE('mdl').drop()).addE('mdl').to(__.V().has('_app','test-app').has('__id','device:soda-mixer').hasLabel('device-model'))))).by(__.V().has('_app','test-app').has('__id','device:soda-mixer:shop-3.1').hasLabel('device').property('__etag','OP8/P5nKSUyWscCtNgGstw==').sideEffect(__.union(__.sideEffect(__.properties('_twin').drop()).sideEffect(__.coalesce(__.outE('_val').has('_key','_twin').has('_ary',false),__.sideEffect(__.outE('_val').has('_key','_twin').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','_twin').property('_ary',false)).inV().sideEffect(__.properties('reported').drop()).sideEffect(__.coalesce(__.outE('_val').has('_key','reported').has('_ary',false),__.sideEffect(__.outE('_val').has('_key','reported').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','reported').property('_ary',false)).inV().sideEffect(__.outE('_val').has('_key','syrup_level').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('syrup_level',2.3))),__.sideEffect(__.outE('_val').has('_key','co2_level').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('co2_level',0.5),__.sideEffect(__.outE('_val').has('_key','name').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('name','Soda Mixer #987456'),__.sideEffect(__.outE('_val').has('_key','serial_number').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('serial_number','4444-987456'),__.sideEffect(__.outE('_val').has('_key','firmware_version').inV().sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())).drop()).property('firmware_version','1.1.2'),__.sideEffect(__.outE('mdl').drop()).addE('mdl').to(__.V().has('_app','test-app').has('__id','device:soda-mixer').hasLabel('device-model'))))).as('#v').union(__.select('#v').union(__.select('#v0').as('#a').constant(['syrup_level']),__.select('#v1').as('#a').constant(['syrup_level','co2_level','name','serial_number','firmware_version']),__.select('#v2').as('#a').constant(['syrup_level','co2_level','name','serial_number','firmware_version']),__.select('#v3').as('#a').constant(['syrup_level','co2_level','name','serial_number','firmware_version'])).as('#p')).select('#a').union(__.identity(),__.as('@v').flatMap(__.optional(__.out('mdl')).inE('ref').and(__.values('_key').where(within('#p')))).repeat(__.as('@e').flatMap(__.outV().as('mdl').select(last,'@v').both().dedup().and(__.optional(__.out('mdl')).where(eq('mdl')))).as('@v').optional(__.flatMap(__.select(last,'@e').values('_key').as('key').select(last,'@v').optional(__.out('mdl')).inE('ref').and(__.values('_ref').where(eq('key')))))).until(__.flatMap(__.as('res').select(last,'@v').where(eq('res')))).select('@v').unfold()).dedup().project('_id','type','_etag').by(__.values('__id')).by(__.label()).by(__.values('__etag')))";
            return graph.ExecuteAndGetResults();
        }

        public List<string> Get_IoT_test_12()
        {
            graph.CommandText =
                "g.V().has('_app','test-app').has('__id','test-app').hasLabel('application').coalesce(__.union(__.not(__.V().has('_app','test-app').has('__id','test-app').hasLabel('application')).constant('~0'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',0).constant('~1'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',2).constant('~2'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_deleted',true).constant('~3')),__.flatMap(__.project('nodes','edges').by(__.union(__.V().has('_app','test-app').has('__id','uber-product:soda-machine:shop-3').hasLabel('product')).fold()).by(__.union().fold()).sideEffect(__.select('edges').unfold().project('name','source','target','properties').by(__.label()).by(__.outV().project('_id','type','_etag').by(__.values('__id')).by(__.label()).by(__.values('__etag'))).by(__.inV().project('_id','type','_etag').by(__.values('__id')).by(__.label()).by(__.values('__etag'))).by(__.properties().group().by(__.key()).by(__.value())).store('^edges')).select('nodes').unfold().union(__.identity().sideEffect(__.id().store('^ids')),__.as('@v').flatMap(__.optional(__.out('mdl')).outE('ref')).repeat(__.as('@e').flatMap(__.inV().as('mdl').select(last,'@v').both().dedup().and(__.optional(__.out('mdl')).where(eq('mdl')))).as('@v').optional(__.flatMap(__.select(last,'@e').values('_ref').as('key').select(last,'@v').optional(__.out('mdl')).outE('ref').and(__.values('_key').where(eq('key')))))).until(__.flatMap(__.as('res').select(last,'@v').where(eq('res')))).sideEffect(__.project('data','info').by(__.select('@e').unfold().project('key','ref').by(__.values('_key')).by(__.values('_ref')).fold()).by(__.select('@v').unfold().project('_id','type','_etag').by(__.values('__id')).by(__.label()).by(__.values('__etag')).fold()).store('^refs'))).dedup().union(__.identity().sideEffect(__.group('^mdls').by(__.id()).by(__.coalesce(__.out('mdl').values('__id'),__.constant('')))),__.out('mdl')).dedup()).union(__.emit().repeat(__.outE('_val').as('_').inV()).tree(),__.cap('^ids'),__.cap('^mdls'),__.cap('^refs')).fold().union(__.identity(),__.cap('^edges')))";
            return graph.ExecuteAndGetResults();
        }

        public List<string> Get_IoT_test_13()
        {
            graph.CommandText =
                "g.V().has('_app','test-app').has('__id','test-app').hasLabel('application').coalesce(__.union(__.not(__.V().has('_app','test-app').has('__id','test-app').hasLabel('application')).constant('~0'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',0).constant('~1'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',2).constant('~2'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_deleted',true).constant('~3')),__.flatMap(__.V().has('_app','test-app').has('__id','uber-product:soda-machine:shop-3').hasLabel('product').range(0,100).union(__.identity().sideEffect(__.id().store('^ids')),__.as('@v').flatMap(__.optional(__.out('mdl')).outE('ref')).repeat(__.as('@e').flatMap(__.inV().as('mdl').select(last,'@v').both().dedup().and(__.optional(__.out('mdl')).where(eq('mdl')))).as('@v').optional(__.flatMap(__.select(last,'@e').values('_ref').as('key').select(last,'@v').optional(__.out('mdl')).outE('ref').and(__.values('_key').where(eq('key')))))).until(__.flatMap(__.as('res').select(last,'@v').where(eq('res')))).sideEffect(__.project('data','info').by(__.select('@e').unfold().project('key','ref').by(__.values('_key')).by(__.values('_ref')).fold()).by(__.select('@v').unfold().project('_id','type','_etag').by(__.values('__id')).by(__.label()).by(__.values('__etag')).fold()).store('^refs'))).dedup().union(__.identity().sideEffect(__.group('^mdls').by(__.id()).by(__.coalesce(__.out('mdl').values('__id'),__.constant('')))),__.out('mdl')).dedup()).union(__.emit().repeat(__.outE('_val').as('_').inV()).tree(),__.cap('^ids'),__.cap('^mdls'),__.cap('^refs')).fold())";
            return graph.ExecuteAndGetResults();
        }

        public List<string> Get_IoT_test_14()
        {
            graph.CommandText =
                "g.inject(0).coalesce(__.union(__.not(__.V().has('_app','test-app').has('__id','test-app').hasLabel('application')).constant('~0'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',0).constant('~1'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',2).constant('~2'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_deleted',true).constant('~3')),__.project('#v0').by(__.coalesce(__.V().has('_app','test-app').has('__id','uber-product:soda-machine:shop-3').hasLabel('product').sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())),__.constant(''))).as('#v').map(__.union(__.select('#v').union(__.select('#v0').is(neq('')).as('#a').constant('')).as('#p')).select('#a').union(__.identity(),__.as('@v').flatMap(__.optional(__.out('mdl')).inE('ref').or(__.select('#p').is(''),__.values('_key').where(within('#p')))).repeat(__.as('@e').flatMap(__.outV().as('mdl').select(last,'@v').both().dedup().and(__.optional(__.out('mdl')).where(eq('mdl')))).as('@v').optional(__.flatMap(__.select(last,'@e').values('_key').as('key').select(last,'@v').optional(__.out('mdl')).inE('ref').and(__.values('_ref').where(eq('key')))))).until(__.flatMap(__.as('res').select(last,'@v').where(eq('res')))).select('@v').unfold()).dedup().fold()).as('#r').map(__.union(__.select('#v').union(__.select('#v0').is(neq('')))).fold()).as('#d').map(__.select('#r').unfold().where(without('#d')).fold()).sideEffect(__.select('#d').unfold().drop()).unfold().project('_id','type','_etag').by(__.values('__id')).by(__.label()).by(__.values('__etag')))";
            return graph.ExecuteAndGetResults();
        }

        public List<string> Get_IoT_test_15()
        {
            graph.CommandText =
                "g.inject(0).coalesce(__.union(__.not(__.V().has('_app','test-app').has('__id','test-app').hasLabel('application')).constant('~0'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',0).constant('~1'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',2).constant('~2'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_deleted',true).constant('~3'),__.V().has('_app','test-app').has('__id','uber-product:soda-machine').hasLabel('product-model').in('mdl').constant('~4')),__.project('#v0').by(__.coalesce(__.V().has('_app','test-app').has('__id','uber-product:soda-machine').hasLabel('product-model').sideEffect(__.union(__.properties().drop(),__.repeat(__.out('_val')).emit().barrier().drop())),__.constant(''))).as('#v').map(__.union(__.select('#v').union(__.select('#v0').is(neq('')).as('#a').constant('')).as('#p')).select('#a').union(__.identity(),__.as('@v').flatMap(__.optional(__.out('mdl')).inE('ref').or(__.select('#p').is(''),__.values('_key').where(within('#p')))).repeat(__.as('@e').flatMap(__.outV().as('mdl').select(last,'@v').both().dedup().and(__.optional(__.out('mdl')).where(eq('mdl')))).as('@v').optional(__.flatMap(__.select(last,'@e').values('_key').as('key').select(last,'@v').optional(__.out('mdl')).inE('ref').and(__.values('_ref').where(eq('key')))))).until(__.flatMap(__.as('res').select(last,'@v').where(eq('res')))).select('@v').unfold()).dedup().fold()).as('#r').map(__.union(__.select('#v').union(__.select('#v0').is(neq('')))).fold()).as('#d').map(__.select('#r').unfold().where(without('#d')).fold()).sideEffect(__.select('#d').unfold().drop()).unfold().project('_id','type','_etag').by(__.values('__id')).by(__.label()).by(__.values('__etag')))";
            return graph.ExecuteAndGetResults();
        }

        public List<string> Get_IoT_test_16()
        {
            graph.CommandText =
                "g.V().has('_app','test-app').has('__id','test-app').hasLabel('application').coalesce(__.union(__.not(__.V().has('_app','test-app').has('__id','test-app').hasLabel('application')).constant('~0'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',0).constant('~1'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_provisioningState',2).constant('~2'),__.V().has('_app','test-app').has('__id','test-app').hasLabel('application').has('_deleted',true).constant('~3')),__.flatMap(__.V().has('_app','test-app').has('__id','uber-product:soda-machine:shop-3').hasLabel('product').range(0,100).union(__.identity().sideEffect(__.id().store('^ids')),__.as('@v').flatMap(__.optional(__.out('mdl')).outE('ref')).repeat(__.as('@e').flatMap(__.inV().as('mdl').select(last,'@v').both().dedup().and(__.optional(__.out('mdl')).where(eq('mdl')))).as('@v').optional(__.flatMap(__.select(last,'@e').values('_ref').as('key').select(last,'@v').optional(__.out('mdl')).outE('ref').and(__.values('_key').where(eq('key')))))).until(__.flatMap(__.as('res').select(last,'@v').where(eq('res')))).sideEffect(__.project('data','info').by(__.select('@e').unfold().project('key','ref').by(__.values('_key')).by(__.values('_ref')).fold()).by(__.select('@v').unfold().project('_id','type','_etag').by(__.values('__id')).by(__.label()).by(__.values('__etag')).fold()).store('^refs'))).dedup().union(__.identity().sideEffect(__.group('^mdls').by(__.id()).by(__.coalesce(__.out('mdl').values('__id'),__.constant('')))),__.out('mdl')).dedup()).union(__.emit().repeat(__.outE('_val').as('_').inV()).tree(),__.cap('^ids'),__.cap('^mdls'),__.cap('^refs')).fold())";
            return graph.ExecuteAndGetResults();
        }

        public List<string> Get_IoT_test_17()
        {
            graph.CommandText =
                "g.V().has('_app', 'test-app').drop()";
            return graph.ExecuteAndGetResults();
        }
    }


    public partial class IoTUnitTest
    {

        [TestMethod]
        [Ignore]
        public void TestNewIoTTest()
        {
            var results = Get_query_processed_1();
            results = Get_query_processed_2();
            results = Get_query_processed_3();
            results = Get_query_processed_4();
            results = Get_query_processed_5();
            results = Get_query_processed_6();
            results = Get_query_processed_7();
            results = Get_query_processed_8();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }


        public List<string> Get_query_processed_1()
        {
            graph.CommandText =
                "g.inject(0).coalesce(__.union(__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').constant('~0')),__.addV('application').property('|app','ed011feb-0db1-40de-b633-9ec16b758259').property('|provisioning',0).property('|deleted',false))";
            return graph.ExecuteAndGetResults();
        }

        public List<string> Get_query_processed_2()
        {
            graph.CommandText =
                "g.inject(0).coalesce(__.union(__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',1).constant('~0'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',2).constant('~1'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|deleted',true).constant('~2')),__.coalesce(__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application'),__.addV('application').property('|app','ed011feb-0db1-40de-b633-9ec16b758259')).property('|provisioning',1).property('|deleted',false))";
            return graph.ExecuteAndGetResults();
        }

        public List<string> Get_query_processed_3()
        {
            //return graph.g().Inject(0).Coalesce(GraphTraversal.__().Union(GraphTraversal.__().Not(GraphTraversal.__().V().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").HasLabel("application")).Constant("~0"), GraphTraversal.__().V().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").HasLabel("application").Has("|provisioning", 0).Constant("~1"), GraphTraversal.__().V().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").HasLabel("application").Has("|provisioning", 2).Constant("~2"), GraphTraversal.__().V().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").HasLabel("application").Has("|deleted", true).Constant("~3"), GraphTraversal.__().V().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Has("/_id", "product-instance-1").Has("|v0", 1).Has("|v1", 0).HasLabel("instance").Constant("~4"), GraphTraversal.__().V().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Has("/_id", "product-instance-2").Has("|v0", 1).Has("|v1", 0).HasLabel("instance").Constant("~5"), GraphTraversal.__().V().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Has("/_id", "device-instance-1").Has("|v0", 1).Has("|v1", 0).HasLabel("instance").Constant("~6"), GraphTraversal.__().V().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Has("/_id", "device-instance-2").Has("|v0", 1).Has("|v1", 0).HasLabel("instance").Constant("~7"), GraphTraversal.__().V().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Has("/_id", "product-model").Has("|v0", 1).Has("|v1", 0).HasLabel("model").Constant("~8"), GraphTraversal.__().V().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Has("/_id", "device-model").Has("|v0", 1).Has("|v1", 0).HasLabel("model").Constant("~9"), GraphTraversal.__().V().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Has("/_id", "product-instance-1").Has("|v0", 1).Has("|v1", 0).HasLabel("instance").OutE().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Has("/_id", "product-to-device-1").Has("|v0", 1).Has("|v1", 0).HasLabel("instance").Constant("~10"), GraphTraversal.__().V().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Has("/_id", "product-model").Has("|v0", 1).Has("|v1", 0).HasLabel("model").OutE().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Has("/_id", "product-to-device-model").Has("|v0", 1).Has("|v1", 0).HasLabel("model").Constant("~11"), GraphTraversal.__().V().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Has("/_id", "product-model").Has("|v0", 1).Has("|v1", 0).HasLabel("model").OutE().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Has("/_id", "device-property").HasLabel("ref").Constant("~12")), GraphTraversal.__().Project("#v0", "#v1", "#v2", "#v3", "#v4", "#v5").By(GraphTraversal.__().AddV("instance").Property("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Property("/_id", "product-instance-1").Property("|v0", 1).Property("|v1", 0).SideEffect(GraphTraversal.__().Union(GraphTraversal.__().Property("/_schemaVersion", "1.0.0"), GraphTraversal.__().Property("/name", "First product instance"), GraphTraversal.__().Property("product-property", "product-property-value-1")))).By(GraphTraversal.__().AddV("instance").Property("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Property("/_id", "product-instance-2").Property("|v0", 1).Property("|v1", 0).SideEffect(GraphTraversal.__().Union(GraphTraversal.__().Property("/_schemaVersion", "1.0.0"), GraphTraversal.__().Property("/name", "Second product instance"), GraphTraversal.__().Property("product-property", "product-property-value-2")))).By(GraphTraversal.__().AddV("instance").Property("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Property("/_id", "device-instance-1").Property("|v0", 1).Property("|v1", 0).SideEffect(GraphTraversal.__().Union(GraphTraversal.__().Property("/_schemaVersion", "1.0.0"), GraphTraversal.__().Property("/name", "First device instance"), GraphTraversal.__().Property("device-property", "device-property-value-1")))).By(GraphTraversal.__().AddV("instance").Property("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Property("/_id", "device-instance-2").Property("|v0", 1).Property("|v1", 0).SideEffect(GraphTraversal.__().Union(GraphTraversal.__().Property("/_schemaVersion", "1.0.0"), GraphTraversal.__().Property("/name", "Second device instance"), GraphTraversal.__().Property("device-property", "device-property-value-2")))).By(GraphTraversal.__().AddV("model").Property("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Property("/_id", "product-model").Property("|v0", 1).Property("|v1", 0).SideEffect(GraphTraversal.__().Union(GraphTraversal.__().Property("/_schemaVersion", "1.0.0"), GraphTraversal.__().Property("/name", "Product Model"), GraphTraversal.__().Property("/category", "product"), GraphTraversal.__().Property("product-property", "property", "name", "Product Property", "dataType", "string", "attributes", ",tag,")))).By(GraphTraversal.__().AddV("model").Property("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Property("/_id", "device-model").Property("|v0", 1).Property("|v1", 0).SideEffect(GraphTraversal.__().Union(GraphTraversal.__().Property("/_schemaVersion", "1.0.0"), GraphTraversal.__().Property("/name", "Device Model"), GraphTraversal.__().Property("/category", "device"), GraphTraversal.__().Property("/deviceMethods", "{}", "json", 1), GraphTraversal.__().Property("/deviceTelemetry", "{\'waterUsed\':{\'properties\':{\'volume\':{\'dataType\':\'int64\',\'minValue\':0,\'maxValue\':1000000000000,\'units\':\'milliliter\'}}}}", "json", 1), GraphTraversal.__().Property("device-property", "property", "name", "Device Property", "dataType", "string", "attributes", ",tag,")))).As("#v").SideEffect(GraphTraversal.__().Union(GraphTraversal.__().Select("#v0").AddE("mdl").To(GraphTraversal.__().Select("#v").Select("#v4")), GraphTraversal.__().Select("#v1").AddE("mdl").To(GraphTraversal.__().Select("#v").Select("#v4")), GraphTraversal.__().Select("#v2").AddE("mdl").To(GraphTraversal.__().Select("#v").Select("#v5")), GraphTraversal.__().Select("#v3").AddE("mdl").To(GraphTraversal.__().Select("#v").Select("#v5")))).Map(GraphTraversal.__().Union(GraphTraversal.__().Select("#v0").AddE("instance").To(GraphTraversal.__().Select("#v").Select("#v2")).Property("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Property("/_id", "product-to-device-1").Property("|v0", 1).Property("|v1", 0).Property("/_schemaVersion", "1.0.0"), GraphTraversal.__().Select("#v4").AddE("model").To(GraphTraversal.__().Select("#v").Select("#v5")).Property("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Property("/_id", "product-to-device-model").Property("|v0", 1).Property("|v1", 0).Property("/_schemaVersion", "1.0.0").Property("|#src", 1).Property("|#tgt", 0), GraphTraversal.__().Select("#v4").AddE("ref").To(GraphTraversal.__().Select("#v").Select("#v5")).Property("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Property("/_id", "device-property").Property("|ref:name", "Reference Property").Property("|ref:propertyId", "device-property").Property("|mdl:/_id", "product-to-device-model").Property("|mdl:|v0", 1).Property("|mdl:|v1", 0)).Property("/etag", "vFSAV9c/Qn6b4IegSI4NMQ==").Fold()).As("#e").Union(GraphTraversal.__().Select("#v").Union(GraphTraversal.__().Select("#v0").As("#a").Constant("product-property"), GraphTraversal.__().Select("#v1").As("#a").Constant("product-property"), GraphTraversal.__().Select("#v2").As("#a").Constant("device-property"), GraphTraversal.__().Select("#v3").As("#a").Constant("device-property"), GraphTraversal.__().Select("#v4").As("#a").Constant("product-property"), GraphTraversal.__().Select("#v5").As("#a").Constant("device-property")).As("#p"), GraphTraversal.__().Union(GraphTraversal.__().Select("#e")).Unfold().As("#f").Union(GraphTraversal.__().InV().As("#a").Select("#f").OutV(), GraphTraversal.__().OutV().As("#a").Select("#f").InV()).Map(GraphTraversal.__().Optional(GraphTraversal.__().Out("mdl")).As("#m").Select("#a").Optional(GraphTraversal.__().Out("mdl")).InE("ref").And(GraphTraversal.__().OutV().Where(Predicate.eq("#m"))).Values("/_id").Fold()).As("#p")).Select("#a").Union(GraphTraversal.__().Identity(), GraphTraversal.__().As("@v").FlatMap(GraphTraversal.__().Optional(GraphTraversal.__().Out("mdl")).InE("ref").And(GraphTraversal.__().Values("/_id").Where(Predicate.within("#p")))).Repeat(GraphTraversal.__().As("@e").FlatMap(GraphTraversal.__().OutV().As("mdl").Select(GremlinKeyword.Pop.last, "@v").Both().Dedup().And(GraphTraversal.__().Optional(GraphTraversal.__().Out("mdl")).Where(Predicate.eq("mdl")))).As("@v").Optional(GraphTraversal.__().FlatMap(GraphTraversal.__().Select(GremlinKeyword.Pop.last, "@e").Values("/_id").As("key").Select(GremlinKeyword.Pop.last, "@v").Optional(GraphTraversal.__().Out("mdl")).InE("ref").And(GraphTraversal.__().Values("|ref:propertyId").Where(Predicate.eq("key")))))).Until(GraphTraversal.__().FlatMap(GraphTraversal.__().As("res").Select(GremlinKeyword.Pop.last, "@v").Where(Predicate.eq("res")))).Select("@v").Unfold()).Dedup().Property("/etag", "vFSAV9c/Qn6b4IegSI4NMQ==").Project("label", "/_id", "|v0", "|v1").By(GraphTraversal.__().Label()).By(GraphTraversal.__().Values("/_id")).By(GraphTraversal.__().Values("|v0")).By(GraphTraversal.__().Values("|v1"))).Next();
            return graph.g().Inject(0).Coalesce(GraphTraversal.__().Union(GraphTraversal.__().Not(GraphTraversal.__().V().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").HasLabel("application")).Constant("~0"), GraphTraversal.__().V().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").HasLabel("application").Has("|provisioning", 0).Constant("~1"), GraphTraversal.__().V().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").HasLabel("application").Has("|provisioning", 2).Constant("~2"), GraphTraversal.__().V().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").HasLabel("application").Has("|deleted", true).Constant("~3"), GraphTraversal.__().V().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Has("/_id", "product-instance-1").Has("|v0", 1).Has("|v1", 0).HasLabel("instance").Constant("~4"), GraphTraversal.__().V().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Has("/_id", "product-instance-2").Has("|v0", 1).Has("|v1", 0).HasLabel("instance").Constant("~5"), GraphTraversal.__().V().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Has("/_id", "device-instance-1").Has("|v0", 1).Has("|v1", 0).HasLabel("instance").Constant("~6"), GraphTraversal.__().V().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Has("/_id", "device-instance-2").Has("|v0", 1).Has("|v1", 0).HasLabel("instance").Constant("~7"), GraphTraversal.__().V().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Has("/_id", "product-model").Has("|v0", 1).Has("|v1", 0).HasLabel("model").Constant("~8"), GraphTraversal.__().V().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Has("/_id", "device-model").Has("|v0", 1).Has("|v1", 0).HasLabel("model").Constant("~9"), GraphTraversal.__().V().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Has("/_id", "product-instance-1").Has("|v0", 1).Has("|v1", 0).HasLabel("instance").OutE().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Has("/_id", "product-to-device-1").Has("|v0", 1).Has("|v1", 0).HasLabel("instance").Constant("~10"), GraphTraversal.__().V().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Has("/_id", "product-model").Has("|v0", 1).Has("|v1", 0).HasLabel("model").OutE().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Has("/_id", "product-to-device-model").Has("|v0", 1).Has("|v1", 0).HasLabel("model").Constant("~11"), GraphTraversal.__().V().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Has("/_id", "product-model").Has("|v0", 1).Has("|v1", 0).HasLabel("model").OutE().Has("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Has("/_id", "device-property").HasLabel("ref").Constant("~12")), GraphTraversal.__().Project("#v0", "#v1", "#v2", "#v3", "#v4", "#v5").By(GraphTraversal.__().AddV("instance").Property("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Property("/_id", "product-instance-1").Property("|v0", 1).Property("|v1", 0).SideEffect(GraphTraversal.__().Union(GraphTraversal.__().Property("/_schemaVersion", "1.0.0"), GraphTraversal.__().Property("/name", "First product instance"), GraphTraversal.__().Property("product-property", "product-property-value-1")))).By(GraphTraversal.__().AddV("instance").Property("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Property("/_id", "product-instance-2").Property("|v0", 1).Property("|v1", 0).SideEffect(GraphTraversal.__().Union(GraphTraversal.__().Property("/_schemaVersion", "1.0.0"), GraphTraversal.__().Property("/name", "Second product instance"), GraphTraversal.__().Property("product-property", "product-property-value-2")))).By(GraphTraversal.__().AddV("instance").Property("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Property("/_id", "device-instance-1").Property("|v0", 1).Property("|v1", 0).SideEffect(GraphTraversal.__().Union(GraphTraversal.__().Property("/_schemaVersion", "1.0.0"), GraphTraversal.__().Property("/name", "First device instance"), GraphTraversal.__().Property("device-property", "device-property-value-1")))).By(GraphTraversal.__().AddV("instance").Property("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Property("/_id", "device-instance-2").Property("|v0", 1).Property("|v1", 0).SideEffect(GraphTraversal.__().Union(GraphTraversal.__().Property("/_schemaVersion", "1.0.0"), GraphTraversal.__().Property("/name", "Second device instance"), GraphTraversal.__().Property("device-property", "device-property-value-2")))).By(GraphTraversal.__().AddV("model").Property("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Property("/_id", "product-model").Property("|v0", 1).Property("|v1", 0).SideEffect(GraphTraversal.__().Union(GraphTraversal.__().Property("/_schemaVersion", "1.0.0"), GraphTraversal.__().Property("/name", "Product Model"), GraphTraversal.__().Property("/category", "product"), GraphTraversal.__().Property("product-property", "property", "name", "Product Property", "dataType", "string", "attributes", ",tag,")))).By(GraphTraversal.__().AddV("model").Property("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Property("/_id", "device-model").Property("|v0", 1).Property("|v1", 0).SideEffect(GraphTraversal.__().Union(GraphTraversal.__().Property("/_schemaVersion", "1.0.0"), GraphTraversal.__().Property("/name", "Device Model"), GraphTraversal.__().Property("/category", "device"), GraphTraversal.__().Property("/deviceMethods", "{}", "json", 1), GraphTraversal.__().Property("/deviceTelemetry", "{\"waterUsed\":{\"properties\":{\"volume\":{\"dataType\":\"int64\",\"minValue\":0,\"maxValue\":1000000000000,\"units\":\"milliliter\"}}}}", "json", 1), GraphTraversal.__().Property("device-property", "property", "name", "Device Property", "dataType", "string", "attributes", ",tag,")))).As("#v").SideEffect(GraphTraversal.__().Union(GraphTraversal.__().Select("#v0").AddE("mdl").To(GraphTraversal.__().Select("#v").Select("#v4")), GraphTraversal.__().Select("#v1").AddE("mdl").To(GraphTraversal.__().Select("#v").Select("#v4")), GraphTraversal.__().Select("#v2").AddE("mdl").To(GraphTraversal.__().Select("#v").Select("#v5")), GraphTraversal.__().Select("#v3").AddE("mdl").To(GraphTraversal.__().Select("#v").Select("#v5")))).Map(GraphTraversal.__().Union(GraphTraversal.__().Select("#v0").AddE("instance").To(GraphTraversal.__().Select("#v").Select("#v2")).Property("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Property("/_id", "product-to-device-1").Property("|v0", 1).Property("|v1", 0).Property("/_schemaVersion", "1.0.0"), GraphTraversal.__().Select("#v4").AddE("model").To(GraphTraversal.__().Select("#v").Select("#v5")).Property("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Property("/_id", "product-to-device-model").Property("|v0", 1).Property("|v1", 0).Property("/_schemaVersion", "1.0.0").Property("|#src", 1).Property("|#tgt", 0), GraphTraversal.__().Select("#v4").AddE("ref").To(GraphTraversal.__().Select("#v").Select("#v5")).Property("|app", "ed011feb-0db1-40de-b633-9ec16b758259").Property("/_id", "device-property").Property("|ref:name", "Reference Property").Property("|ref:propertyId", "device-property").Property("|mdl:/_id", "product-to-device-model").Property("|mdl:|v0", 1).Property("|mdl:|v1", 0)).Property("/etag", "vFSAV9c/Qn6b4IegSI4NMQ==").Fold()).As("#e").Union(GraphTraversal.__().Select("#v").Union(GraphTraversal.__().Select("#v0").As("#a").Constant(new List<Object> { "product-property" }), GraphTraversal.__().Select("#v1").As("#a").Constant(new List<Object> { "product-property" }), GraphTraversal.__().Select("#v2").As("#a").Constant(new List<Object> { "device-property" }), GraphTraversal.__().Select("#v3").As("#a").Constant(new List<Object> { "device-property" }), GraphTraversal.__().Select("#v4").As("#a").Constant(new List<Object> { "product-property" }), GraphTraversal.__().Select("#v5").As("#a").Constant(new List<Object> { "device-property" })).As("#p"), GraphTraversal.__().Union(GraphTraversal.__().Select("#e")).Unfold().As("#f").Union(GraphTraversal.__().InV().As("#a").Select("#f").OutV(), GraphTraversal.__().OutV().As("#a").Select("#f").InV()).Map(GraphTraversal.__().Optional(GraphTraversal.__().Out("mdl")).As("#m").Select("#a").Optional(GraphTraversal.__().Out("mdl")).InE("ref").And(GraphTraversal.__().OutV().Where(Predicate.eq("#m"))).Values("/_id").Fold()).As("#p")).Select("#a").Union(GraphTraversal.__().Identity(), GraphTraversal.__().As("@v").FlatMap(GraphTraversal.__().Optional(GraphTraversal.__().Out("mdl")).InE("ref").And(GraphTraversal.__().Values("/_id").Where(Predicate.within("#p")))).Repeat(GraphTraversal.__().As("@e").FlatMap(GraphTraversal.__().OutV().As("mdl").Select(GremlinKeyword.Pop.Last, "@v").Both().Dedup().And(GraphTraversal.__().Optional(GraphTraversal.__().Out("mdl")).Where(Predicate.eq("mdl")))).As("@v").Optional(GraphTraversal.__().FlatMap(GraphTraversal.__().Select(GremlinKeyword.Pop.Last, "@e").Values("/_id").As("key").Select(GremlinKeyword.Pop.Last, "@v").Optional(GraphTraversal.__().Out("mdl")).InE("ref").And(GraphTraversal.__().Values("|ref:propertyId").Where(Predicate.eq("key")))))).Until(GraphTraversal.__().FlatMap(GraphTraversal.__().As("res").Select(GremlinKeyword.Pop.Last, "@v").Where(Predicate.eq("res")))).Select("@v").Unfold()).Dedup().Property("/etag", "vFSAV9c/Qn6b4IegSI4NMQ==").Project("label", "/_id", "|v0", "|v1").By(GraphTraversal.__().Label()).By(GraphTraversal.__().Values("/_id")).By(GraphTraversal.__().Values("|v0")).By(GraphTraversal.__().Values("|v1"))).Next();
        }

        public List<string> Get_query_processed_4()
        {
            graph.CommandText =
                "g.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').coalesce(__.union(__.not(__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application')).constant('~0'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',0).constant('~1'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',2).constant('~2'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|deleted',true).constant('~3')),__.flatMap(__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('model').has('/category','product').range(0,100).sideEffect(__.id().store('^ids')).union(__.identity(),__.as('@v').flatMap(__.optional(__.out('mdl')).outE('ref')).repeat(__.as('@e').flatMap(__.inV().as('mdl').select(last,'@v').both().dedup().and(__.optional(__.out('mdl')).where(eq('mdl')))).as('@v').optional(__.flatMap(__.select(last,'@e').values('|ref:propertyId').as('key').select(last,'@v').optional(__.out('mdl')).outE('ref').and(__.values('/_id').where(eq('key')))))).until(__.flatMap(__.as('res').select(last,'@v').where(eq('res')))).sideEffect(__.project('segments','targets').by(__.select('@e').unfold().fold()).by(__.select('@v').unfold().project('label','/_id','|v0','|v1').by(__.label()).by(__.values('/_id')).by(__.values('|v0')).by(__.values('|v1')).fold()).store('^refs'))).dedup().union(__.identity().sideEffect(__.group('^mdls').by(__.id()).by(__.coalesce(__.out('mdl').id(),__.constant('')))),__.out('mdl')).dedup()).union(__.emit().repeat(__.outE('_val').as('_').inV()).tree(),__.cap('^ids'),__.cap('^mdls'),__.cap('^refs')).fold())";
            return graph.ExecuteAndGetResults();
        }

        public List<string> Get_query_processed_5()
        {
            graph.CommandText =
                "g.inject(0).coalesce(__.union(__.not(__.V().has('|app','unknownApp').hasLabel('application')).constant('~0'),__.V().has('|app','unknownApp').hasLabel('application').has('|provisioning',0).constant('~1'),__.V().has('|app','unknownApp').hasLabel('application').has('|provisioning',2).constant('~2'),__.V().has('|app','unknownApp').hasLabel('application').has('|deleted',true).constant('~3'),__.V().has('|app','unknownApp').has('/_id','parent-model').has('|v0',1).has('|v1',0).hasLabel('model').constant('~4')),__.project('#v0').by(__.addV('model').property('|app','unknownApp').property('/_id','parent-model').property('|v0',1).property('|v1',0).sideEffect(__.union(__.property('/_schemaVersion','1.0.0'),__.property('/name','Parent Product Model'),__.property('/category','product')))).as('#v').union(__.select('#v').union(__.select('#v0').as('#a').constant('')).as('#p')).select('#a').union(__.identity(),__.as('@v').flatMap(__.optional(__.out('mdl')).inE('ref').and(__.values('/_id').where(within('#p')))).repeat(__.as('@e').flatMap(__.outV().as('mdl').select(last,'@v').both().dedup().and(__.optional(__.out('mdl')).where(eq('mdl')))).as('@v').optional(__.flatMap(__.select(last,'@e').values('/_id').as('key').select(last,'@v').optional(__.out('mdl')).inE('ref').and(__.values('|ref:propertyId').where(eq('key')))))).until(__.flatMap(__.as('res').select(last,'@v').where(eq('res')))).select('@v').unfold()).dedup().property('/etag','3xlLYYHxQ3unW3lXwiFZVA==').project('label','/_id','|v0','|v1').by(__.label()).by(__.values('/_id')).by(__.values('|v0')).by(__.values('|v1')))";
            return graph.ExecuteAndGetResults();
        }

        public List<string> Get_query_processed_6()
        {
            graph.CommandText =
                "g.inject(0).coalesce(__.union(__.not(__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application')).constant('~0'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',0).constant('~1'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',2).constant('~2'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|deleted',true).constant('~3'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').has('/_id','parent-model').has('|v0',1).has('|v1',0).hasLabel('model').constant('~4')),__.project('#v0').by(__.addV('model').property('|app','ed011feb-0db1-40de-b633-9ec16b758259').property('/_id','parent-model').property('|v0',1).property('|v1',0).sideEffect(__.union(__.property('/_schemaVersion','1.0.0'),__.property('/name','Parent Product Model'),__.property('/category','product')))).as('#v').union(__.select('#v').union(__.select('#v0').as('#a').constant('')).as('#p')).select('#a').union(__.identity(),__.as('@v').flatMap(__.optional(__.out('mdl')).inE('ref').and(__.values('/_id').where(within('#p')))).repeat(__.as('@e').flatMap(__.outV().as('mdl').select(last,'@v').both().dedup().and(__.optional(__.out('mdl')).where(eq('mdl')))).as('@v').optional(__.flatMap(__.select(last,'@e').values('/_id').as('key').select(last,'@v').optional(__.out('mdl')).inE('ref').and(__.values('|ref:propertyId').where(eq('key')))))).until(__.flatMap(__.as('res').select(last,'@v').where(eq('res')))).select('@v').unfold()).dedup().property('/etag','+KcBCvZsTUWxEf6N1YJY0w==').project('label','/_id','|v0','|v1').by(__.label()).by(__.values('/_id')).by(__.values('|v0')).by(__.values('|v1')))";
            return graph.ExecuteAndGetResults();
        }

        public List<string> Get_query_processed_7()
        {
            graph.CommandText =
                "g.inject(0).coalesce(__.union(__.not(__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application')).constant('~0'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',0).constant('~1'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',2).constant('~2'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|deleted',true).constant('~3'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').has('/_id','parent-model').has('|v0',1).has('|v1',0).hasLabel('model').constant('~4')),__.project('#v0').by(__.addV('model').property('|app','ed011feb-0db1-40de-b633-9ec16b758259').property('/_id','parent-model').property('|v0',1).property('|v1',0).sideEffect(__.union(__.property('/_schemaVersion','1.0.0'),__.property('/name','Parent Product Model'),__.property('/category','product')))).as('#v').union(__.select('#v').union(__.select('#v0').as('#a').constant('')).as('#p')).select('#a').union(__.identity(),__.as('@v').flatMap(__.optional(__.out('mdl')).inE('ref').and(__.values('/_id').where(within('#p')))).repeat(__.as('@e').flatMap(__.outV().as('mdl').select(last,'@v').both().dedup().and(__.optional(__.out('mdl')).where(eq('mdl')))).as('@v').optional(__.flatMap(__.select(last,'@e').values('/_id').as('key').select(last,'@v').optional(__.out('mdl')).inE('ref').and(__.values('|ref:propertyId').where(eq('key')))))).until(__.flatMap(__.as('res').select(last,'@v').where(eq('res')))).select('@v').unfold()).dedup().property('/etag','91RyclGUTM2rTif31TQoQA==').project('label','/_id','|v0','|v1').by(__.label()).by(__.values('/_id')).by(__.values('|v0')).by(__.values('|v1')))";
            return graph.ExecuteAndGetResults();
        }

        public List<string> Get_query_processed_8()
        {
            graph.CommandText =
                "g.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').coalesce(__.union(__.not(__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application')).constant('~0'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',0).constant('~1'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',2).constant('~2'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|deleted',true).constant('~3')),__.flatMap(__.project('vertices','edges').by(__.union(__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').has('/_id','parent-model').has('|v0',1).has('|v1',0).hasLabel('model'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').has('/_id','product-model').has('|v0',1).has('|v1',0).hasLabel('model')).fold()).by(__.constant('')).sideEffect(__.select('edges').unfold().union(__.identity(),__.hasLabel('model').as('eM').inV().as('end').select('eM').outV().BothE('ref').and(__.otherV().where(eq('end')))).project('edge','source','target').by().by(__.outV().id()).by(__.inV().id()).store('^edges')).union(__.select('vertices').unfold().sideEffect(__.id().store('^ids')),__.select('edges').unfold().union(__.inV(),__.outV())).dedup().union(__.identity(),__.as('@v').flatMap(__.optional(__.out('mdl')).outE('ref')).repeat(__.as('@e').flatMap(__.inV().as('mdl').select(last,'@v').both().dedup().and(__.optional(__.out('mdl')).where(eq('mdl')))).as('@v').optional(__.flatMap(__.select(last,'@e').values('|ref:propertyId').as('key').select(last,'@v').optional(__.out('mdl')).outE('ref').and(__.values('/_id').where(eq('key')))))).until(__.flatMap(__.as('res').select(last,'@v').where(eq('res')))).sideEffect(__.project('segments','targets').by(__.select('@e').unfold().fold()).by(__.select('@v').unfold().project('label','/_id','|v0','|v1').by(__.label()).by(__.values('/_id')).by(__.values('|v0')).by(__.values('|v1')).fold()).store('^refs'))).dedup().union(__.identity().sideEffect(__.group('^mdls').by(__.id()).by(__.coalesce(__.out('mdl').id(),__.constant('')))),__.out('mdl')).dedup()).union(__.emit().repeat(__.outE('_val').as('_').inV()).tree(),__.cap('^ids'),__.cap('^mdls'),__.cap('^refs')).fold().union(__.identity(),__.cap('^edges')))";


            return graph.ExecuteAndGetResults();
        }
    }
}