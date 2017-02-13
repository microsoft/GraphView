using System;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.CompilerServices;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewUnitTest
{
    [TestClass]
    public class TestLarge
    {
        private const string DOCDB_URL = "https://graphview.documents.azure.com:443/";
        private const string DOCDB_AUTHKEY = "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==";
        private const string DOCDB_DATABASE = "Temperary";


        [MethodImpl(MethodImplOptions.NoInlining)]
        private static GraphViewConnection CreateConnection(string tips = null)
        {
            StackFrame frame = new StackFrame(1);
            if (!string.IsNullOrEmpty(tips)) {
                tips = $" {tips}";
            }
            string collectionName = $"[{frame.GetMethod().Name}]{tips}";

            GraphViewConnection connection = new GraphViewConnection(DOCDB_URL, DOCDB_AUTHKEY, DOCDB_DATABASE, collectionName);
            connection.ResetCollection();
            return connection;
        }

        [TestMethod]
        public void TestModernGraph_Dummy()
        {
            GraphViewConnection connection = CreateConnection();
            GraphViewCommand graph = new GraphViewCommand(connection);

            graph.g().AddV("person").Property("age", "27").Property("name", "vadas").Next();
            graph.g().AddV("person").Property("age", "29").Property("name", "marko").Next();
            graph.g().AddV("person").Property("age", "35").Property("name", "peter").Next();
            graph.g().AddV("person").Property("age", "32").Property("name", "josh").Next();
            graph.g().AddV("software").Property("lang", "java").Property("name", "lop").Next();
            graph.g().AddV("software").Property("lang", "java").Property("name", "ripple").Next();

            graph.g().V().Has("name", "marko").AddE("knows").Property("weight", 0.5).To(graph.g().V().Has("name", "vadas")).Next();
            graph.g().V().Has("name", "marko").AddE("knows").Property("weight", 1.0).To(graph.g().V().Has("name", "josh")).Next();
            graph.g().V().Has("name", "marko").AddE("knows").Property("weight", 0.4).To(graph.g().V().Has("name", "lop")).Next();
            graph.g().V().Has("name", "peter").AddE("created").Property("weight", 0.2).To(graph.g().V().Has("name", "lop")).Next();
            graph.g().V().Has("name", "josh").AddE("created").Property("weight", 0.4).To(graph.g().V().Has("name", "lop")).Next();
            graph.g().V().Has("name", "josh").AddE("created").Property("my", "josh2ripple").Property("weight", 1.0).To(graph.g().V().Has("name", "ripple")).Next();
        }

        [TestMethod]
        public void TestDocDBLimit()
        {
            GraphViewConnection connection = CreateConnection();
            GraphViewCommand graph = new GraphViewCommand(connection);

            string largeProperty = new string('_', 1024 * 1024 * 2 - 1024);  // Size = 2MB
            graph.g().AddV("dummy").Property("large", largeProperty).Next();
        }


        [TestMethod]
        public void TestAddHeavyEdges_LargeQuantity()
        {
            const int EDGE_COUNT = 100;

            GraphViewConnection connection = CreateConnection($"E={EDGE_COUNT}");
            GraphViewCommand graph = new GraphViewCommand(connection);

            graph.g().AddV("SourceV").Next();
            graph.g().AddV("SinkV").Next();
            for (int i = 0; i < EDGE_COUNT; i++) {
                Debug.WriteLine($"AddEdge: i = {i}");
                string longLabel = new string('-', 1024 * 10 * i);  // Size = 10-1000KB
                graph.g().V().HasLabel("SourceV").AddE(edgeLabel: longLabel).To(graph.g().V().HasLabel("SinkV")).Next();
            }
        }

        [TestMethod]
        public void TestAddHeavyEdges_MidiumQuantity()
        {
            const int EDGE_COUNT = 10;
            GraphViewConnection connection = CreateConnection($"E={EDGE_COUNT}");
            GraphViewCommand graph = new GraphViewCommand(connection);

            graph.g().AddV("SourceV").Next();
            graph.g().AddV("SinkV").Next();
            for (int i = 0; i < EDGE_COUNT; i++) {
                string longLabel = new string((char)('0' + i), 1024 * 900);  // Size = 900KB
                graph.g().V().HasLabel("SourceV").AddE(edgeLabel: longLabel).To(graph.g().V().HasLabel("SinkV")).Next();
            }
        }

        [TestMethod]
        public void TestAddHeavyEdges_SmallQuantity()
        {
            const int EDGE_COUNT = 3;
            GraphViewConnection connection = CreateConnection($"E={EDGE_COUNT}");
            GraphViewCommand graph = new GraphViewCommand(connection);

            graph.g().AddV("SourceV").Next();
            graph.g().AddV("SinkV").Next();
            for (int i = 0; i < EDGE_COUNT; i++) {
                string longLabel = new string((char)('0' + i), 1024 * 900);  // Size = 900KB
                graph.g().V().HasLabel("SourceV").AddE(edgeLabel: longLabel).To(graph.g().V().HasLabel("SinkV")).Next();
            }
        }


        [TestMethod]
        public void TestAddAndDropEdges_Small()
        {
            GraphViewConnection connection = CreateConnection("AddAndDropEdges E=3");
            GraphViewCommand graph = new GraphViewCommand(connection);

            graph.g().AddV("SourceV").Next();
            graph.g().AddV("SinkV").Next();
            for (int i = 0; i <= 10; i++) {
                graph.g().V().HasLabel("SourceV").AddE(edgeLabel: "Dummy").To(graph.g().V().HasLabel("SinkV")).Next();
            }
            graph.g().V().E().HasLabel("Dummy").Drop().Next();
        }


        [TestMethod]
        public void TestAddAndDropEdges_DropAll_Small()
        {
            const int EDGE_COUNT = 10;
            GraphViewConnection connection = CreateConnection($"AddAndDropEdges_DropAll_Small E={EDGE_COUNT}");
            GraphViewCommand graph = new GraphViewCommand(connection);

            graph.g().AddV("SourceV").Next();
            graph.g().AddV("SinkV").Next();
            for (int i = 0; i < EDGE_COUNT; i++) {
                graph.g().V().HasLabel("SourceV").AddE().To(graph.g().V().HasLabel("SinkV")).Next();
            }
            graph.g().V().OutE().Drop().Next();
        }

        [TestMethod]
        public void TestAddAndDropEdges_DropSome_Small()
        {
            const int EDGE_COUNT = 20;
            GraphViewConnection connection = CreateConnection($"AddAndDropEdges_DropSome_Small E={EDGE_COUNT}");
            GraphViewCommand graph = new GraphViewCommand(connection);

            graph.g().AddV("SourceV").Next();
            graph.g().AddV("SinkV").Next();
            for (int i = 0; i < EDGE_COUNT; i++) {
                graph.g().V().HasLabel("SourceV").AddE(edgeLabel: $"MOD_{i % 4}").To(graph.g().V().HasLabel("SinkV")).Next();
            }
            graph.g().V().OutE().HasLabel("MOD_0", "MOD_2").Drop().Next();
        }

        [TestMethod]
        public void TestAddDropEdges_DropAll_Large()
        {
            const int EDGE_COUNT = 10;
            GraphViewConnection connection = CreateConnection($"AddAndDropEdges_DropAll_Large E={EDGE_COUNT}");
            GraphViewCommand graph = new GraphViewCommand(connection);

            graph.g().AddV("SourceV").Next();
            graph.g().AddV("SinkV").Next();
            for (int i = 0; i < EDGE_COUNT; i++) {
                string label = $"{i}@{new string('_', 1024 * 900)}";
                graph.g().V().HasLabel("SourceV").AddE(edgeLabel: label).To(graph.g().V().HasLabel("SinkV")).Next();
            }
            graph.g().V().OutE().Drop().Next();
        }

        [TestMethod]
        public void TestAddDropEdges_DropSome_Large()
        {
            const int EDGE_COUNT = 100;
            GraphViewConnection connection = CreateConnection($"AddAndDropEdges_DropSome_Large E={EDGE_COUNT}");
            GraphViewCommand graph = new GraphViewCommand(connection);
            string suffix = new string('_', 1024 * 900);

            graph.g().AddV("SourceV").Next();
            graph.g().AddV("SinkV").Next();
            for (int i = 0; i < EDGE_COUNT; i++) {
                graph.g().V().HasLabel("SourceV").AddE(edgeLabel: $"MOD_{i % 4}{suffix}").To(graph.g().V().HasLabel("SinkV")).Next();
            }
            graph.g().V().OutE().HasLabel($"MOD_0{suffix}", $"MOD_2{suffix}").Drop().Next();
        }

        [TestMethod]
        public void TestDropNodes_Small()
        {
            const int EDGE_COUNT = 5;
            GraphViewConnection connection = CreateConnection($"TestDropNodes_Small(A->B->C, A->C) E={EDGE_COUNT}");
            GraphViewCommand graph = new GraphViewCommand(connection);
            string suffix = string.Empty;

            graph.g().AddV("A").Next();
            graph.g().AddV("B").Next();
            graph.g().AddV("C").Next();
            for (int i = 0; i < EDGE_COUNT; i++) {
                graph.g().V().HasLabel("A").AddE(edgeLabel: $"AB_{i}{suffix}").To(graph.g().V().HasLabel("B")).Next();
                graph.g().V().HasLabel("A").AddE(edgeLabel: $"AC_{i}{suffix}").To(graph.g().V().HasLabel("C")).Next();
                graph.g().V().HasLabel("B").AddE(edgeLabel: $"BC_{i}{suffix}").To(graph.g().V().HasLabel("C")).Next();
            }
            graph.g().V().HasLabel("B").Drop().Next();
        }


        [TestMethod]
        public void TestDropNodes_Large()
        {
            const int EDGE_COUNT = 10;
            GraphViewConnection connection = CreateConnection($"{MethodBase.GetCurrentMethod().Name}(A->B->C, A->C) E={EDGE_COUNT}");
            GraphViewCommand graph = new GraphViewCommand(connection);
            string suffix = new string('_', 1024 * 900);

            graph.g().AddV("A").Next();
            graph.g().AddV("B").Next();
            graph.g().AddV("C").Next();
            for (int i = 0; i < EDGE_COUNT; i++) {
                graph.g().V().HasLabel("A").AddE(edgeLabel: $"AB_{i}{suffix}").To(graph.g().V().HasLabel("B")).Next();
                graph.g().V().HasLabel("A").AddE(edgeLabel: $"AC_{i}{suffix}").To(graph.g().V().HasLabel("C")).Next();
                graph.g().V().HasLabel("B").AddE(edgeLabel: $"BC_{i}{suffix}").To(graph.g().V().HasLabel("C")).Next();
            }
            graph.g().V().HasLabel("B").Drop().Next();
        }

        [TestMethod]
        public void TestChangeEdgeProperties_Small()
        {
            const int EDGE_COUNT = 4;
            GraphViewConnection connection = CreateConnection($"{MethodBase.GetCurrentMethod().Name} E={EDGE_COUNT}");
            GraphViewCommand graph = new GraphViewCommand(connection);
            string suffix = string.Empty;

            graph.g().AddV("SourceV").Next();
            graph.g().AddV("SinkV").Next();
            for (int i = 0; i < EDGE_COUNT; i++) {
                graph.g().V().HasLabel("SourceV").AddE($"E{i}").Property($"E{i} Property", $"E{i} PropValue").To(graph.g().V().HasLabel("SinkV")).Next();
            }

            for (int i = 0; i < EDGE_COUNT; i++) {
                if (i % 2 == 1) {
                    graph.g().V().OutE().HasLabel($"E{i}").Property($"E{i} Property", null).Next();
                    graph.g().V().OutE().HasLabel($"E{i}").Property($"E{i} Another Property", "Dummy!").Next();
                }
            }
        }

        [TestMethod]
        public void TestChangeEdgeProperties_Large()
        {
            const int EDGE_COUNT = 10;
            GraphViewConnection connection = CreateConnection($"{MethodBase.GetCurrentMethod().Name} E={EDGE_COUNT}");
            GraphViewCommand graph = new GraphViewCommand(connection);
            string suffix = new string('_', 1024 * 900);

            graph.g().AddV("SourceV").Next();
            graph.g().AddV("SinkV").Next();
            for (int i = 0; i < EDGE_COUNT; i++) {
                graph.g().V().HasLabel("SourceV").AddE($"E{i}{suffix}").Property($"E{i} Property", $"E{i} PropValue").To(graph.g().V().HasLabel("SinkV")).Next();
            }

            for (int i = 0; i < EDGE_COUNT; i++) {
                if (i % 2 == 1) {
                    graph.g().V().OutE().HasLabel($"E{i}{suffix}").Property($"E{i} Property", null).Next();
                    graph.g().V().OutE().HasLabel($"E{i}{suffix}").Property($"E{i} Another Property", "Dummy!").Next();
                }
            }
        }


        [TestMethod]
        public void TestChangeEdgeProperties_Large2Larger()
        {
            const int EDGE_COUNT = 4;
            GraphViewConnection connection = CreateConnection($"{MethodBase.GetCurrentMethod().Name} E={EDGE_COUNT}");
            GraphViewCommand graph = new GraphViewCommand(connection);
            string suffix = new string('_', 1024 * 900);  // 900 KB
            string suffix2 = new string('_', 1024 * 1900);  // nearly 2MB

            graph.g().AddV("SourceV").Next();
            graph.g().AddV("SinkV").Next();
            for (int i = 0; i < EDGE_COUNT; i++) {
                graph.g().V().HasLabel("SourceV").AddE($"E{i}{suffix}").Property($"E{i} Property", $"E{i} PropValue").To(graph.g().V().HasLabel("SinkV")).Next();
            }

            // Drop all added properties
            for (int i = 0; i < EDGE_COUNT; i++) {
                graph.g().V().OutE().HasLabel($"E{i}{suffix}").Property($"E{i} Property", null).Next();
            }

            // Add new property
            // Each document can only hold ONE edge-object!
            for (int i = 0; i < EDGE_COUNT; i++) {
                graph.g().V().OutE().HasLabel($"E{i}{suffix}").Property($"E{i} Another Property", "Dummy!").Next();
            }
        }

    }
}
