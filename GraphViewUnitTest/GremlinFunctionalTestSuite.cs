using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewUnitTest
{
    /// <summary>
    /// The original "id", "_id" and "_etag" have been replaced by "_id", "__id" and "__etag" 
    /// </summary>
    [TestClass]
    public class GremlinFunctionalTestSuite
    {
        static GraphViewConnection GetGraphViewConnection()
        {
            return new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "GremlinFunctionalTestSuite");

            //return new GraphViewConnection("https://localhost:8081/",
            //    "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
            //    "GroupMatch", "GremlinFunctionalTestSuite");
        }

        static GraphViewCommand GetGraphViewCommand(GraphViewConnection connection)
        {
            GraphViewCommand command = new GraphViewCommand(connection);
            //command.UseReverseEdges = false;

            return command;
        }

        [TestMethod]
        public void Test1CreateApplication()
        {
            GraphViewConnection connection = GetGraphViewConnection();
            connection.ResetCollection();

            GraphViewCommand graph = GetGraphViewCommand(connection);

            //graph.g().V().Drop().Next();

            var results =
                graph.g()
                    .AddV("application")
                    .Property("_app", "test-app")
                    .Property("__id", "test-app")
                    .Property("_provisioningState", 1)
                    .Property("_deleted", false)
                    .Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }

            //var nodeCount = graph.g().V().Count();
            //foreach (var result in nodeCount)
            //{
            //    Console.WriteLine(result);
            //}

            //var edgeCount = graph.g().V().OutE().Count();
            //foreach (var result in edgeCount)
            //{
            //    Console.WriteLine(result);
            //}
        }

        [TestMethod]
        public void Test2ImportModels()
        {
            GraphViewConnection connection = GetGraphViewConnection();
            //connection.ResetCollection();

            GraphViewCommand graph = GetGraphViewCommand(connection);

            var results = graph.g().Inject(0).Coalesce(
                GraphTraversal2.__().Union(
                    GraphTraversal2.__()
                        .Not(GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "test-app")
                            .HasLabel("application"))
                        .Constant("~0"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "test-app")
                        .HasLabel("application")
                        .Has("_provisioningState", 0)
                        .Constant("~1"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "test-app")
                        .HasLabel("application")
                        .Has("_provisioningState", 2)
                        .Constant("~2"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "test-app")
                        .HasLabel("application")
                        .Has("_deleted", true)
                        .Constant("~3"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "product:soda-machine")
                        .HasLabel("product-model")
                        .Constant("~4"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "uber-product:soda-machine")
                        .HasLabel("product-model")
                        .Constant("~5"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "device:ice-machine")
                        .HasLabel("device-model")
                        .Constant("~6"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "device:soda-mixer")
                        .HasLabel("device-model")
                        .Constant("~7")),
                GraphTraversal2.__()
                    .Project("#v0", "#v1", "#v2", "#v3")
                    .By(GraphTraversal2.__()
                        .AddV("product-model")
                        .Property("_app", "test-app")
                        .Property("__id", "product:soda-machine")
                        .Property("__etag", "B0vDw1xnS/agXzX9F7wxHg==")
                        .SideEffect(GraphTraversal2.__().Union(
                            GraphTraversal2.__().Property("_name", "Soda Machine"),
                            GraphTraversal2.__().SideEffect(
                                GraphTraversal2.__()
                                    .AddE("_val")
                                    .To(GraphTraversal2.__().AddV("_val").Property(
                                        "_app", "test-app"))
                                    .Property("_key", "_properties")
                                    .Property("_ary", true)
                                    .InV()
                                    .SideEffect(
                                        GraphTraversal2.__()
                                            .AddE("_val")
                                            .To(GraphTraversal2.__()
                                                .AddV("_val")
                                                .Property("_app", "test-app"))
                                            .Property("_key", "0")
                                            .Property("_ary", false)
                                            .InV()
                                            .Property("_id", "location")
                                            .Property("name", "Soda machine location")
                                            .Property("kind", "property")
                                            .Property("type", "string"))
                                    .SideEffect(
                                        GraphTraversal2.__()
                                            .AddE("_val")
                                            .To(GraphTraversal2.__()
                                                .AddV("_val")
                                                .Property("_app", "test-app"))
                                            .Property("_key", "1")
                                            .Property("_ary", false)
                                            .InV()
                                            .Property("_id", "installer")
                                            .Property("name", "Soda machine installer")
                                            .Property("kind", "property")
                                            .Property("type", "string"))
                                    .SideEffect(
                                        GraphTraversal2.__()
                                            .AddE("_val")
                                            .To(GraphTraversal2.__()
                                                .AddV("_val")
                                                .Property("_app", "test-app"))
                                            .Property("_key", "2")
                                            .Property("_ary", false)
                                            .InV()
                                            .Property("_id", "syrup_level")
                                            .Property("name", "Syrup level")
                                            .Property("kind", "reference")
                                            .SideEffect(
                                                GraphTraversal2.__()
                                                    .AddE("_val")
                                                    .To(GraphTraversal2.__()
                                                        .AddV("_val")
                                                        .Property("_app",
                                                            "test-app"))
                                                    .Property("_key", "target")
                                                    .Property("_ary", false)
                                                    .InV()
                                                    .Property("_id", "device:soda-mixer")
                                                    .Property("type", "device")))
                                    .SideEffect(
                                        GraphTraversal2.__()
                                            .AddE("_val")
                                            .To(GraphTraversal2.__()
                                                .AddV("_val")
                                                .Property("_app", "test-app"))
                                            .Property("_key", "3")
                                            .Property("_ary", false)
                                            .InV()
                                            .Property("_id", "ice_level")
                                            .Property("name", "Ice level")
                                            .Property("kind", "reference")
                                            .SideEffect(
                                                GraphTraversal2.__()
                                                    .AddE("_val")
                                                    .To(GraphTraversal2.__()
                                                        .AddV("_val")
                                                        .Property("_app",
                                                            "test-app"))
                                                    .Property("_key", "target")
                                                    .Property("_ary", false)
                                                    .InV()
                                                    .Property("_id",
                                                        "device:ice-machine")
                                                    .Property("type", "device")))))))
                    .By(GraphTraversal2.__()
                        .AddV("product-model")
                        .Property("_app", "test-app")
                        .Property("__id", "uber-product:soda-machine")
                        .Property("__etag", "SkYTpr1hSkCL4NkpsfNwvQ==")
                        .SideEffect(GraphTraversal2.__().Union(
                            GraphTraversal2.__().Property("_name", "Uber Soda Machine"),
                            GraphTraversal2.__().SideEffect(
                                GraphTraversal2.__()
                                    .AddE("_val")
                                    .To(GraphTraversal2.__().AddV("_val").Property(
                                        "_app", "test-app"))
                                    .Property("_key", "_properties")
                                    .Property("_ary", true)
                                    .InV()
                                    .SideEffect(
                                        GraphTraversal2.__()
                                            .AddE("_val")
                                            .To(GraphTraversal2.__()
                                                .AddV("_val")
                                                .Property("_app", "test-app"))
                                            .Property("_key", "0")
                                            .Property("_ary", false)
                                            .InV()
                                            .Property("_id", "location")
                                            .Property("name", "Soda machine location")
                                            .Property("kind", "property")
                                            .Property("type", "string"))
                                    .SideEffect(
                                        GraphTraversal2.__()
                                            .AddE("_val")
                                            .To(GraphTraversal2.__()
                                                .AddV("_val")
                                                .Property("_app", "test-app"))
                                            .Property("_key", "1")
                                            .Property("_ary", false)
                                            .InV()
                                            .Property("_id", "installer")
                                            .Property("name", "Soda machine installer")
                                            .Property("kind", "property")
                                            .Property("type", "string"))
                                    .SideEffect(
                                        GraphTraversal2.__()
                                            .AddE("_val")
                                            .To(GraphTraversal2.__()
                                                .AddV("_val")
                                                .Property("_app", "test-app"))
                                            .Property("_key", "2")
                                            .Property("_ary", false)
                                            .InV()
                                            .Property("_id", "syrup_level")
                                            .Property("name", "Syrup Level")
                                            .Property("kind", "reference")
                                            .SideEffect(
                                                GraphTraversal2.__()
                                                    .AddE("_val")
                                                    .To(GraphTraversal2.__()
                                                        .AddV("_val")
                                                        .Property("_app",
                                                            "test-app"))
                                                    .Property("_key", "target")
                                                    .Property("_ary", false)
                                                    .InV()
                                                    .Property("_id",
                                                        "product:soda-machine")
                                                    .Property("type", "product")))))))
                    .By(GraphTraversal2.__()
                        .AddV("device-model")
                        .Property("_app", "test-app")
                        .Property("__id", "device:ice-machine")
                        .Property("__etag", "SWnFiMWDTVGOWUJvcqCbtg==")
                        .SideEffect(GraphTraversal2.__().Union(
                            GraphTraversal2.__().Property("_name", "Ice Machine"),
                            GraphTraversal2.__().SideEffect(
                                GraphTraversal2.__()
                                    .AddE("_val")
                                    .To(GraphTraversal2.__().AddV("_val").Property(
                                        "_app", "test-app"))
                                    .Property("_key", "_properties")
                                    .Property("_ary", true)
                                    .InV()
                                    .SideEffect(
                                        GraphTraversal2.__()
                                            .AddE("_val")
                                            .To(GraphTraversal2.__()
                                                .AddV("_val")
                                                .Property("_app", "test-app"))
                                            .Property("_key", "0")
                                            .Property("_ary", false)
                                            .InV()
                                            .Property("_id", "firmware_version")
                                            .Property("name", "Firmware Version")
                                            .Property("kind", "desired")
                                            .Property("type", "string")
                                            .Property("path", "/firmware_version"))
                                    .SideEffect(GraphTraversal2.__()
                                        .AddE("_val")
                                        .To(GraphTraversal2.__()
                                            .AddV("_val")
                                            .Property("_app",
                                                "test-app"))
                                        .Property("_key", "1")
                                        .Property("_ary", false)
                                        .InV()
                                        .Property("_id", "serial_number")
                                        .Property("name", "Serial Number")
                                        .Property("kind", "desired")
                                        .Property("type", "string")
                                        .Property("path", "/serial_number"))
                                    .SideEffect(GraphTraversal2.__()
                                        .AddE("_val")
                                        .To(GraphTraversal2.__()
                                            .AddV("_val")
                                            .Property("_app",
                                                "test-app"))
                                        .Property("_key", "2")
                                        .Property("_ary", false)
                                        .InV()
                                        .Property("_id", "ice_level")
                                        .Property("name", "Ice Level")
                                        .Property("kind", "reported")
                                        .Property("type", "number")
                                        .Property("path", "/ice_level"))))))
                    .By(GraphTraversal2.__()
                        .AddV("device-model")
                        .Property("_app", "test-app")
                        .Property("__id", "device:soda-mixer")
                        .Property("__etag", "lsRrd7JWSBqW9kiBVPS7aQ==")
                        .SideEffect(GraphTraversal2.__().Union(
                            GraphTraversal2.__().Property("_name", "Soda Mixer"),
                            GraphTraversal2.__().SideEffect(
                                GraphTraversal2.__()
                                    .AddE("_val")
                                    .To(GraphTraversal2.__().AddV("_val").Property(
                                        "_app", "test-app"))
                                    .Property("_key", "_properties")
                                    .Property("_ary", true)
                                    .InV()
                                    .SideEffect(
                                        GraphTraversal2.__()
                                            .AddE("_val")
                                            .To(GraphTraversal2.__()
                                                .AddV("_val")
                                                .Property("_app", "test-app"))
                                            .Property("_key", "0")
                                            .Property("_ary", false)
                                            .InV()
                                            .Property("_id", "firmware_version")
                                            .Property("name", "Firmware Version")
                                            .Property("kind", "desired")
                                            .Property("type", "string")
                                            .Property("path", "/firmware_version"))
                                    .SideEffect(GraphTraversal2.__()
                                        .AddE("_val")
                                        .To(GraphTraversal2.__()
                                            .AddV("_val")
                                            .Property("_app",
                                                "test-app"))
                                        .Property("_key", "1")
                                        .Property("_ary", false)
                                        .InV()
                                        .Property("_id", "serial_number")
                                        .Property("name", "Serial Number")
                                        .Property("kind", "desired")
                                        .Property("type", "string")
                                        .Property("path", "/serial_number"))
                                    .SideEffect(GraphTraversal2.__()
                                        .AddE("_val")
                                        .To(GraphTraversal2.__()
                                            .AddV("_val")
                                            .Property("_app",
                                                "test-app"))
                                        .Property("_key", "2")
                                        .Property("_ary", false)
                                        .InV()
                                        .Property("_id", "co2_level")
                                        .Property("name", "CO2 Level")
                                        .Property("kind", "reported")
                                        .Property("type", "number")
                                        .Property("path", "/co2_level"))
                                    .SideEffect(
                                        GraphTraversal2.__()
                                            .AddE("_val")
                                            .To(GraphTraversal2.__()
                                                .AddV("_val")
                                                .Property("_app", "test-app"))
                                            .Property("_key", "3")
                                            .Property("_ary", false)
                                            .InV()
                                            .Property("_id", "syrup_level")
                                            .Property("name", "Syrup Level")
                                            .Property("kind", "reported")
                                            .Property("type", "number")
                                            .Property("path", "/syrup_level"))))))
                    .As("#v")
                    .Project("#e0", "#e1", "#e2", "#e3", "#e4", "#e5")
                    .By(GraphTraversal2.__()
                        .Select("#v2")
                        .AddE("device-product")
                        .To(GraphTraversal2.__().Select("#v").Select("#v0")))
                    .By(GraphTraversal2.__()
                        .Select("#v3")
                        .AddE("device-product")
                        .To(GraphTraversal2.__().Select("#v").Select("#v0")))
                    .By(GraphTraversal2.__()
                        .Select("#v0")
                        .AddE("product-product")
                        .To(GraphTraversal2.__().Select("#v").Select("#v1")))
                    .By(GraphTraversal2.__()
                        .Select("#v0")
                        .AddE("ref")
                        .To(GraphTraversal2.__().Select("#v").Select("#v3"))
                        .Property("_key", "syrup_level")
                        .Property("_ref", "syrup_level"))
                    .By(GraphTraversal2.__()
                        .Select("#v0")
                        .AddE("ref")
                        .To(GraphTraversal2.__().Select("#v").Select("#v2"))
                        .Property("_key", "ice_level")
                        .Property("_ref", "ice_level"))
                    .By(GraphTraversal2.__()
                        .Select("#v1")
                        .AddE("ref")
                        .To(GraphTraversal2.__().Select("#v").Select("#v0"))
                        .Property("_key", "syrup_level")
                        .Property("_ref", "syrup_level"))
                    .As("#e")
                    .Union(
                        GraphTraversal2.__()
                            .Select("#v")
                            .Union(GraphTraversal2.__().Select("#v0").As("#a").Constant(
                                new List<string>() { "_name", "_properties" }),
                                GraphTraversal2.__().Select("#v1").As("#a").Constant(
                                    new List<string>() { "_name", "_properties" }),
                                GraphTraversal2.__().Select("#v2").As("#a").Constant(
                                    new List<string>() { "_name", "_properties" }),
                                GraphTraversal2.__().Select("#v3").As("#a").Constant(
                                    new List<string>() { "_name", "_properties" }))
                            .As("#p"),
                        GraphTraversal2.__()
                            .Select("#e")
                            .Union(GraphTraversal2.__().Select("#e0"),
                                GraphTraversal2.__().Select("#e1"),
                                GraphTraversal2.__().Select("#e2"),
                                GraphTraversal2.__().Select("#e3"),
                                GraphTraversal2.__().Select("#e4"),
                                GraphTraversal2.__().Select("#e5"))
                            .As("#f")
                            .Union(GraphTraversal2.__().InV().As("#a").Select("#f").OutV(),
                                GraphTraversal2.__().OutV().As("#a").Select("#f").InV())
                            .Map(GraphTraversal2.__()
                                .Optional(GraphTraversal2.__().Out("mdl"))
                                .As("#m")
                                .Select("#a")
                                .Optional(GraphTraversal2.__().Out("mdl"))
                                .InE("ref")
                                .And(GraphTraversal2.__().OutV().Where(
                                    Predicate.eq("#m")))
                                .Values("_key")
                                .Fold())
                            .As("#p"))
                    .Select("#a")
                    .Union(
                        GraphTraversal2.__().Identity(),
                        GraphTraversal2.__()
                            .As("@v")
                            .FlatMap(GraphTraversal2.__()
                                .Optional(GraphTraversal2.__().Out("mdl"))
                                .InE("ref")
                                .And(GraphTraversal2.__().Values("_key").Where(
                                    Predicate.within("#p"))))
                            .Repeat(GraphTraversal2.__()
                                .As("@e")
                                .FlatMap(GraphTraversal2.__()
                                    .OutV()
                                    .As("mdl")
                                    .Select(GremlinKeyword.Pop.Last, "@v")
                                    .Both()
                                    .Dedup()
                                    .And(GraphTraversal2.__()
                                        .Optional(GraphTraversal2.__()
                                            .Out("mdl"))
                                        .Where(Predicate.eq("mdl"))))
                                .As("@v")
                                .Optional(GraphTraversal2.__().FlatMap(
                                    GraphTraversal2.__()
                                        .Select(GremlinKeyword.Pop.Last, "@e")
                                        .Values("_key")
                                        .As("key")
                                        .Select(GremlinKeyword.Pop.Last, "@v")
                                        .Optional(GraphTraversal2.__().Out("mdl"))
                                        .InE("ref")
                                        .And(GraphTraversal2.__()
                                            .Values("_ref")
                                            .Where(Predicate.eq("key"))))))
                            .Until(GraphTraversal2.__().FlatMap(
                                GraphTraversal2.__()
                                    .As("res")
                                    .Select(GremlinKeyword.Pop.Last, "@v")
                                    .Where(Predicate.eq("res"))))
                            .Select("@v")
                            .Unfold())
                    .Dedup()
                    .Project("_id", "type", "etag")
                    .By(GraphTraversal2.__().Values("__id"))
                    .By(GraphTraversal2.__().Label())
                    .By(GraphTraversal2.__().Values("__etag"))).Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void Test3ImportInstances()
        {
            GraphViewConnection connection = GetGraphViewConnection();
            //connection.ResetCollection();

            GraphViewCommand graph = GetGraphViewCommand(connection);

            //var pre_fetch = graph.g()
            //    .V()
            //    .Has("_app", "test-app")
            //    .Has("__id", "test-app")
            //    .HasLabel("application")
            //    .Coalesce(
            //        GraphTraversal2.__().Union(GraphTraversal2.__()
            //            .Not(GraphTraversal2.__()
            //                .V()
            //                .Has("_app", "test-app")
            //                .Has("__id", "test-app")
            //                .HasLabel("application"))
            //            .Constant("~0"),
            //            GraphTraversal2.__()
            //                .V()
            //                .Has("_app", "test-app")
            //                .Has("__id", "test-app")
            //                .HasLabel("application")
            //                .Has("_provisioningState", 0)
            //                .Constant("~1"),
            //            GraphTraversal2.__()
            //                .V()
            //                .Has("_app", "test-app")
            //                .Has("__id", "test-app")
            //                .HasLabel("application")
            //                .Has("_provisioningState", 2)
            //                .Constant("~2"),
            //            GraphTraversal2.__()
            //                .V()
            //                .Has("_app", "test-app")
            //                .Has("__id", "test-app")
            //                .HasLabel("application")
            //                .Has("_deleted", true)
            //                .Constant("~3")),
            //        GraphTraversal2.__()
            //            .FlatMap(
            //                GraphTraversal2.__()
            //                    .Project("nodes", "edges")
            //                    .By(GraphTraversal2.__()
            //                        .Union(GraphTraversal2.__()
            //                            .V()
            //                            .Has("_app", "test-app")
            //                            .Has("__id", "product:soda-machine")
            //                            .HasLabel("product-model"),
            //                            GraphTraversal2.__()
            //                                .V()
            //                                .Has("_app", "test-app")
            //                                .Has("__id", "uber-product:soda-machine")
            //                                .HasLabel("product-model"),
            //                            GraphTraversal2.__()
            //                                .V()
            //                                .Has("_app", "test-app")
            //                                .Has("__id", "device:ice-machine")
            //                                .HasLabel("device-model"),
            //                            GraphTraversal2.__()
            //                                .V()
            //                                .Has("_app", "test-app")
            //                                .Has("__id", "device:soda-mixer")
            //                                .HasLabel("device-model"))
            //                        .Fold())
            //                    .By(GraphTraversal2.__()
            //                        .Union(
            //                            GraphTraversal2.__()
            //                                .V()
            //                                .Has("_app", "test-app")
            //                                .Has("__id", "device:ice-machine")
            //                                .HasLabel("device-model")
            //                                .FlatMap(
            //                                    GraphTraversal2.__()
            //                                        .As("src")
            //                                        .FlatMap(
            //                                            GraphTraversal2.__()
            //                                                .V()
            //                                                .Has("_app", "test-app")
            //                                                .Has("__id",
            //                                                    "product:soda-machine")
            //                                                .HasLabel("product-model"))
            //                                        .As("tgt")
            //                                        .Select("src")
            //                                        .OutE("device-product")
            //                                        .And(GraphTraversal2.__()
            //                                            .InV()
            //                                            .Where(
            //                                                Predicate.eq("tgt")))),
            //                            GraphTraversal2.__()
            //                                .V()
            //                                .Has("_app", "test-app")
            //                                .Has("__id", "device:soda-mixer")
            //                                .HasLabel("device-model")
            //                                .FlatMap(
            //                                    GraphTraversal2.__()
            //                                        .As("src")
            //                                        .FlatMap(
            //                                            GraphTraversal2.__()
            //                                                .V()
            //                                                .Has("_app", "test-app")
            //                                                .Has("__id",
            //                                                    "product:soda-machine")
            //                                                .HasLabel("product-model"))
            //                                        .As("tgt")
            //                                        .Select("src")
            //                                        .OutE("device-product")
            //                                        .And(GraphTraversal2.__()
            //                                            .InV()
            //                                            .Where(
            //                                                Predicate.eq("tgt")))),
            //                            GraphTraversal2.__()
            //                                .V()
            //                                .Has("_app", "test-app")
            //                                .Has("__id", "product:soda-machine")
            //                                .HasLabel("product-model")
            //                                .FlatMap(
            //                                    GraphTraversal2.__()
            //                                        .As("src")
            //                                        .FlatMap(
            //                                            GraphTraversal2.__()
            //                                                .V()
            //                                                .Has("_app", "test-app")
            //                                                .Has("__id",
            //                                                    "uber-product:soda-machine")
            //                                                .HasLabel("product-model"))
            //                                        .As("tgt")
            //                                        .Select("src")
            //                                        .OutE("product-product")
            //                                        .And(GraphTraversal2.__()
            //                                            .InV()
            //                                            .Where(
            //                                                Predicate.eq("tgt")))))
            //                        .Fold())
            //                    .SideEffect(
            //                        GraphTraversal2.__()
            //                            .Select("edges")
            //                            .Unfold()
            //                            .Project("name", "source", "target", "properties")
            //                            .By(GraphTraversal2.__().Label())
            //                            .By(GraphTraversal2.__()
            //                                .OutV()
            //                                .Project("_id", "type", "etag")
            //                                .By(GraphTraversal2.__().Values("__id"))
            //                                .By(GraphTraversal2.__().Label())
            //                                .By(GraphTraversal2.__().Values("__etag")))
            //                            .By(GraphTraversal2.__()
            //                                .InV()
            //                                .Project("_id", "type", "etag")
            //                                .By(GraphTraversal2.__().Values("__id"))
            //                                .By(GraphTraversal2.__().Label())
            //                                .By(GraphTraversal2.__().Values("__etag")))
            //                            .By(GraphTraversal2.__()
            //                                .Properties()
            //                                .Group()
            //                                .By(GraphTraversal2.__().Key())
            //                                .By(GraphTraversal2.__().Value()))
            //                            .Store("^edges"))
            //                    .Select("nodes")
            //                    .Unfold()
            //                    .Union(GraphTraversal2.__().Identity().SideEffect(
            //                        GraphTraversal2.__().Id().Store("^ids")),
            //                        GraphTraversal2.__()
            //                            .As("@v")
            //                            .FlatMap(GraphTraversal2.__()
            //                                .Optional(
            //                                    GraphTraversal2.__().Out("mdl"))
            //                                .OutE("ref"))
            //                            .Repeat(
            //                                GraphTraversal2.__()
            //                                    .As("@e")
            //                                    .FlatMap(
            //                                        GraphTraversal2.__()
            //                                            .InV()
            //                                            .As("mdl")
            //                                            .Select(GremlinKeyword.Pop.Last,
            //                                                "@v")
            //                                            .Both()
            //                                            .Dedup()
            //                                            .And(GraphTraversal2.__()
            //                                                .Optional(
            //                                                    GraphTraversal2.__()
            //                                                        .Out("mdl"))
            //                                                .Where(Predicate.eq(
            //                                                    "mdl"))))
            //                                    .As("@v")
            //                                    .Optional(GraphTraversal2.__().FlatMap(
            //                                        GraphTraversal2.__()
            //                                            .Select(GremlinKeyword.Pop.Last,
            //                                                "@e")
            //                                            .Values("_ref")
            //                                            .As("key")
            //                                            .Select(GremlinKeyword.Pop.Last,
            //                                                "@v")
            //                                            .Optional(GraphTraversal2.__()
            //                                                .Out("mdl"))
            //                                            .OutE("ref")
            //                                            .And(GraphTraversal2.__()
            //                                                .Values("_key")
            //                                                .Where(Predicate.eq(
            //                                                    "key"))))))
            //                            .Until(GraphTraversal2.__().FlatMap(
            //                                GraphTraversal2.__()
            //                                    .As("res")
            //                                    .Select(GremlinKeyword.Pop.Last, "@v")
            //                                    .Where(Predicate.eq("res"))))
            //                            .SideEffect(
            //                                GraphTraversal2.__()
            //                                    .Project("data", "info")
            //                                    .By(GraphTraversal2.__()
            //                                        .Select("@e")
            //                                        .Unfold()
            //                                        .Project("key", "ref")
            //                                        .By(GraphTraversal2.__().Values(
            //                                            "_key"))
            //                                        .By(GraphTraversal2.__().Values(
            //                                            "_ref"))
            //                                        .Fold())
            //                                    .By(GraphTraversal2.__()
            //                                        .Select("@v")
            //                                        .Unfold()
            //                                        .Project("_id", "type", "etag")
            //                                        .By(GraphTraversal2.__().Values(
            //                                            "__id"))
            //                                        .By(GraphTraversal2.__().Label())
            //                                        .By(GraphTraversal2.__().Values(
            //                                            "__etag"))
            //                                        .Fold())
            //                                    .Store("^refs")))
            //                    .Dedup()
            //                    .Union(GraphTraversal2.__().Identity().SideEffect(
            //                        GraphTraversal2.__()
            //                            .Group("^mdls")
            //                            .By(GraphTraversal2.__().Id())
            //                            .By(GraphTraversal2.__().Coalesce(
            //                                GraphTraversal2.__().Out("mdl").Values(
            //                                    "__id"),
            //                                GraphTraversal2.__().Constant("")))),
            //                        GraphTraversal2.__().Out("mdl"))
            //                    .Dedup())
            //            .Union(GraphTraversal2.__()
            //                .Emit()
            //                .Repeat(GraphTraversal2.__().OutE("_val").As("_").InV())
            //                .Tree(),
            //                GraphTraversal2.__().Cap("^ids"),
            //                GraphTraversal2.__().Cap("^mdls"),
            //                GraphTraversal2.__().Cap("^refs"))
            //            .Fold()
            //            .Union(GraphTraversal2.__().Identity(),
            //                GraphTraversal2.__().Cap("^edges"))).Next();

            //foreach (var result in pre_fetch)
            //{
            //    Console.WriteLine(result);
            //}

            var write = graph.g().Inject(0).Coalesce(
                GraphTraversal2.__().Union(
                    GraphTraversal2.__()
                        .Not(GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "test-app")
                            .HasLabel("application"))
                        .Constant("~0"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "test-app")
                        .HasLabel("application")
                        .Has("_provisioningState", 0)
                        .Constant("~1"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "test-app")
                        .HasLabel("application")
                        .Has("_provisioningState", 2)
                        .Constant("~2"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "test-app")
                        .HasLabel("application")
                        .Has("_deleted", true)
                        .Constant("~3"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "product:soda-machine:shop-1")
                        .HasLabel("product")
                        .Constant("~4"),
                    GraphTraversal2.__()
                        .Not(GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "product:soda-machine")
                            .HasLabel("product-model")
                            .Has("__etag", "B0vDw1xnS/agXzX9F7wxHg=="))
                        .Constant("~5"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "product:soda-machine:shop-2")
                        .HasLabel("product")
                        .Constant("~6"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "product:soda-machine:shop-3.1")
                        .HasLabel("product")
                        .Constant("~7"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "product:soda-machine:shop-3.2")
                        .HasLabel("product")
                        .Constant("~8"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "uber-product:soda-machine:shop-3")
                        .HasLabel("product")
                        .Constant("~9"),
                    GraphTraversal2.__()
                        .Not(GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "uber-product:soda-machine")
                            .HasLabel("product-model")
                            .Has("__etag", "SkYTpr1hSkCL4NkpsfNwvQ=="))
                        .Constant("~10"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "device:ice-machine:shop-1")
                        .HasLabel("device")
                        .Constant("~11"),
                    GraphTraversal2.__()
                        .Not(GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "device:ice-machine")
                            .HasLabel("device-model")
                            .Has("__etag", "SWnFiMWDTVGOWUJvcqCbtg=="))
                        .Constant("~12"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "device:soda-mixer:shop-1")
                        .HasLabel("device")
                        .Constant("~13"),
                    GraphTraversal2.__()
                        .Not(GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "device:soda-mixer")
                            .HasLabel("device-model")
                            .Has("__etag", "lsRrd7JWSBqW9kiBVPS7aQ=="))
                        .Constant("~14"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "device:ice-machine:shop-2")
                        .HasLabel("device")
                        .Constant("~15"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "device:cola-mixer:shop-2")
                        .HasLabel("device")
                        .Constant("~16"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "device:root-beer-mixer:shop-2")
                        .HasLabel("device")
                        .Constant("~17"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "device:lemonade-mixer:shop-2")
                        .HasLabel("device")
                        .Constant("~18"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "device:ice-machine:shop-3.1")
                        .HasLabel("device")
                        .Constant("~19"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "device:soda-mixer:shop-3.1")
                        .HasLabel("device")
                        .Constant("~20"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "device:ice-machine:shop-3.2")
                        .HasLabel("device")
                        .Constant("~21"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "device:cola-mixer:shop-3.2")
                        .HasLabel("device")
                        .Constant("~22"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "device:kool-aid-mixer:shop-3.2")
                        .HasLabel("device")
                        .Constant("~23")),
                GraphTraversal2.__()
                    .Project("#v0", "#v1", "#v2", "#v3", "#v4", "#v5", "#v6", "#v7", "#v8",
                        "#v9", "#v10", "#v11", "#v12", "#v13", "#v14", "#v15")
                    .By(GraphTraversal2.__()
                        .AddV("product")
                        .Property("_app", "test-app")
                        .Property("__id", "product:soda-machine:shop-1")
                        .Property("__etag", "gtxVWBOYROCC4We9TdK8yA==")
                        .SideEffect(GraphTraversal2.__().Union(
                            GraphTraversal2.__().Property("name", "Soda Machine #1"),
                            GraphTraversal2.__().Property("location",
                                "Building 43 - Garage"),
                            GraphTraversal2.__().Property("installer", "Jack Brown"),
                            GraphTraversal2.__().AddE("mdl").To(
                                GraphTraversal2.__()
                                    .V()
                                    .Has("_app", "test-app")
                                    .Has("__id", "product:soda-machine")
                                    .HasLabel("product-model")))))
                    .By(GraphTraversal2.__()
                        .AddV("product")
                        .Property("_app", "test-app")
                        .Property("__id", "product:soda-machine:shop-2")
                        .Property("__etag", "XVALE7oMRR63jfS4biDS9w==")
                        .SideEffect(GraphTraversal2.__().Union(
                            GraphTraversal2.__().Property("name", "Soda Machine #2"),
                            GraphTraversal2.__().Property("location",
                                "Building 44 - Cafe"),
                            GraphTraversal2.__().Property("installer", "Jim Johns"),
                            GraphTraversal2.__().AddE("mdl").To(
                                GraphTraversal2.__()
                                    .V()
                                    .Has("_app", "test-app")
                                    .Has("__id", "product:soda-machine")
                                    .HasLabel("product-model")))))
                    .By(GraphTraversal2.__()
                        .AddV("product")
                        .Property("_app", "test-app")
                        .Property("__id", "product:soda-machine:shop-3.1")
                        .Property("__etag", "WJAjOSurTmGZ6CnfBELyUA==")
                        .SideEffect(GraphTraversal2.__().Union(
                            GraphTraversal2.__().Property("name", "Soda Machine #3.1"),
                            GraphTraversal2.__().Property(
                                "location", "Microsoft Visitor Center - Ground Floor"),
                            GraphTraversal2.__().Property("installer", "Eva Green"),
                            GraphTraversal2.__().AddE("mdl").To(
                                GraphTraversal2.__()
                                    .V()
                                    .Has("_app", "test-app")
                                    .Has("__id", "product:soda-machine")
                                    .HasLabel("product-model")))))
                    .By(GraphTraversal2.__()
                        .AddV("product")
                        .Property("_app", "test-app")
                        .Property("__id", "product:soda-machine:shop-3.2")
                        .Property("__etag", "3pO/jDqlR0mfoDy1csN+Yw==")
                        .SideEffect(GraphTraversal2.__().Union(
                            GraphTraversal2.__().Property("name", "Soda Machine #3.2"),
                            GraphTraversal2.__().Property("location",
                                "Building 43 - Second Floor"),
                            GraphTraversal2.__().Property("installer", "Ronnie Wood"),
                            GraphTraversal2.__().AddE("mdl").To(
                                GraphTraversal2.__()
                                    .V()
                                    .Has("_app", "test-app")
                                    .Has("__id", "product:soda-machine")
                                    .HasLabel("product-model")))))
                    .By(GraphTraversal2.__()
                        .AddV("product")
                        .Property("_app", "test-app")
                        .Property("__id", "uber-product:soda-machine:shop-3")
                        .Property("__etag", "TMaJk/CGRyurJIle/FncMA==")
                        .SideEffect(GraphTraversal2.__().Union(
                            GraphTraversal2.__().Property("name",
                                "Uber Soda Machine #3"),
                            GraphTraversal2.__().Property("location",
                                "Building 43 - Third Floor"),
                            GraphTraversal2.__().Property("installer", "Albert Sims"),
                            GraphTraversal2.__().AddE("mdl").To(
                                GraphTraversal2.__()
                                    .V()
                                    .Has("_app", "test-app")
                                    .Has("__id", "uber-product:soda-machine")
                                    .HasLabel("product-model")))))
                    .By(GraphTraversal2.__()
                        .AddV("device")
                        .Property("_app", "test-app")
                        .Property("__id", "device:ice-machine:shop-1")
                        .Property("__etag", "wPY/iDq7RiqmokdVPeENcQ==")
                        .SideEffect(GraphTraversal2.__().Union(
                            GraphTraversal2.__().Property("name",
                                "Ice Machine #456789"),
                            GraphTraversal2.__().Property("serial_number",
                                "3333-456789"),
                            GraphTraversal2.__().Property("firmware_version", "1.0.0"),
                            GraphTraversal2.__().Property("ice_level", 1.2),
                            GraphTraversal2.__().AddE("mdl").To(
                                GraphTraversal2.__()
                                    .V()
                                    .Has("_app", "test-app")
                                    .Has("__id", "device:ice-machine")
                                    .HasLabel("device-model")))))
                    .By(GraphTraversal2.__()
                        .AddV("device")
                        .Property("_app", "test-app")
                        .Property("__id", "device:soda-mixer:shop-1")
                        .Property("__etag", "uA54hXcmQmyaRwOAkQWcWQ==")
                        .SideEffect(GraphTraversal2.__().Union(
                            GraphTraversal2.__().Property("name", "Soda Mixer #123456"),
                            GraphTraversal2.__().Property("serial_number",
                                "4444-123456"),
                            GraphTraversal2.__().Property("firmware_version", "1.1.0"),
                            GraphTraversal2.__().Property("co2_level", 0.1),
                            GraphTraversal2.__().Property("syrup_level", 0.1),
                            GraphTraversal2.__().AddE("mdl").To(
                                GraphTraversal2.__()
                                    .V()
                                    .Has("_app", "test-app")
                                    .Has("__id", "device:soda-mixer")
                                    .HasLabel("device-model")))))
                    .By(GraphTraversal2.__()
                        .AddV("device")
                        .Property("_app", "test-app")
                        .Property("__id", "device:ice-machine:shop-2")
                        .Property("__etag", "FBYA/q6dTE6Ny7/v3iTNQg==")
                        .SideEffect(GraphTraversal2.__().Union(
                            GraphTraversal2.__().Property("name",
                                "Ice Machine #456123"),
                            GraphTraversal2.__().Property("serial_number",
                                "3333-456123"),
                            GraphTraversal2.__().Property("firmware_version", "1.1.0"),
                            GraphTraversal2.__().Property("ice_level", 2.4),
                            GraphTraversal2.__().AddE("mdl").To(
                                GraphTraversal2.__()
                                    .V()
                                    .Has("_app", "test-app")
                                    .Has("__id", "device:ice-machine")
                                    .HasLabel("device-model")))))
                    .By(GraphTraversal2.__()
                        .AddV("device")
                        .Property("_app", "test-app")
                        .Property("__id", "device:cola-mixer:shop-2")
                        .Property("__etag", "oqielLa9QWeVjd2p9lWZPQ==")
                        .SideEffect(GraphTraversal2.__().Union(
                            GraphTraversal2.__().Property("name", "Cola Mixer #789123"),
                            GraphTraversal2.__().Property("serial_number",
                                "4444-789123"),
                            GraphTraversal2.__().Property("firmware_version", "1.0.1"),
                            GraphTraversal2.__().Property("co2_level", 0.2),
                            GraphTraversal2.__().Property("syrup_level", 0.2),
                            GraphTraversal2.__().AddE("mdl").To(
                                GraphTraversal2.__()
                                    .V()
                                    .Has("_app", "test-app")
                                    .Has("__id", "device:soda-mixer")
                                    .HasLabel("device-model")))))
                    .By(GraphTraversal2.__()
                        .AddV("device")
                        .Property("_app", "test-app")
                        .Property("__id", "device:root-beer-mixer:shop-2")
                        .Property("__etag", "4u7k7lAaSKuUUL2iHbBcRQ==")
                        .SideEffect(GraphTraversal2.__().Union(
                            GraphTraversal2.__().Property("name",
                                "Root Beer Mixer #654123"),
                            GraphTraversal2.__().Property("serial_number",
                                "4444-654123"),
                            GraphTraversal2.__().Property("firmware_version", "1.0.0"),
                            GraphTraversal2.__().Property("co2_level", 0.3),
                            GraphTraversal2.__().Property("syrup_level", 0.3),
                            GraphTraversal2.__().AddE("mdl").To(
                                GraphTraversal2.__()
                                    .V()
                                    .Has("_app", "test-app")
                                    .Has("__id", "device:soda-mixer")
                                    .HasLabel("device-model")))))
                    .By(GraphTraversal2.__()
                        .AddV("device")
                        .Property("_app", "test-app")
                        .Property("__id", "device:lemonade-mixer:shop-2")
                        .Property("__etag", "kkLGbSdzSbiCi7w7VM12gw==")
                        .SideEffect(GraphTraversal2.__().Union(
                            GraphTraversal2.__().Property("name",
                                "Lemonade Mixer #654122"),
                            GraphTraversal2.__().Property("serial_number",
                                "4444-654122"),
                            GraphTraversal2.__().Property("firmware_version", "1.0.1"),
                            GraphTraversal2.__().Property("co2_level", 0.4),
                            GraphTraversal2.__().Property("syrup_level", 0.4),
                            GraphTraversal2.__().AddE("mdl").To(
                                GraphTraversal2.__()
                                    .V()
                                    .Has("_app", "test-app")
                                    .Has("__id", "device:soda-mixer")
                                    .HasLabel("device-model")))))
                    .By(GraphTraversal2.__()
                        .AddV("device")
                        .Property("_app", "test-app")
                        .Property("__id", "device:ice-machine:shop-3.1")
                        .Property("__etag", "cWI7zlmBSNei70b7zoqghw==")
                        .SideEffect(GraphTraversal2.__().Union(
                            GraphTraversal2.__().Property("name",
                                "Ice Machine #654111"),
                            GraphTraversal2.__().Property("serial_number",
                                "3333-654111"),
                            GraphTraversal2.__().Property("firmware_version", "1.1.1"),
                            GraphTraversal2.__().Property("ice_level", 3.6),
                            GraphTraversal2.__().AddE("mdl").To(
                                GraphTraversal2.__()
                                    .V()
                                    .Has("_app", "test-app")
                                    .Has("__id", "device:ice-machine")
                                    .HasLabel("device-model")))))
                    .By(GraphTraversal2.__()
                        .AddV("device")
                        .Property("_app", "test-app")
                        .Property("__id", "device:soda-mixer:shop-3.1")
                        .Property("__etag", "yOXsJu84SJW6Amtm9FF9ug==")
                        .SideEffect(GraphTraversal2.__().Union(
                            GraphTraversal2.__().Property("name", "Soda Mixer #987456"),
                            GraphTraversal2.__().Property("serial_number",
                                "4444-987456"),
                            GraphTraversal2.__().Property("firmware_version", "1.1.2"),
                            GraphTraversal2.__().Property("co2_level", 0.5),
                            GraphTraversal2.__().Property("syrup_level", 0.5),
                            GraphTraversal2.__().AddE("mdl").To(
                                GraphTraversal2.__()
                                    .V()
                                    .Has("_app", "test-app")
                                    .Has("__id", "device:soda-mixer")
                                    .HasLabel("device-model")))))
                    .By(GraphTraversal2.__()
                        .AddV("device")
                        .Property("_app", "test-app")
                        .Property("__id", "device:ice-machine:shop-3.2")
                        .Property("__etag", "XTb4lY83SLes2c+gZZ6vfA==")
                        .SideEffect(GraphTraversal2.__().Union(
                            GraphTraversal2.__().Property("name",
                                "Ice Machine #555444"),
                            GraphTraversal2.__().Property("serial_number",
                                "3333-555444"),
                            GraphTraversal2.__().Property("firmware_version", "1.0.0"),
                            GraphTraversal2.__().Property("ice_level", 4.8),
                            GraphTraversal2.__().AddE("mdl").To(
                                GraphTraversal2.__()
                                    .V()
                                    .Has("_app", "test-app")
                                    .Has("__id", "device:ice-machine")
                                    .HasLabel("device-model")))))
                    .By(GraphTraversal2.__()
                        .AddV("device")
                        .Property("_app", "test-app")
                        .Property("__id", "device:cola-mixer:shop-3.2")
                        .Property("__etag", "G1lCXUnhRSCqohWUaZza8w==")
                        .SideEffect(GraphTraversal2.__().Union(
                            GraphTraversal2.__().Property("name", "Cola Mixer #111222"),
                            GraphTraversal2.__().Property("serial_number",
                                "4444-111222"),
                            GraphTraversal2.__().Property("firmware_version", "1.0.0"),
                            GraphTraversal2.__().Property("co2_level", 0.6),
                            GraphTraversal2.__().Property("syrup_level", 0.6),
                            GraphTraversal2.__().AddE("mdl").To(
                                GraphTraversal2.__()
                                    .V()
                                    .Has("_app", "test-app")
                                    .Has("__id", "device:soda-mixer")
                                    .HasLabel("device-model")))))
                    .By(GraphTraversal2.__()
                        .AddV("device")
                        .Property("_app", "test-app")
                        .Property("__id", "device:kool-aid-mixer:shop-3.2")
                        .Property("__etag", "E5h6wBBpRjuDWkVaJ/Ud+Q==")
                        .SideEffect(GraphTraversal2.__().Union(
                            GraphTraversal2.__().Property("name",
                                "Kool Aid Mixer #999888"),
                            GraphTraversal2.__().Property("serial_number",
                                "4444-999888"),
                            GraphTraversal2.__().Property("firmware_version", "1.0.2"),
                            GraphTraversal2.__().Property("co2_level", 0.7),
                            GraphTraversal2.__().Property("syrup_level", 0.7),
                            GraphTraversal2.__().AddE("mdl").To(
                                GraphTraversal2.__()
                                    .V()
                                    .Has("_app", "test-app")
                                    .Has("__id", "device:soda-mixer")
                                    .HasLabel("device-model")))))
                    .As("#v")
                    .Project("#e0", "#e1", "#e2", "#e3", "#e4", "#e5", "#e6", "#e7", "#e8",
                        "#e9", "#e10", "#e11", "#e12")
                    .By(GraphTraversal2.__()
                        .Select("#v5")
                        .AddE("device-product")
                        .To(GraphTraversal2.__().Select("#v").Select("#v0")))
                    .By(GraphTraversal2.__()
                        .Select("#v6")
                        .AddE("device-product")
                        .To(GraphTraversal2.__().Select("#v").Select("#v0")))
                    .By(GraphTraversal2.__()
                        .Select("#v7")
                        .AddE("device-product")
                        .To(GraphTraversal2.__().Select("#v").Select("#v1")))
                    .By(GraphTraversal2.__()
                        .Select("#v8")
                        .AddE("device-product")
                        .To(GraphTraversal2.__().Select("#v").Select("#v1")))
                    .By(GraphTraversal2.__()
                        .Select("#v9")
                        .AddE("device-product")
                        .To(GraphTraversal2.__().Select("#v").Select("#v1")))
                    .By(GraphTraversal2.__()
                        .Select("#v10")
                        .AddE("device-product")
                        .To(GraphTraversal2.__().Select("#v").Select("#v1")))
                    .By(GraphTraversal2.__()
                        .Select("#v11")
                        .AddE("device-product")
                        .To(GraphTraversal2.__().Select("#v").Select("#v2")))
                    .By(GraphTraversal2.__()
                        .Select("#v12")
                        .AddE("device-product")
                        .To(GraphTraversal2.__().Select("#v").Select("#v2")))
                    .By(GraphTraversal2.__()
                        .Select("#v13")
                        .AddE("device-product")
                        .To(GraphTraversal2.__().Select("#v").Select("#v3")))
                    .By(GraphTraversal2.__()
                        .Select("#v14")
                        .AddE("device-product")
                        .To(GraphTraversal2.__().Select("#v").Select("#v3")))
                    .By(GraphTraversal2.__()
                        .Select("#v15")
                        .AddE("device-product")
                        .To(GraphTraversal2.__().Select("#v").Select("#v3")))
                    .By(GraphTraversal2.__()
                        .Select("#v2")
                        .AddE("product-product")
                        .To(GraphTraversal2.__().Select("#v").Select("#v4")))
                    .By(GraphTraversal2.__()
                        .Select("#v3")
                        .AddE("product-product")
                        .To(GraphTraversal2.__().Select("#v").Select("#v4")))
                    .As("#e")
                    .Union(
                        GraphTraversal2.__()
                            .Select("#v")
                            .Union(GraphTraversal2.__().Select("#v0").As("#a").Constant(
                                new List<string>() { "name", "location", "installer" }),
                                GraphTraversal2.__().Select("#v1").As("#a").Constant(
                                    new List<string>() { "name", "location", "installer" }),
                                GraphTraversal2.__().Select("#v2").As("#a").Constant(
                                    new List<string>() { "name", "location", "installer" }),
                                GraphTraversal2.__().Select("#v3").As("#a").Constant(
                                    new List<string>() { "name", "location", "installer" }),
                                GraphTraversal2.__().Select("#v4").As("#a").Constant(
                                    new List<string>() { "name", "location", "installer" }),
                                GraphTraversal2.__().Select("#v5").As("#a").Constant(
                                    new List<string>()
                                    {
                                        "name",
                                        "serial_number",
                                        "firmware_version",
                                        "ice_level"
                                    }),
                                GraphTraversal2.__().Select("#v6").As("#a").Constant(
                                    new List<string>()
                                    {
                                        "name",
                                        "serial_number",
                                        "firmware_version",
                                        "co2_level",
                                        "syrup_level"
                                    }),
                                GraphTraversal2.__().Select("#v7").As("#a").Constant(
                                    new List<string>()
                                    {
                                        "name",
                                        "serial_number",
                                        "firmware_version",
                                        "ice_level"
                                    }),
                                GraphTraversal2.__().Select("#v8").As("#a").Constant(
                                    new List<string>()
                                    {
                                        "name",
                                        "serial_number",
                                        "firmware_version",
                                        "co2_level",
                                        "syrup_level"
                                    }),
                                GraphTraversal2.__().Select("#v9").As("#a").Constant(
                                    new List<string>()
                                    {
                                        "name",
                                        "serial_number",
                                        "firmware_version",
                                        "co2_level",
                                        "syrup_level"
                                    }),
                                GraphTraversal2.__().Select("#v10").As("#a").Constant(
                                    new List<string>()
                                    {
                                        "name",
                                        "serial_number",
                                        "firmware_version",
                                        "co2_level",
                                        "syrup_level"
                                    }),
                                GraphTraversal2.__().Select("#v11").As("#a").Constant(
                                    new List<string>()
                                    {
                                        "name",
                                        "serial_number",
                                        "firmware_version",
                                        "ice_level"
                                    }),
                                GraphTraversal2.__().Select("#v12").As("#a").Constant(
                                    new List<string>()
                                    {
                                        "name",
                                        "serial_number",
                                        "firmware_version",
                                        "co2_level",
                                        "syrup_level"
                                    }),
                                GraphTraversal2.__().Select("#v13").As("#a").Constant(
                                    new List<string>()
                                    {
                                        "name",
                                        "serial_number",
                                        "firmware_version",
                                        "ice_level"
                                    }),
                                GraphTraversal2.__().Select("#v14").As("#a").Constant(
                                    new List<string>()
                                    {
                                        "name",
                                        "serial_number",
                                        "firmware_version",
                                        "co2_level",
                                        "syrup_level"
                                    }),
                                GraphTraversal2.__().Select("#v15").As("#a").Constant(
                                    new List<string>()
                                    {
                                        "name",
                                        "serial_number",
                                        "firmware_version",
                                        "co2_level",
                                        "syrup_level"
                                    }))
                            .As("#p"),
                        GraphTraversal2.__()
                            .Select("#e")
                            .Union(GraphTraversal2.__().Select("#e0"),
                                GraphTraversal2.__().Select("#e1"),
                                GraphTraversal2.__().Select("#e2"),
                                GraphTraversal2.__().Select("#e3"),
                                GraphTraversal2.__().Select("#e4"),
                                GraphTraversal2.__().Select("#e5"),
                                GraphTraversal2.__().Select("#e6"),
                                GraphTraversal2.__().Select("#e7"),
                                GraphTraversal2.__().Select("#e8"),
                                GraphTraversal2.__().Select("#e9"),
                                GraphTraversal2.__().Select("#e10"),
                                GraphTraversal2.__().Select("#e11"),
                                GraphTraversal2.__().Select("#e12"))
                            .As("#f")
                            .Union(GraphTraversal2.__().InV().As("#a").Select("#f").OutV(),
                                GraphTraversal2.__().OutV().As("#a").Select("#f").InV())
                            .Map(GraphTraversal2.__()
                                .Optional(GraphTraversal2.__().Out("mdl"))
                                .As("#m")
                                .Select("#a")
                                .Optional(GraphTraversal2.__().Out("mdl"))
                                .InE("ref")
                                .And(GraphTraversal2.__().OutV().Where(
                                    Predicate.eq("#m")))
                                .Values("_key")
                                .Fold())
                            .As("#p"))
                    .Select("#a")
                    .Union(
                        GraphTraversal2.__().Identity(),
                        GraphTraversal2.__()
                            .As("@v")
                            .FlatMap(GraphTraversal2.__()
                                .Optional(GraphTraversal2.__().Out("mdl"))
                                .InE("ref")
                                .And(GraphTraversal2.__().Values("_key").Where(
                                    Predicate.within("#p"))))
                            .Repeat(GraphTraversal2.__()
                                .As("@e")
                                .FlatMap(GraphTraversal2.__()
                                    .OutV()
                                    .As("mdl")
                                    .Select(GremlinKeyword.Pop.Last, "@v")
                                    .Both()
                                    .Dedup()
                                    .And(GraphTraversal2.__()
                                        .Optional(GraphTraversal2.__()
                                            .Out("mdl"))
                                        .Where(Predicate.eq("mdl"))))
                                .As("@v")
                                .Optional(GraphTraversal2.__().FlatMap(
                                    GraphTraversal2.__()
                                        .Select(GremlinKeyword.Pop.Last, "@e")
                                        .Values("_key")
                                        .As("key")
                                        .Select(GremlinKeyword.Pop.Last, "@v")
                                        .Optional(GraphTraversal2.__().Out("mdl"))
                                        .InE("ref")
                                        .And(GraphTraversal2.__()
                                            .Values("_ref")
                                            .Where(Predicate.eq("key"))))))
                            .Until(GraphTraversal2.__().FlatMap(
                                GraphTraversal2.__()
                                    .As("res")
                                    .Select(GremlinKeyword.Pop.Last, "@v")
                                    .Where(Predicate.eq("res"))))
                            .Select("@v")
                            .Unfold())
                    .Dedup()
                    .Project("_id", "type", "etag")
                    .By(GraphTraversal2.__().Values("__id"))
                    .By(GraphTraversal2.__().Label())
                    .By(GraphTraversal2.__().Values("__etag"))).Next();

            foreach (var result in write)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void Test4ListAllProducts()
        {
            GraphViewConnection connection = GetGraphViewConnection();
            //connection.ResetCollection();

            GraphViewCommand graph = GetGraphViewCommand(connection);
            graph.OutputFormat = OutputFormat.GraphSON;

            var results = graph.g()
                .V()
                .Has("_app", "test-app")
                .Has("__id", "test-app")
                .HasLabel("application")
                .Coalesce(
                    GraphTraversal2.__().Union(GraphTraversal2.__()
                        .Not(GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "test-app")
                            .HasLabel("application"))
                        .Constant("~0"),
                        GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "test-app")
                            .HasLabel("application")
                            .Has("_provisioningState", 0)
                            .Constant("~1"),
                        GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "test-app")
                            .HasLabel("application")
                            .Has("_provisioningState", 2)
                            .Constant("~2"),
                        GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "test-app")
                            .HasLabel("application")
                            .Has("_deleted", true)
                            .Constant("~3")),
                    GraphTraversal2.__()
                        .FlatMap(
                            GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .HasLabel("product")
                                .Range(0, 100)
                                .Union(GraphTraversal2.__().Identity().SideEffect(
                                    GraphTraversal2.__().Id().Store("^ids")),
                                    GraphTraversal2.__()
                                        .As("@v")
                                        .FlatMap(GraphTraversal2.__()
                                            .Optional(
                                                GraphTraversal2.__().Out("mdl"))
                                            .OutE("ref"))
                                        .Repeat(
                                            GraphTraversal2.__()
                                                .As("@e")
                                                .FlatMap(
                                                    GraphTraversal2
                                                        .__()
                                                        .InV()
                                                        .As("mdl")
                                                        .Select(GremlinKeyword.Pop.Last,
                                                            "@v")
                                                        .Both()
                                                        .Dedup()
                                                        .And(GraphTraversal2.__()
                                                            .Optional(
                                                                GraphTraversal2.__()
                                                                    .Out("mdl"))
                                                            .Where(Predicate.eq(
                                                                "mdl"))))
                                                .As("@v")
                                                .Optional(GraphTraversal2.__().FlatMap(
                                                    GraphTraversal2.__()
                                                        .Select(GremlinKeyword.Pop.Last,
                                                            "@e")
                                                        .Values("_ref")
                                                        .As("key")
                                                        .Select(GremlinKeyword.Pop.Last,
                                                            "@v")
                                                        .Optional(GraphTraversal2.__()
                                                            .Out("mdl"))
                                                        .OutE("ref")
                                                        .And(GraphTraversal2.__()
                                                            .Values("_key")
                                                            .Where(Predicate.eq(
                                                                "key"))))))
                                        .Until(GraphTraversal2.__().FlatMap(
                                            GraphTraversal2.__()
                                                .As("res")
                                                .Select(GremlinKeyword.Pop.Last, "@v")
                                                .Where(Predicate.eq("res"))))
                                        .SideEffect(
                                            GraphTraversal2.__()
                                                .Project("data", "info")
                                                .By(GraphTraversal2.__()
                                                    .Select("@e")
                                                    .Unfold()
                                                    .Project("key", "ref")
                                                    .By(GraphTraversal2.__().Values(
                                                        "_key"))
                                                    .By(GraphTraversal2.__().Values(
                                                        "_ref"))
                                                    .Fold())
                                                .By(GraphTraversal2.__()
                                                    .Select("@v")
                                                    .Unfold()
                                                    .Project("_id", "type", "etag")
                                                    .By(GraphTraversal2.__().Values(
                                                        "__id"))
                                                    .By(GraphTraversal2.__().Label())
                                                    .By(GraphTraversal2.__().Values(
                                                        "__etag"))
                                                    .Fold())
                                                .Store("^refs")))
                                .Dedup()
                                .Union(GraphTraversal2.__().Identity().SideEffect(
                                    GraphTraversal2.__()
                                        .Group("^mdls")
                                        .By(GraphTraversal2.__().Id())
                                        .By(GraphTraversal2.__().Coalesce(
                                            GraphTraversal2.__().Out("mdl").Values(
                                                "__id"),
                                            GraphTraversal2.__().Constant("")))),
                                    GraphTraversal2.__().Out("mdl"))
                                .Dedup())
                        .Union(GraphTraversal2.__()
                               .Emit()
                               .Repeat(GraphTraversal2.__().OutE("_val").As("_").InV())
                               .Tree(),
                            GraphTraversal2.__().Cap("^ids"),
                            GraphTraversal2.__().Cap("^mdls"),
                            GraphTraversal2.__().Cap("^refs"))
                     .Fold()).Next();
                     //.Unfold().Count()).Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void Test5GetProduct()
        {
            GraphViewConnection connection = GetGraphViewConnection();
            //connection.ResetCollection();

            GraphViewCommand graph = GetGraphViewCommand(connection);
            graph.OutputFormat = OutputFormat.GraphSON;

            var results =
                graph.g()
                    .V()
                    .Has("_app", "test-app")
                    .Has("__id", "test-app")
                    .HasLabel("application")
                    .Coalesce(
                        GraphTraversal2.__().Union(GraphTraversal2.__()
                            .Not(GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "test-app")
                                .HasLabel("application"))
                            .Constant("~0"),
                            GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "test-app")
                                .HasLabel("application")
                                .Has("_provisioningState", 0)
                                .Constant("~1"),
                            GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "test-app")
                                .HasLabel("application")
                                .Has("_provisioningState", 2)
                                .Constant("~2"),
                            GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "test-app")
                                .HasLabel("application")
                                .Has("_deleted", true)
                                .Constant("~3")),
                        GraphTraversal2.__()
                            .FlatMap(
                                GraphTraversal2.__()
                                    .V()
                                    .Has("_app", "test-app")
                                    .Has("__id", "uber-product:soda-machine:shop-3")
                                    .HasLabel("product")
                                    .Range(0, 100)
                                    .Union(
                                        GraphTraversal2.__()
                                            .Identity()
                                            .SideEffect(GraphTraversal2.__().Id().Store("^ids")),
                                        GraphTraversal2.__()
                                            .As("@v")
                                            .FlatMap(
                                                GraphTraversal2.__()
                                                    .Optional(GraphTraversal2.__().Out("mdl"))
                                                    .OutE("ref"))
                                            .Repeat(
                                                GraphTraversal2.__()
                                                    .As("@e")
                                                    .FlatMap(
                                                        GraphTraversal2
                                                            .__()
                                                            .InV()
                                                            .As("mdl")
                                                            .Select(GremlinKeyword.Pop.Last, "@v")
                                                            .Both()
                                                            .Dedup()
                                                            .And(
                                                                GraphTraversal2.__()
                                                                    .Optional(GraphTraversal2.__().Out("mdl"))
                                                                    .Where(Predicate.eq("mdl"))))
                                                    .As("@v")
                                                    .Optional(GraphTraversal2.__().FlatMap(
                                                        GraphTraversal2.__()
                                                            .Select(GremlinKeyword.Pop.Last, "@e")
                                                            .Values("_ref")
                                                            .As("key")
                                                            .Select(GremlinKeyword.Pop.Last, "@v")
                                                            .Optional(GraphTraversal2.__().Out("mdl"))
                                                            .OutE("ref")
                                                            .And(
                                                                GraphTraversal2.__()
                                                                    .Values("_key")
                                                                    .Where(Predicate.eq("key"))))))
                                            .Until(GraphTraversal2.__().FlatMap(
                                                GraphTraversal2.__()
                                                    .As("res")
                                                    .Select(GremlinKeyword.Pop.Last, "@v")
                                                    .Where(Predicate.eq("res"))))
                                            .SideEffect(
                                                GraphTraversal2.__()
                                                    .Project("data", "info")
                                                    .By(GraphTraversal2.__()
                                                        .Select("@e")
                                                        .Unfold()
                                                        .Project("key", "ref")
                                                        .By(GraphTraversal2.__().Values("_key"))
                                                        .By(GraphTraversal2.__().Values("_ref"))
                                                        .Fold())
                                                    .By(GraphTraversal2.__()
                                                        .Select("@v")
                                                        .Unfold()
                                                        .Project("_id", "type", "etag")
                                                        .By(GraphTraversal2.__().Values("__id"))
                                                        .By(GraphTraversal2.__().Label())
                                                        .By(GraphTraversal2.__().Values("__etag"))
                                                        .Fold())
                                                    .Store("^refs")))
                                    .Dedup()
                                    .Union(GraphTraversal2.__().Identity().SideEffect(
                                        GraphTraversal2.__()
                                            .Group("^mdls")
                                            .By(GraphTraversal2.__().Id())
                                            .By(GraphTraversal2.__().Coalesce(
                                                GraphTraversal2.__().Out("mdl").Values("__id"),
                                                GraphTraversal2.__().Constant("")))),
                                        GraphTraversal2.__().Out("mdl"))
                                    .Dedup())
                            .Union(GraphTraversal2.__()
                                .Emit()
                                .Repeat(GraphTraversal2.__().OutE("_val").As("_").InV())
                                .Tree(),
                                GraphTraversal2.__().Cap("^ids"),
                                GraphTraversal2.__().Cap("^mdls"),
                                GraphTraversal2.__().Cap("^refs"))
                            .Fold()).Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void Test6GetProductModel()
        {
            GraphViewConnection connection = GetGraphViewConnection();
            //connection.ResetCollection();

            GraphViewCommand graph = GetGraphViewCommand(connection);
            graph.OutputFormat = OutputFormat.GraphSON;

            var results =
                graph.g()
                    .V()
                    .Has("_app", "test-app")
                    .Has("__id", "test-app")
                    .HasLabel("application")
                    .Coalesce(
                        GraphTraversal2.__().Union(GraphTraversal2.__()
                            .Not(GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "test-app")
                                .HasLabel("application"))
                            .Constant("~0"),
                            GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "test-app")
                                .HasLabel("application")
                                .Has("_provisioningState", 0)
                                .Constant("~1"),
                            GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "test-app")
                                .HasLabel("application")
                                .Has("_provisioningState", 2)
                                .Constant("~2"),
                            GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "test-app")
                                .HasLabel("application")
                                .Has("_deleted", true)
                                .Constant("~3")),
                        GraphTraversal2.__()
                            .FlatMap(
                                GraphTraversal2.__()
                                    .V()
                                    .Has("_app", "test-app")
                                    .Has("__id", "uber-product:soda-machine")
                                    .HasLabel("product-model")
                                    .Range(0, 100)
                                    .Union(
                                        GraphTraversal2.__()
                                            .Identity()
                                            .SideEffect(GraphTraversal2.__().Id().Store("^ids")),
                                        GraphTraversal2.__()
                                            .As("@v")
                                            .FlatMap(
                                                GraphTraversal2.__()
                                                    .Optional(GraphTraversal2.__().Out("mdl"))
                                                    .OutE("ref"))
                                            .Repeat(
                                                GraphTraversal2.__()
                                                    .As("@e")
                                                    .FlatMap(
                                                        GraphTraversal2
                                                            .__()
                                                            .InV()
                                                            .As("mdl")
                                                            .Select(GremlinKeyword.Pop.Last, "@v")
                                                            .Both()
                                                            .Dedup()
                                                            .And(
                                                                GraphTraversal2.__()
                                                                    .Optional(GraphTraversal2.__().Out("mdl"))
                                                                    .Where(Predicate.eq("mdl"))))
                                                    .As("@v")
                                                    .Optional(GraphTraversal2.__().FlatMap(
                                                        GraphTraversal2.__()
                                                            .Select(GremlinKeyword.Pop.Last, "@e")
                                                            .Values("_ref")
                                                            .As("key")
                                                            .Select(GremlinKeyword.Pop.Last, "@v")
                                                            .Optional(GraphTraversal2.__().Out("mdl"))
                                                            .OutE("ref")
                                                            .And(
                                                                GraphTraversal2.__()
                                                                    .Values("_key")
                                                                    .Where(Predicate.eq("key"))))))
                                            .Until(GraphTraversal2.__().FlatMap(
                                                GraphTraversal2.__()
                                                    .As("res")
                                                    .Select(GremlinKeyword.Pop.Last, "@v")
                                                    .Where(Predicate.eq("res"))))
                                            .SideEffect(
                                                GraphTraversal2.__()
                                                    .Project("data", "info")
                                                    .By(GraphTraversal2.__()
                                                        .Select("@e")
                                                        .Unfold()
                                                        .Project("key", "ref")
                                                        .By(GraphTraversal2.__().Values("_key"))
                                                        .By(GraphTraversal2.__().Values("_ref"))
                                                        .Fold())
                                                    .By(GraphTraversal2.__()
                                                        .Select("@v")
                                                        .Unfold()
                                                        .Project("id", "type", "etag")
                                                        .By(GraphTraversal2.__().Values("__id"))
                                                        .By(GraphTraversal2.__().Label())
                                                        .By(GraphTraversal2.__().Values("__etag"))
                                                        .Fold())
                                                    .Store("^refs")))
                                    .Dedup()
                                    .Union(GraphTraversal2.__().Identity().SideEffect(
                                        GraphTraversal2.__()
                                            .Group("^mdls")
                                            .By(GraphTraversal2.__().Id())
                                            .By(GraphTraversal2.__().Coalesce(
                                                GraphTraversal2.__().Out("mdl").Values("__id"),
                                                GraphTraversal2.__().Constant("")))),
                                        GraphTraversal2.__().Out("mdl"))
                                    .Dedup())
                            .Union(GraphTraversal2.__()
                                .Emit()
                                .Repeat(GraphTraversal2.__().OutE("_val").As("_").InV())
                                .Tree(),
                                GraphTraversal2.__().Cap("^ids"),
                                GraphTraversal2.__().Cap("^mdls"),
                                GraphTraversal2.__().Cap("^refs"))
                            .Fold()).Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void Test7GetDevicesforProduct()
        {
            GraphViewConnection connection = GetGraphViewConnection();
            //connection.ResetCollection();

            GraphViewCommand graph = GetGraphViewCommand(connection);

            var results =
                graph.g()
                    .V()
                    .Has("_app", "test-app")
                    .Has("__id", "test-app")
                    .HasLabel("application")
                    .Coalesce(
                        GraphTraversal2.__().Union(
                            GraphTraversal2.__()
                                .Not(GraphTraversal2.__()
                                    .V()
                                    .Has("_app", "test-app")
                                    .Has("__id", "test-app")
                                    .HasLabel("application"))
                                .Constant("~0"),
                            GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "test-app")
                                .HasLabel("application")
                                .Has("_provisioningState", 0)
                                .Constant("~1"),
                            GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "test-app")
                                .HasLabel("application")
                                .Has("_provisioningState", 2)
                                .Constant("~2"),
                            GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "test-app")
                                .HasLabel("application")
                                .Has("_deleted", true)
                                .Constant("~3")),
                        GraphTraversal2.__()
                            .FlatMap(GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "product:soda-machine:shop-2")
                                .HasLabel("product")
                                .InE("device-product")
                                .As("_")
                                .OutV()
                                .Dedup()
                                .Range(0, 100)
                            )
                            .Tree()
                            .Fold()
                    ).Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void Test8UpdateProductModel_AddRefProperty()
        {
            GraphViewConnection connection = GetGraphViewConnection();
            //connection.ResetCollection();

            GraphViewCommand graph = GetGraphViewCommand(connection);
            graph.OutputFormat = OutputFormat.GraphSON;

            var results = graph.g().Inject(0).Coalesce(
                GraphTraversal2.__().Union(
                    GraphTraversal2.__()
                        .Not(GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "test-app")
                            .HasLabel("application"))
                        .Constant("~0"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "test-app")
                        .HasLabel("application")
                        .Has("_provisioningState", 0)
                        .Constant("~1"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "test-app")
                        .HasLabel("application")
                        .Has("_provisioningState", 2)
                        .Constant("~2"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "test-app")
                        .HasLabel("application")
                        .Has("_deleted", true)
                        .Constant("~3"),
                    GraphTraversal2.__()
                        .Not(GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "uber-product:soda-machine")
                            .HasLabel("product-model")
                            .Has("__etag", "SkYTpr1hSkCL4NkpsfNwvQ=="))
                        .Constant("~4"),
                    GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "uber-product:soda-machine")
                        .HasLabel("product-model")
                        .In("mdl")
                        .Local(GraphTraversal2.__()
                            .Union(GraphTraversal2.__().Properties().Key(),
                                GraphTraversal2.__().OutE("_val").Values("_key"))
                            .Fold())
                        .As("key")
                        .FlatMap(GraphTraversal2.__()
                            .Constant(new List<string>() { "location", "installer" })
                            .Unfold())
                        .Where(Predicate.without("key"))
                        .Dedup()
                        .Constant("~5"),
                    GraphTraversal2.__()
                        .Not(GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "product:soda-machine")
                            .HasLabel("product-model"))
                        .Constant("~6"),
                    GraphTraversal2.__()
                        .Not(GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "uber-product:soda-machine")
                            .HasLabel("product-model")
                            .Both()
                            .Has("_app", "test-app")
                            .Has("__id", "product:soda-machine")
                            .HasLabel("product-model"))
                        .Constant("~7")),
                GraphTraversal2.__()
                    .Project("#v0", "#v1")
                    .By(GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "uber-product:soda-machine")
                        .HasLabel("product-model")
                        .Property("__etag", "0Ro9MX91RYWT3ZWuot53FA==")
                        .SideEffect(GraphTraversal2.__().Union(
                            GraphTraversal2.__()
                                .SideEffect(GraphTraversal2.__()
                                    .Properties("_properties")
                                    .Drop())
                                .SideEffect(
                                    GraphTraversal2.__()
                                        .SideEffect(
                                            GraphTraversal2.__()
                                                .OutE("_val")
                                                .Has("_key", "_properties")
                                                .InV()
                                                .SideEffect(GraphTraversal2.__().Union(
                                                    GraphTraversal2.__()
                                                        .Properties()
                                                        .Drop(),
                                                    GraphTraversal2.__()
                                                        .Repeat(GraphTraversal2.__()
                                                            .Out("_val"))
                                                        .Emit()
                                                        .Barrier()
                                                        .Drop()))
                                                .Drop())
                                        .AddE("_val")
                                        .To(GraphTraversal2.__().AddV("_val").Property(
                                            "_app", "test-app"))
                                        .Property("_key", "_properties")
                                        .Property("_ary", true)
                                        .InV()
                                        .SideEffect(
                                            GraphTraversal2.__().Properties("0").Drop())
                                        .SideEffect(
                                            GraphTraversal2.__()
                                                .Coalesce(
                                                    GraphTraversal2.__()
                                                        .OutE("_val")
                                                        .Has("_key", "0")
                                                        .Has("_ary", false),
                                                    GraphTraversal2.__()
                                                        .SideEffect(
                                                            GraphTraversal2.__()
                                                                .OutE("_val")
                                                                .Has("_key", "0")
                                                                .InV()
                                                                .SideEffect(
                                                                    GraphTraversal2.__().Union(
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Properties()
                                                                            .Drop(),
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Repeat(
                                                                                GraphTraversal2
                                                                                    .__()
                                                                                    .Out(
                                                                                        "_val"))
                                                                            .Emit()
                                                                            .Barrier()
                                                                            .Drop()))
                                                                .Drop())
                                                        .AddE("_val")
                                                        .To(GraphTraversal2.__()
                                                            .AddV("_val")
                                                            .Property("_app",
                                                                "test-app"))
                                                        .Property("_key", "0")
                                                        .Property("_ary", false))
                                                .InV()
                                                .SideEffect(
                                                    GraphTraversal2.__()
                                                        .OutE("_val")
                                                        .Has("_key", "kind")
                                                        .InV()
                                                        .SideEffect(
                                                            GraphTraversal2.__().Union(
                                                                GraphTraversal2.__()
                                                                    .Properties()
                                                                    .Drop(),
                                                                GraphTraversal2.__()
                                                                    .Repeat(
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Out(
                                                                                "_val"))
                                                                    .Emit()
                                                                    .Barrier()
                                                                    .Drop()))
                                                        .Drop())
                                                .Property("kind", "property")
                                                .SideEffect(
                                                    GraphTraversal2.__()
                                                        .OutE("_val")
                                                        .Has("_key", "name")
                                                        .InV()
                                                        .SideEffect(
                                                            GraphTraversal2.__().Union(
                                                                GraphTraversal2.__()
                                                                    .Properties()
                                                                    .Drop(),
                                                                GraphTraversal2.__()
                                                                    .Repeat(
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Out(
                                                                                "_val"))
                                                                    .Emit()
                                                                    .Barrier()
                                                                    .Drop()))
                                                        .Drop())
                                                .Property("name",
                                                    "Soda machine location")
                                                .SideEffect(
                                                    GraphTraversal2.__()
                                                        .OutE("_val")
                                                        .Has("_key", "_id")
                                                        .InV()
                                                        .SideEffect(
                                                            GraphTraversal2.__().Union(
                                                                GraphTraversal2.__()
                                                                    .Properties()
                                                                    .Drop(),
                                                                GraphTraversal2.__()
                                                                    .Repeat(
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Out(
                                                                                "_val"))
                                                                    .Emit()
                                                                    .Barrier()
                                                                    .Drop()))
                                                        .Drop())
                                                .Property("_id", "location")
                                                .SideEffect(
                                                    GraphTraversal2.__()
                                                        .OutE("_val")
                                                        .Has("_key", "type")
                                                        .InV()
                                                        .SideEffect(
                                                            GraphTraversal2.__().Union(
                                                                GraphTraversal2.__()
                                                                    .Properties()
                                                                    .Drop(),
                                                                GraphTraversal2.__()
                                                                    .Repeat(
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Out(
                                                                                "_val"))
                                                                    .Emit()
                                                                    .Barrier()
                                                                    .Drop()))
                                                        .Drop())
                                                .Property("type", "string"))
                                        .SideEffect(
                                            GraphTraversal2.__().Properties("1").Drop())
                                        .SideEffect(
                                            GraphTraversal2.__()
                                                .Coalesce(
                                                    GraphTraversal2.__()
                                                        .OutE("_val")
                                                        .Has("_key", "1")
                                                        .Has("_ary", false),
                                                    GraphTraversal2.__()
                                                        .SideEffect(
                                                            GraphTraversal2.__()
                                                                .OutE("_val")
                                                                .Has("_key", "1")
                                                                .InV()
                                                                .SideEffect(
                                                                    GraphTraversal2.__().Union(
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Properties()
                                                                            .Drop(),
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Repeat(
                                                                                GraphTraversal2
                                                                                    .__()
                                                                                    .Out(
                                                                                        "_val"))
                                                                            .Emit()
                                                                            .Barrier()
                                                                            .Drop()))
                                                                .Drop())
                                                        .AddE("_val")
                                                        .To(GraphTraversal2.__()
                                                            .AddV("_val")
                                                            .Property("_app",
                                                                "test-app"))
                                                        .Property("_key", "1")
                                                        .Property("_ary", false))
                                                .InV()
                                                .SideEffect(
                                                    GraphTraversal2.__()
                                                        .OutE("_val")
                                                        .Has("_key", "kind")
                                                        .InV()
                                                        .SideEffect(
                                                            GraphTraversal2.__().Union(
                                                                GraphTraversal2.__()
                                                                    .Properties()
                                                                    .Drop(),
                                                                GraphTraversal2.__()
                                                                    .Repeat(
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Out(
                                                                                "_val"))
                                                                    .Emit()
                                                                    .Barrier()
                                                                    .Drop()))
                                                        .Drop())
                                                .Property("kind", "property")
                                                .SideEffect(
                                                    GraphTraversal2.__()
                                                        .OutE("_val")
                                                        .Has("_key", "name")
                                                        .InV()
                                                        .SideEffect(
                                                            GraphTraversal2.__().Union(
                                                                GraphTraversal2.__()
                                                                    .Properties()
                                                                    .Drop(),
                                                                GraphTraversal2.__()
                                                                    .Repeat(
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Out(
                                                                                "_val"))
                                                                    .Emit()
                                                                    .Barrier()
                                                                    .Drop()))
                                                        .Drop())
                                                .Property("name",
                                                    "Soda machine installer")
                                                .SideEffect(
                                                    GraphTraversal2.__()
                                                        .OutE("_val")
                                                        .Has("_key", "_id")
                                                        .InV()
                                                        .SideEffect(
                                                            GraphTraversal2.__().Union(
                                                                GraphTraversal2.__()
                                                                    .Properties()
                                                                    .Drop(),
                                                                GraphTraversal2.__()
                                                                    .Repeat(
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Out(
                                                                                "_val"))
                                                                    .Emit()
                                                                    .Barrier()
                                                                    .Drop()))
                                                        .Drop())
                                                .Property("_id", "installer")
                                                .SideEffect(
                                                    GraphTraversal2.__()
                                                        .OutE("_val")
                                                        .Has("_key", "type")
                                                        .InV()
                                                        .SideEffect(
                                                            GraphTraversal2.__().Union(
                                                                GraphTraversal2.__()
                                                                    .Properties()
                                                                    .Drop(),
                                                                GraphTraversal2.__()
                                                                    .Repeat(
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Out(
                                                                                "_val"))
                                                                    .Emit()
                                                                    .Barrier()
                                                                    .Drop()))
                                                        .Drop())
                                                .Property("type", "string"))
                                        .SideEffect(
                                            GraphTraversal2.__().Properties("2").Drop())
                                        .SideEffect(
                                            GraphTraversal2.__()
                                                .Coalesce(
                                                    GraphTraversal2.__()
                                                        .OutE("_val")
                                                        .Has("_key", "2")
                                                        .Has("_ary", false),
                                                    GraphTraversal2.__()
                                                        .SideEffect(
                                                            GraphTraversal2.__()
                                                                .OutE("_val")
                                                                .Has("_key", "2")
                                                                .InV()
                                                                .SideEffect(
                                                                    GraphTraversal2.__().Union(
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Properties()
                                                                            .Drop(),
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Repeat(
                                                                                GraphTraversal2
                                                                                    .__()
                                                                                    .Out(
                                                                                        "_val"))
                                                                            .Emit()
                                                                            .Barrier()
                                                                            .Drop()))
                                                                .Drop())
                                                        .AddE("_val")
                                                        .To(GraphTraversal2.__()
                                                            .AddV("_val")
                                                            .Property("_app",
                                                                "test-app"))
                                                        .Property("_key", "2")
                                                        .Property("_ary", false))
                                                .InV()
                                                .SideEffect(
                                                    GraphTraversal2.__()
                                                        .OutE("_val")
                                                        .Has("_key", "kind")
                                                        .InV()
                                                        .SideEffect(
                                                            GraphTraversal2.__().Union(
                                                                GraphTraversal2.__()
                                                                    .Properties()
                                                                    .Drop(),
                                                                GraphTraversal2.__()
                                                                    .Repeat(
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Out(
                                                                                "_val"))
                                                                    .Emit()
                                                                    .Barrier()
                                                                    .Drop()))
                                                        .Drop())
                                                .Property("kind", "reference")
                                                .SideEffect(
                                                    GraphTraversal2.__()
                                                        .OutE("_val")
                                                        .Has("_key", "name")
                                                        .InV()
                                                        .SideEffect(
                                                            GraphTraversal2.__().Union(
                                                                GraphTraversal2.__()
                                                                    .Properties()
                                                                    .Drop(),
                                                                GraphTraversal2.__()
                                                                    .Repeat(
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Out(
                                                                                "_val"))
                                                                    .Emit()
                                                                    .Barrier()
                                                                    .Drop()))
                                                        .Drop())
                                                .Property("name", "Syrup Level")
                                                .SideEffect(
                                                    GraphTraversal2.__()
                                                        .OutE("_val")
                                                        .Has("_key", "_id")
                                                        .InV()
                                                        .SideEffect(
                                                            GraphTraversal2.__().Union(
                                                                GraphTraversal2.__()
                                                                    .Properties()
                                                                    .Drop(),
                                                                GraphTraversal2.__()
                                                                    .Repeat(
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Out(
                                                                                "_val"))
                                                                    .Emit()
                                                                    .Barrier()
                                                                    .Drop()))
                                                        .Drop())
                                                .Property("_id", "syrup_level")
                                                .SideEffect(GraphTraversal2.__()
                                                    .Properties("target")
                                                    .Drop())
                                                .SideEffect(
                                                    GraphTraversal2.__()
                                                        .Coalesce(
                                                            GraphTraversal2.__()
                                                                .OutE("_val")
                                                                .Has("_key", "target")
                                                                .Has("_ary", false),
                                                            GraphTraversal2.__()
                                                                .SideEffect(
                                                                    GraphTraversal2.__()
                                                                        .OutE("_val")
                                                                        .Has("_key",
                                                                            "target")
                                                                        .InV()
                                                                        .SideEffect(GraphTraversal2.__().Union(
                                                                            GraphTraversal2
                                                                                .__()
                                                                                .Properties()
                                                                                .Drop(),
                                                                            GraphTraversal2
                                                                                .__()
                                                                                .Repeat(
                                                                                    GraphTraversal2
                                                                                        .__()
                                                                                        .Out(
                                                                                            "_val"))
                                                                                .Emit()
                                                                                .Barrier()
                                                                                .Drop()))
                                                                        .Drop())
                                                                .AddE("_val")
                                                                .To(GraphTraversal2.__()
                                                                    .AddV("_val")
                                                                    .Property(
                                                                        "_app",
                                                                        "test-app"))
                                                                .Property("_key",
                                                                    "target")
                                                                .Property("_ary",
                                                                    false))
                                                        .InV()
                                                        .SideEffect(
                                                            GraphTraversal2.__()
                                                                .OutE("_val")
                                                                .Has("_key", "_id")
                                                                .InV()
                                                                .SideEffect(
                                                                    GraphTraversal2.__().Union(
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Properties()
                                                                            .Drop(),
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Repeat(
                                                                                GraphTraversal2
                                                                                    .__()
                                                                                    .Out(
                                                                                        "_val"))
                                                                            .Emit()
                                                                            .Barrier()
                                                                            .Drop()))
                                                                .Drop())
                                                        .Property(
                                                            "_id",
                                                            "product:soda-machine")
                                                        .SideEffect(
                                                            GraphTraversal2.__()
                                                                .OutE("_val")
                                                                .Has("_key", "type")
                                                                .InV()
                                                                .SideEffect(
                                                                    GraphTraversal2.__().Union(
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Properties()
                                                                            .Drop(),
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Repeat(
                                                                                GraphTraversal2
                                                                                    .__()
                                                                                    .Out(
                                                                                        "_val"))
                                                                            .Emit()
                                                                            .Barrier()
                                                                            .Drop()))
                                                                .Drop())
                                                        .Property("type", "product")))
                                        .SideEffect(
                                            GraphTraversal2.__().Properties("3").Drop())
                                        .SideEffect(
                                            GraphTraversal2.__()
                                                .Coalesce(
                                                    GraphTraversal2.__()
                                                        .OutE("_val")
                                                        .Has("_key", "3")
                                                        .Has("_ary", false),
                                                    GraphTraversal2.__()
                                                        .SideEffect(
                                                            GraphTraversal2.__()
                                                                .OutE("_val")
                                                                .Has("_key", "3")
                                                                .InV()
                                                                .SideEffect(
                                                                    GraphTraversal2.__().Union(
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Properties()
                                                                            .Drop(),
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Repeat(
                                                                                GraphTraversal2
                                                                                    .__()
                                                                                    .Out(
                                                                                        "_val"))
                                                                            .Emit()
                                                                            .Barrier()
                                                                            .Drop()))
                                                                .Drop())
                                                        .AddE("_val")
                                                        .To(GraphTraversal2.__()
                                                            .AddV("_val")
                                                            .Property("_app",
                                                                "test-app"))
                                                        .Property("_key", "3")
                                                        .Property("_ary", false))
                                                .InV()
                                                .SideEffect(
                                                    GraphTraversal2.__()
                                                        .OutE("_val")
                                                        .Has("_key", "kind")
                                                        .InV()
                                                        .SideEffect(
                                                            GraphTraversal2.__().Union(
                                                                GraphTraversal2.__()
                                                                    .Properties()
                                                                    .Drop(),
                                                                GraphTraversal2.__()
                                                                    .Repeat(
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Out(
                                                                                "_val"))
                                                                    .Emit()
                                                                    .Barrier()
                                                                    .Drop()))
                                                        .Drop())
                                                .Property("kind", "reference")
                                                .SideEffect(
                                                    GraphTraversal2.__()
                                                        .OutE("_val")
                                                        .Has("_key", "name")
                                                        .InV()
                                                        .SideEffect(
                                                            GraphTraversal2.__().Union(
                                                                GraphTraversal2.__()
                                                                    .Properties()
                                                                    .Drop(),
                                                                GraphTraversal2.__()
                                                                    .Repeat(
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Out(
                                                                                "_val"))
                                                                    .Emit()
                                                                    .Barrier()
                                                                    .Drop()))
                                                        .Drop())
                                                .Property("name", "Ice Level")
                                                .SideEffect(
                                                    GraphTraversal2.__()
                                                        .OutE("_val")
                                                        .Has("_key", "_id")
                                                        .InV()
                                                        .SideEffect(
                                                            GraphTraversal2.__().Union(
                                                                GraphTraversal2.__()
                                                                    .Properties()
                                                                    .Drop(),
                                                                GraphTraversal2.__()
                                                                    .Repeat(
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Out(
                                                                                "_val"))
                                                                    .Emit()
                                                                    .Barrier()
                                                                    .Drop()))
                                                        .Drop())
                                                .Property("_id", "ice_level")
                                                .SideEffect(GraphTraversal2.__()
                                                    .Properties("target")
                                                    .Drop())
                                                .SideEffect(
                                                    GraphTraversal2.__()
                                                        .Coalesce(
                                                            GraphTraversal2.__()
                                                                .OutE("_val")
                                                                .Has("_key", "target")
                                                                .Has("_ary", false),
                                                            GraphTraversal2.__()
                                                                .SideEffect(
                                                                    GraphTraversal2.__()
                                                                        .OutE("_val")
                                                                        .Has("_key",
                                                                            "target")
                                                                        .InV()
                                                                        .SideEffect(GraphTraversal2.__().Union(
                                                                            GraphTraversal2
                                                                                .__()
                                                                                .Properties()
                                                                                .Drop(),
                                                                            GraphTraversal2
                                                                                .__()
                                                                                .Repeat(
                                                                                    GraphTraversal2
                                                                                        .__()
                                                                                        .Out(
                                                                                            "_val"))
                                                                                .Emit()
                                                                                .Barrier()
                                                                                .Drop()))
                                                                        .Drop())
                                                                .AddE("_val")
                                                                .To(GraphTraversal2.__()
                                                                    .AddV("_val")
                                                                    .Property(
                                                                        "_app",
                                                                        "test-app"))
                                                                .Property("_key",
                                                                    "target")
                                                                .Property("_ary",
                                                                    false))
                                                        .InV()
                                                        .SideEffect(
                                                            GraphTraversal2.__()
                                                                .OutE("_val")
                                                                .Has("_key", "_id")
                                                                .InV()
                                                                .SideEffect(
                                                                    GraphTraversal2.__().Union(
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Properties()
                                                                            .Drop(),
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Repeat(
                                                                                GraphTraversal2
                                                                                    .__()
                                                                                    .Out(
                                                                                        "_val"))
                                                                            .Emit()
                                                                            .Barrier()
                                                                            .Drop()))
                                                                .Drop())
                                                        .Property(
                                                            "_id",
                                                            "product:soda-machine")
                                                        .SideEffect(
                                                            GraphTraversal2.__()
                                                                .OutE("_val")
                                                                .Has("_key", "type")
                                                                .InV()
                                                                .SideEffect(
                                                                    GraphTraversal2.__().Union(
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Properties()
                                                                            .Drop(),
                                                                        GraphTraversal2
                                                                            .__()
                                                                            .Repeat(
                                                                                GraphTraversal2
                                                                                    .__()
                                                                                    .Out(
                                                                                        "_val"))
                                                                            .Emit()
                                                                            .Barrier()
                                                                            .Drop()))
                                                                .Drop())
                                                        .Property("type",
                                                            "product")))))))
                    .By(GraphTraversal2.__()
                        .V()
                        .Has("_app", "test-app")
                        .Has("__id", "product:soda-machine")
                        .HasLabel("product-model")
                        .Property("__etag", "iBuelvJFQuSGRQfEvvzPrA=="))
                    .As("#v")
                    .Project("#e0", "#e1")
                    .By(GraphTraversal2.__().Select("#v0").FlatMap(
                        GraphTraversal2.__()
                            .As("src")
                            .FlatMap(GraphTraversal2.__().Select("#v").Select("#v1"))
                            .As("tgt")
                            .Select("src")
                            .Coalesce(GraphTraversal2.__()
                                    .OutE("ref")
                                    .And(GraphTraversal2.__().InV().Where(
                                        Predicate.eq("tgt")))
                                    .Has("_key", "syrup_level")
                                    .Has("_ref", "syrup_level"),
                                GraphTraversal2.__()
                                    .AddE("ref")
                                    .To("tgt")
                                    .Property("_key", "syrup_level")
                                    .Property("_ref", "syrup_level"))))
                    .By(GraphTraversal2.__().Select("#v0").FlatMap(
                        GraphTraversal2.__()
                            .As("src")
                            .FlatMap(GraphTraversal2.__().Select("#v").Select("#v1"))
                            .As("tgt")
                            .Select("src")
                            .Coalesce(GraphTraversal2.__()
                                    .OutE("ref")
                                    .And(GraphTraversal2.__().InV().Where(
                                        Predicate.eq("tgt")))
                                    .Has("_key", "ice_level")
                                    .Has("_ref", "ice_level"),
                                GraphTraversal2.__()
                                    .AddE("ref")
                                    .To("tgt")
                                    .Property("_key", "ice_level")
                                    .Property("_ref", "ice_level"))))
                    .As("#e")
                    .Union(
                        GraphTraversal2.__()
                            .Select("#v")
                            .Union(GraphTraversal2.__().Select("#v0").As("#a").Constant(
                                new List<string>() { "_properties" }))
                            .As("#p"),
                        GraphTraversal2.__()
                            .Select("#e")
                            .Union(GraphTraversal2.__().Select("#e0"),
                                GraphTraversal2.__().Select("#e1"))
                            .As("#f")
                            .Union(GraphTraversal2.__().InV().As("#a").Select("#f").OutV(),
                                GraphTraversal2.__().OutV().As("#a").Select("#f").InV())
                            .Map(GraphTraversal2.__()
                                .Optional(GraphTraversal2.__().Out("mdl"))
                                .As("#m")
                                .Select("#a")
                                .Optional(GraphTraversal2.__().Out("mdl"))
                                .InE("ref")
                                .And(GraphTraversal2.__().OutV().Where(
                                    Predicate.eq("#m")))
                                .Values("_key")
                                .Fold())
                            .As("#p"))
                    .Select("#a")
                    .Union(
                        GraphTraversal2.__().Identity(),
                        GraphTraversal2.__()
                            .As("@v")
                            .FlatMap(GraphTraversal2.__()
                                .Optional(GraphTraversal2.__().Out("mdl"))
                                .InE("ref")
                                .And(GraphTraversal2.__().Values("_key").Where(
                                    Predicate.within("#p"))))
                            .Repeat(GraphTraversal2.__()
                                .As("@e")
                                .FlatMap(GraphTraversal2.__()
                                    .OutV()
                                    .As("mdl")
                                    .Select(GremlinKeyword.Pop.Last, "@v")
                                    .Both()
                                    .Dedup()
                                    .And(GraphTraversal2.__()
                                        .Optional(GraphTraversal2.__()
                                            .Out("mdl"))
                                        .Where(Predicate.eq("mdl"))))
                                .As("@v")
                                .Optional(GraphTraversal2.__().FlatMap(
                                    GraphTraversal2.__()
                                        .Select(GremlinKeyword.Pop.Last, "@e")
                                        .Values("_key")
                                        .As("key")
                                        .Select(GremlinKeyword.Pop.Last, "@v")
                                        .Optional(GraphTraversal2.__().Out("mdl"))
                                        .InE("ref")
                                        .And(GraphTraversal2.__()
                                            .Values("_ref")
                                            .Where(Predicate.eq("key"))))))
                            .Until(GraphTraversal2.__().FlatMap(
                                GraphTraversal2.__()
                                    .As("res")
                                    .Select(GremlinKeyword.Pop.Last, "@v")
                                    .Where(Predicate.eq("res"))))
                            .Select("@v")
                            .Unfold())
                    .Dedup()
                    .Project("_id", "type", "etag")
                    .By(GraphTraversal2.__().Values("__id"))
                    .By(GraphTraversal2.__().Label())
                    .By(GraphTraversal2.__().Values("__etag"))).Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }

        }

        [TestMethod]
        public void Test9LinkProducttoDevice()
        {
            GraphViewConnection connection = GetGraphViewConnection();
            //connection.ResetCollection();

            GraphViewCommand graph = GetGraphViewCommand(connection);
            graph.OutputFormat = OutputFormat.GraphSON;

            var results =
                graph.g().Inject(0).Coalesce(
                    GraphTraversal2.__().Union(
                        GraphTraversal2.__()
                            .Not(GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "test-app")
                                .HasLabel("application"))
                            .Constant("~0"),
                        GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "test-app")
                            .HasLabel("application")
                            .Has("_provisioningState", 0)
                            .Constant("~1"),
                        GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "test-app")
                            .HasLabel("application")
                            .Has("_provisioningState", 2)
                            .Constant("~2"),
                        GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "test-app")
                            .HasLabel("application")
                            .Has("_deleted", true)
                            .Constant("~3"),
                        GraphTraversal2.__()
                            .Not(GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "device:soda-mixer:shop-1")
                                .HasLabel("device"))
                            .Constant("~4"),
                        GraphTraversal2.__()
                            .Not(GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "product:soda-machine:shop-2")
                                .HasLabel("product"))
                            .Constant("~5")),
                    GraphTraversal2.__()
                        .Project("#v0", "#v1")
                        .By(GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "device:soda-mixer:shop-1")
                            .HasLabel("device")
                            .Property("__etag", "LmyeSEx1RL+cIZRKKvFPvA=="))
                        .By(GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "product:soda-machine:shop-2")
                            .HasLabel("product")
                            .Property("__etag", "2dGQ3DDwSUKY2Jv+9K9t3A=="))
                        .As("#v")
                        .Project("#e0")
                        .By(GraphTraversal2.__().Select("#v0").FlatMap(
                            GraphTraversal2.__()
                                .As("src")
                                .FlatMap(GraphTraversal2.__().Select("#v").Select("#v1"))
                                .As("tgt")
                                .Select("src")
                                .Coalesce(
                                    GraphTraversal2.__()
                                        .OutE("device-product")
                                        .And(GraphTraversal2.__().InV().Where(Predicate.eq("tgt"))),
                                    GraphTraversal2.__().AddE("device-product").To("tgt"))))
                        .As("#e")
                        .Union(
                            GraphTraversal2.__()
                                .Select("#e")
                                .Union(GraphTraversal2.__().Select("#e0"))
                                .As("#f")
                                .Union(GraphTraversal2.__().InV().As("#a").Select("#f").OutV(),
                                    GraphTraversal2.__().OutV().As("#a").Select("#f").InV())
                                .Map(GraphTraversal2.__()
                                    .Optional(GraphTraversal2.__().Out("mdl"))
                                    .As("#m")
                                    .Select("#a")
                                    .Optional(GraphTraversal2.__().Out("mdl"))
                                    .InE("ref")
                                    .And(GraphTraversal2.__().OutV().Where(Predicate.eq("#m")))
                                    .Values("_key")
                                    .Fold())
                                .As("#p"))
                        .Select("#a")
                        .Union(
                            GraphTraversal2.__().Identity(),
                            GraphTraversal2.__()
                                .As("@v")
                                .FlatMap(GraphTraversal2.__()
                                    .Optional(GraphTraversal2.__().Out("mdl"))
                                    .InE("ref")
                                    .And(GraphTraversal2.__().Values("_key").Where(Predicate.within("#p"))))
                                .Repeat(GraphTraversal2.__()
                                    .As("@e")
                                    .FlatMap(GraphTraversal2.__()
                                        .OutV()
                                        .As("mdl")
                                        .Select(GremlinKeyword.Pop.Last, "@v")
                                        .Both()
                                        .Dedup()
                                        .And(GraphTraversal2.__()
                                            .Optional(GraphTraversal2.__().Out("mdl"))
                                            .Where(Predicate.eq("mdl"))))
                                    .As("@v")
                                    .Optional(GraphTraversal2.__().FlatMap(
                                        GraphTraversal2.__()
                                            .Select(GremlinKeyword.Pop.Last, "@e")
                                            .Values("_key")
                                            .As("key")
                                            .Select(GremlinKeyword.Pop.Last, "@v")
                                            .Optional(GraphTraversal2.__().Out("mdl"))
                                            .InE("ref")
                                            .And(GraphTraversal2.__().Values("_ref").Where(Predicate.eq("key"))))))
                                .Until(GraphTraversal2.__().FlatMap(
                                    GraphTraversal2.__()
                                        .As("res")
                                        .Select(GremlinKeyword.Pop.Last, "@v")
                                        .Where(Predicate.eq("res"))))
                                .Select("@v")
                                .Unfold())
                        .Dedup()
                        .Project("id", "type", "etag")
                        .By(GraphTraversal2.__().Values("__id"))
                        .By(GraphTraversal2.__().Label())
                        .By(GraphTraversal2.__().Values("__etag"))).Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void Test10UpdateProductProperty()
        {
            GraphViewConnection connection = GetGraphViewConnection();
            //connection.ResetCollection();

            GraphViewCommand graph = GetGraphViewCommand(connection);
            graph.OutputFormat = OutputFormat.GraphSON;

            var results =
                graph.g().Inject(0).Coalesce(
                    GraphTraversal2.__().Union(
                        GraphTraversal2.__()
                            .Not(GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "test-app")
                                .HasLabel("application"))
                            .Constant("~0"),
                        GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "test-app")
                            .HasLabel("application")
                            .Has("_provisioningState", 0)
                            .Constant("~1"),
                        GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "test-app")
                            .HasLabel("application")
                            .Has("_provisioningState", 2)
                            .Constant("~2"),
                        GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "test-app")
                            .HasLabel("application")
                            .Has("_deleted", true)
                            .Constant("~3"),
                        GraphTraversal2.__()
                            .Not(GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "uber-product:soda-machine:shop-3")
                                .HasLabel("product")
                                .Has("__etag", "TMaJk/CGRyurJIle/FncMA=="))
                            .Constant("~4"),
                        GraphTraversal2.__()
                            .Not(GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "uber-product:soda-machine")
                                .HasLabel("product-model")
                                .Has("__etag", "0Ro9MX91RYWT3ZWuot53FA=="))
                            .Constant("~5"),
                        GraphTraversal2.__()
                            .Not(GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "device:cola-mixer:shop-3.2")
                                .HasLabel("device")
                                .Has("__etag", "G1lCXUnhRSCqohWUaZza8w=="))
                            .Constant("~6"),
                        GraphTraversal2.__()
                            .Not(GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "device:kool-aid-mixer:shop-3.2")
                                .HasLabel("device")
                                .Has("__etag", "E5h6wBBpRjuDWkVaJ/Ud+Q=="))
                            .Constant("~7"),
                        GraphTraversal2.__()
                            .Not(GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "device:soda-mixer:shop-3.1")
                                .HasLabel("device")
                                .Has("__etag", "yOXsJu84SJW6Amtm9FF9ug=="))
                            .Constant("~8"),
                        GraphTraversal2.__()
                            .Not(GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "device:ice-machine:shop-3.2")
                                .HasLabel("device")
                                .Has("__etag", "XTb4lY83SLes2c+gZZ6vfA=="))
                            .Constant("~9"),
                        GraphTraversal2.__()
                            .Not(GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "device:ice-machine:shop-3.1")
                                .HasLabel("device")
                                .Has("__etag", "cWI7zlmBSNei70b7zoqghw=="))
                            .Constant("~10")),
                    GraphTraversal2.__()
                        .Project("#v0")
                        .By(GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "uber-product:soda-machine:shop-3")
                            .HasLabel("product")
                            .Property("__etag", "lunRO6wJQg6WMNq/CGr7QA==")
                            .SideEffect(GraphTraversal2.__().Union(
                                GraphTraversal2.__()
                                    .SideEffect(
                                        GraphTraversal2.__()
                                            .OutE("_val")
                                            .Has("_key", "name")
                                            .InV()
                                            .SideEffect(GraphTraversal2.__().Union(
                                                GraphTraversal2.__().Properties().Drop(),
                                                GraphTraversal2.__()
                                                    .Repeat(
                                                        GraphTraversal2.__().Out("_val"))
                                                    .Emit()
                                                    .Barrier()
                                                    .Drop()))
                                            .Drop())
                                    .Property("name", "Uber Soda Machine #3 - New Name"))))
                        .As("#v")
                        .Union(GraphTraversal2.__()
                            .Select("#v")
                            .Union(GraphTraversal2.__().Select("#v0").As("#a").Constant(
                                new List<string>() {"name"}))
                            .As("#p"))
                        .Select("#a")
                        .Union(
                            GraphTraversal2.__().Identity(),
                            GraphTraversal2.__()
                                .As("@v")
                                .FlatMap(GraphTraversal2.__()
                                    .Optional(GraphTraversal2.__().Out("mdl"))
                                    .InE("ref")
                                    .And(GraphTraversal2.__().Values("_key").Where(
                                        Predicate.within("#p"))))
                                .Repeat(GraphTraversal2.__()
                                    .As("@e")
                                    .FlatMap(GraphTraversal2.__()
                                        .OutV()
                                        .As("mdl")
                                        .Select(GremlinKeyword.Pop.Last, "@v")
                                        .Both()
                                        .Dedup()
                                        .And(GraphTraversal2.__()
                                            .Optional(GraphTraversal2.__()
                                                .Out("mdl"))
                                            .Where(Predicate.eq("mdl"))))
                                    .As("@v")
                                    .Optional(GraphTraversal2.__().FlatMap(
                                        GraphTraversal2.__()
                                            .Select(GremlinKeyword.Pop.Last, "@e")
                                            .Values("_key")
                                            .As("key")
                                            .Select(GremlinKeyword.Pop.Last, "@v")
                                            .Optional(GraphTraversal2.__().Out("mdl"))
                                            .InE("ref")
                                            .And(GraphTraversal2.__()
                                                .Values("_ref")
                                                .Where(Predicate.eq("key"))))))
                                .Until(GraphTraversal2.__().FlatMap(
                                    GraphTraversal2.__()
                                        .As("res")
                                        .Select(GremlinKeyword.Pop.Last, "@v")
                                        .Where(Predicate.eq("res"))))
                                .Select("@v")
                                .Unfold())
                        .Dedup()
                        .Project("id", "type", "etag")
                        .By(GraphTraversal2.__().Values("__id"))
                        .By(GraphTraversal2.__().Label())
                        .By(GraphTraversal2.__().Values("__etag"))).Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void Test11UpdateProductrefProperty()
        {
            GraphViewConnection connection = GetGraphViewConnection();
            //connection.ResetCollection();

            GraphViewCommand graph = GetGraphViewCommand(connection);
            graph.OutputFormat = OutputFormat.GraphSON;

            var results =
                graph.g().Inject(0).Coalesce(
                    GraphTraversal2.__().Union(
                        GraphTraversal2.__()
                            .Not(GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "test-app")
                                .HasLabel("application"))
                            .Constant("~0"),
                        GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "test-app")
                            .HasLabel("application")
                            .Has("_provisioningState", 0)
                            .Constant("~1"),
                        GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "test-app")
                            .HasLabel("application")
                            .Has("_provisioningState", 2)
                            .Constant("~2"),
                        GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "test-app")
                            .HasLabel("application")
                            .Has("_deleted", true)
                            .Constant("~3"),
                        GraphTraversal2.__()
                            .Not(GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "uber-product:soda-machine:shop-3")
                                .HasLabel("product")
                                .Has("__etag", "lunRO6wJQg6WMNq/CGr7QA=="))
                            .Constant("~4"),
                        GraphTraversal2.__()
                            .Not(GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "uber-product:soda-machine")
                                .HasLabel("product-model")
                                .Has("__etag", "0Ro9MX91RYWT3ZWuot53FA=="))
                            .Constant("~5"),
                        GraphTraversal2.__()
                            .Not(GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "device:cola-mixer:shop-3.2")
                                .HasLabel("device")
                                .Has("__etag", "G1lCXUnhRSCqohWUaZza8w=="))
                            .Constant("~6"),
                        GraphTraversal2.__()
                            .Not(GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "device:kool-aid-mixer:shop-3.2")
                                .HasLabel("device")
                                .Has("__etag", "E5h6wBBpRjuDWkVaJ/Ud+Q=="))
                            .Constant("~7"),
                        GraphTraversal2.__()
                            .Not(GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "device:soda-mixer:shop-3.1")
                                .HasLabel("device")
                                .Has("__etag", "yOXsJu84SJW6Amtm9FF9ug=="))
                            .Constant("~8"),
                        GraphTraversal2.__()
                            .Not(GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "device:ice-machine:shop-3.2")
                                .HasLabel("device")
                                .Has("__etag", "XTb4lY83SLes2c+gZZ6vfA=="))
                            .Constant("~9"),
                        GraphTraversal2.__()
                            .Not(GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "device:ice-machine:shop-3.1")
                                .HasLabel("device")
                                .Has("__etag", "cWI7zlmBSNei70b7zoqghw=="))
                            .Constant("~10"),
                        GraphTraversal2.__()
                            .Not(GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "device:soda-mixer")
                                .HasLabel("device-model")
                                .Has("__etag", "lsRrd7JWSBqW9kiBVPS7aQ=="))
                            .Constant("~11")),
                    GraphTraversal2.__()
                        .Project("#v0", "#v1", "#v2", "#v3")
                        .By(GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "uber-product:soda-machine:shop-3")
                            .HasLabel("product")
                            .Property("__etag", "yzm2GRluTOim/fvMmuxh2g=="))
                        .By(GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "device:cola-mixer:shop-3.2")
                            .HasLabel("device")
                            .Property("__etag", "aj+sec3TRnCF1mwWDErzqA==")
                            .SideEffect(GraphTraversal2.__().Union(
                                GraphTraversal2.__()
                                    .SideEffect(
                                        GraphTraversal2.__().Properties("_twin").Drop())
                                    .SideEffect(
                                        GraphTraversal2.__()
                                            .Coalesce(
                                                GraphTraversal2.__()
                                                    .OutE("_val")
                                                    .Has("_key", "_twin")
                                                    .Has("_ary", false),
                                                GraphTraversal2.__()
                                                    .SideEffect(
                                                        GraphTraversal2.__()
                                                            .OutE("_val")
                                                            .Has("_key", "_twin")
                                                            .InV()
                                                            .SideEffect(
                                                                GraphTraversal2.__().Union(
                                                                    GraphTraversal2.__()
                                                                        .Properties()
                                                                        .Drop(),
                                                                    GraphTraversal2.__()
                                                                        .Repeat(
                                                                            GraphTraversal2
                                                                                .__()
                                                                                .Out(
                                                                                    "_val"))
                                                                        .Emit()
                                                                        .Barrier()
                                                                        .Drop()))
                                                            .Drop())
                                                    .AddE("_val")
                                                    .To(GraphTraversal2.__()
                                                        .AddV("_val")
                                                        .Property("_app", "test-app"))
                                                    .Property("_key", "_twin")
                                                    .Property("_ary", false))
                                            .InV()
                                            .SideEffect(GraphTraversal2.__()
                                                .Properties("reported")
                                                .Drop())
                                            .SideEffect(
                                                GraphTraversal2.__()
                                                    .Coalesce(
                                                        GraphTraversal2.__()
                                                            .OutE("_val")
                                                            .Has("_key", "reported")
                                                            .Has("_ary", false),
                                                        GraphTraversal2.__()
                                                            .SideEffect(
                                                                GraphTraversal2.__()
                                                                    .OutE("_val")
                                                                    .Has("_key", "reported")
                                                                    .InV()
                                                                    .SideEffect(
                                                                        GraphTraversal2.__().Union(
                                                                            GraphTraversal2
                                                                                .__()
                                                                                .Properties()
                                                                                .Drop(),
                                                                            GraphTraversal2
                                                                                .__()
                                                                                .Repeat(
                                                                                    GraphTraversal2
                                                                                        .__()
                                                                                        .Out(
                                                                                            "_val"))
                                                                                .Emit()
                                                                                .Barrier()
                                                                                .Drop()))
                                                                    .Drop())
                                                            .AddE("_val")
                                                            .To(GraphTraversal2.__()
                                                                .AddV("_val")
                                                                .Property("_app",
                                                                    "test-app"))
                                                            .Property("_key", "reported")
                                                            .Property("_ary", false))
                                                    .InV()
                                                    .SideEffect(
                                                        GraphTraversal2.__()
                                                            .OutE("_val")
                                                            .Has("_key", "syrup_level")
                                                            .InV()
                                                            .SideEffect(
                                                                GraphTraversal2.__().Union(
                                                                    GraphTraversal2.__()
                                                                        .Properties()
                                                                        .Drop(),
                                                                    GraphTraversal2.__()
                                                                        .Repeat(
                                                                            GraphTraversal2
                                                                                .__()
                                                                                .Out(
                                                                                    "_val"))
                                                                        .Emit()
                                                                        .Barrier()
                                                                        .Drop()))
                                                            .Drop())
                                                    .Property("syrup_level", 2.3))),
                                GraphTraversal2.__()
                                    .SideEffect(
                                        GraphTraversal2.__()
                                            .OutE("_val")
                                            .Has("_key", "co2_level")
                                            .InV()
                                            .SideEffect(GraphTraversal2.__().Union(
                                                GraphTraversal2.__().Properties().Drop(),
                                                GraphTraversal2.__()
                                                    .Repeat(
                                                        GraphTraversal2.__().Out("_val"))
                                                    .Emit()
                                                    .Barrier()
                                                    .Drop()))
                                            .Drop())
                                    .Property("co2_level", 0.6),
                                GraphTraversal2.__()
                                    .SideEffect(
                                        GraphTraversal2.__()
                                            .OutE("_val")
                                            .Has("_key", "name")
                                            .InV()
                                            .SideEffect(GraphTraversal2.__().Union(
                                                GraphTraversal2.__().Properties().Drop(),
                                                GraphTraversal2.__()
                                                    .Repeat(
                                                        GraphTraversal2.__().Out("_val"))
                                                    .Emit()
                                                    .Barrier()
                                                    .Drop()))
                                            .Drop())
                                    .Property("name", "Cola Mixer #111222"),
                                GraphTraversal2.__()
                                    .SideEffect(
                                        GraphTraversal2.__()
                                            .OutE("_val")
                                            .Has("_key", "serial_number")
                                            .InV()
                                            .SideEffect(GraphTraversal2.__().Union(
                                                GraphTraversal2.__().Properties().Drop(),
                                                GraphTraversal2.__()
                                                    .Repeat(
                                                        GraphTraversal2.__().Out("_val"))
                                                    .Emit()
                                                    .Barrier()
                                                    .Drop()))
                                            .Drop())
                                    .Property("serial_number", "4444-111222"),
                                GraphTraversal2.__()
                                    .SideEffect(
                                        GraphTraversal2.__()
                                            .OutE("_val")
                                            .Has("_key", "firmware_version")
                                            .InV()
                                            .SideEffect(GraphTraversal2.__().Union(
                                                GraphTraversal2.__().Properties().Drop(),
                                                GraphTraversal2.__()
                                                    .Repeat(
                                                        GraphTraversal2.__().Out("_val"))
                                                    .Emit()
                                                    .Barrier()
                                                    .Drop()))
                                            .Drop())
                                    .Property("firmware_version", "1.0.0"),
                                GraphTraversal2.__()
                                    .SideEffect(GraphTraversal2.__().OutE("mdl").Drop())
                                    .AddE("mdl")
                                    .To(GraphTraversal2.__()
                                        .V()
                                        .Has("_app", "test-app")
                                        .Has("__id", "device:soda-mixer")
                                        .HasLabel("device-model")))))
                        .By(GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "device:kool-aid-mixer:shop-3.2")
                            .HasLabel("device")
                            .Property("__etag", "k0maOZ1/QF+d9fn7WR8YWQ==")
                            .SideEffect(GraphTraversal2.__().Union(
                                GraphTraversal2.__()
                                    .SideEffect(
                                        GraphTraversal2.__().Properties("_twin").Drop())
                                    .SideEffect(
                                        GraphTraversal2.__()
                                            .Coalesce(
                                                GraphTraversal2.__()
                                                    .OutE("_val")
                                                    .Has("_key", "_twin")
                                                    .Has("_ary", false),
                                                GraphTraversal2.__()
                                                    .SideEffect(
                                                        GraphTraversal2.__()
                                                            .OutE("_val")
                                                            .Has("_key", "_twin")
                                                            .InV()
                                                            .SideEffect(
                                                                GraphTraversal2.__().Union(
                                                                    GraphTraversal2.__()
                                                                        .Properties()
                                                                        .Drop(),
                                                                    GraphTraversal2.__()
                                                                        .Repeat(
                                                                            GraphTraversal2
                                                                                .__()
                                                                                .Out(
                                                                                    "_val"))
                                                                        .Emit()
                                                                        .Barrier()
                                                                        .Drop()))
                                                            .Drop())
                                                    .AddE("_val")
                                                    .To(GraphTraversal2.__()
                                                        .AddV("_val")
                                                        .Property("_app", "test-app"))
                                                    .Property("_key", "_twin")
                                                    .Property("_ary", false))
                                            .InV()
                                            .SideEffect(GraphTraversal2.__()
                                                .Properties("reported")
                                                .Drop())
                                            .SideEffect(
                                                GraphTraversal2.__()
                                                    .Coalesce(
                                                        GraphTraversal2.__()
                                                            .OutE("_val")
                                                            .Has("_key", "reported")
                                                            .Has("_ary", false),
                                                        GraphTraversal2.__()
                                                            .SideEffect(
                                                                GraphTraversal2.__()
                                                                    .OutE("_val")
                                                                    .Has("_key", "reported")
                                                                    .InV()
                                                                    .SideEffect(
                                                                        GraphTraversal2.__().Union(
                                                                            GraphTraversal2
                                                                                .__()
                                                                                .Properties()
                                                                                .Drop(),
                                                                            GraphTraversal2
                                                                                .__()
                                                                                .Repeat(
                                                                                    GraphTraversal2
                                                                                        .__()
                                                                                        .Out(
                                                                                            "_val"))
                                                                                .Emit()
                                                                                .Barrier()
                                                                                .Drop()))
                                                                    .Drop())
                                                            .AddE("_val")
                                                            .To(GraphTraversal2.__()
                                                                .AddV("_val")
                                                                .Property("_app",
                                                                    "test-app"))
                                                            .Property("_key", "reported")
                                                            .Property("_ary", false))
                                                    .InV()
                                                    .SideEffect(
                                                        GraphTraversal2.__()
                                                            .OutE("_val")
                                                            .Has("_key", "syrup_level")
                                                            .InV()
                                                            .SideEffect(
                                                                GraphTraversal2.__().Union(
                                                                    GraphTraversal2.__()
                                                                        .Properties()
                                                                        .Drop(),
                                                                    GraphTraversal2.__()
                                                                        .Repeat(
                                                                            GraphTraversal2
                                                                                .__()
                                                                                .Out(
                                                                                    "_val"))
                                                                        .Emit()
                                                                        .Barrier()
                                                                        .Drop()))
                                                            .Drop())
                                                    .Property("syrup_level", 2.3))),
                                GraphTraversal2.__()
                                    .SideEffect(
                                        GraphTraversal2.__()
                                            .OutE("_val")
                                            .Has("_key", "co2_level")
                                            .InV()
                                            .SideEffect(GraphTraversal2.__().Union(
                                                GraphTraversal2.__().Properties().Drop(),
                                                GraphTraversal2.__()
                                                    .Repeat(
                                                        GraphTraversal2.__().Out("_val"))
                                                    .Emit()
                                                    .Barrier()
                                                    .Drop()))
                                            .Drop())
                                    .Property("co2_level", 0.7),
                                GraphTraversal2.__()
                                    .SideEffect(
                                        GraphTraversal2.__()
                                            .OutE("_val")
                                            .Has("_key", "name")
                                            .InV()
                                            .SideEffect(GraphTraversal2.__().Union(
                                                GraphTraversal2.__().Properties().Drop(),
                                                GraphTraversal2.__()
                                                    .Repeat(
                                                        GraphTraversal2.__().Out("_val"))
                                                    .Emit()
                                                    .Barrier()
                                                    .Drop()))
                                            .Drop())
                                    .Property("name", "Kool Aid Mixer #999888"),
                                GraphTraversal2.__()
                                    .SideEffect(
                                        GraphTraversal2.__()
                                            .OutE("_val")
                                            .Has("_key", "serial_number")
                                            .InV()
                                            .SideEffect(GraphTraversal2.__().Union(
                                                GraphTraversal2.__().Properties().Drop(),
                                                GraphTraversal2.__()
                                                    .Repeat(
                                                        GraphTraversal2.__().Out("_val"))
                                                    .Emit()
                                                    .Barrier()
                                                    .Drop()))
                                            .Drop())
                                    .Property("serial_number", "4444-999888"),
                                GraphTraversal2.__()
                                    .SideEffect(
                                        GraphTraversal2.__()
                                            .OutE("_val")
                                            .Has("_key", "firmware_version")
                                            .InV()
                                            .SideEffect(GraphTraversal2.__().Union(
                                                GraphTraversal2.__().Properties().Drop(),
                                                GraphTraversal2.__()
                                                    .Repeat(
                                                        GraphTraversal2.__().Out("_val"))
                                                    .Emit()
                                                    .Barrier()
                                                    .Drop()))
                                            .Drop())
                                    .Property("firmware_version", "1.0.2"),
                                GraphTraversal2.__()
                                    .SideEffect(GraphTraversal2.__().OutE("mdl").Drop())
                                    .AddE("mdl")
                                    .To(GraphTraversal2.__()
                                        .V()
                                        .Has("_app", "test-app")
                                        .Has("__id", "device:soda-mixer")
                                        .HasLabel("device-model")))))
                        .By(GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "device:soda-mixer:shop-3.1")
                            .HasLabel("device")
                            .Property("__etag", "OP8/P5nKSUyWscCtNgGstw==")
                            .SideEffect(GraphTraversal2.__().Union(
                                GraphTraversal2.__()
                                    .SideEffect(
                                        GraphTraversal2.__().Properties("_twin").Drop())
                                    .SideEffect(
                                        GraphTraversal2.__()
                                            .Coalesce(
                                                GraphTraversal2.__()
                                                    .OutE("_val")
                                                    .Has("_key", "_twin")
                                                    .Has("_ary", false),
                                                GraphTraversal2.__()
                                                    .SideEffect(
                                                        GraphTraversal2.__()
                                                            .OutE("_val")
                                                            .Has("_key", "_twin")
                                                            .InV()
                                                            .SideEffect(
                                                                GraphTraversal2.__().Union(
                                                                    GraphTraversal2.__()
                                                                        .Properties()
                                                                        .Drop(),
                                                                    GraphTraversal2.__()
                                                                        .Repeat(
                                                                            GraphTraversal2
                                                                                .__()
                                                                                .Out(
                                                                                    "_val"))
                                                                        .Emit()
                                                                        .Barrier()
                                                                        .Drop()))
                                                            .Drop())
                                                    .AddE("_val")
                                                    .To(GraphTraversal2.__()
                                                        .AddV("_val")
                                                        .Property("_app", "test-app"))
                                                    .Property("_key", "_twin")
                                                    .Property("_ary", false))
                                            .InV()
                                            .SideEffect(GraphTraversal2.__()
                                                .Properties("reported")
                                                .Drop())
                                            .SideEffect(
                                                GraphTraversal2.__()
                                                    .Coalesce(
                                                        GraphTraversal2.__()
                                                            .OutE("_val")
                                                            .Has("_key", "reported")
                                                            .Has("_ary", false),
                                                        GraphTraversal2.__()
                                                            .SideEffect(
                                                                GraphTraversal2.__()
                                                                    .OutE("_val")
                                                                    .Has("_key", "reported")
                                                                    .InV()
                                                                    .SideEffect(
                                                                        GraphTraversal2.__().Union(
                                                                            GraphTraversal2
                                                                                .__()
                                                                                .Properties()
                                                                                .Drop(),
                                                                            GraphTraversal2
                                                                                .__()
                                                                                .Repeat(
                                                                                    GraphTraversal2
                                                                                        .__()
                                                                                        .Out(
                                                                                            "_val"))
                                                                                .Emit()
                                                                                .Barrier()
                                                                                .Drop()))
                                                                    .Drop())
                                                            .AddE("_val")
                                                            .To(GraphTraversal2.__()
                                                                .AddV("_val")
                                                                .Property("_app",
                                                                    "test-app"))
                                                            .Property("_key", "reported")
                                                            .Property("_ary", false))
                                                    .InV()
                                                    .SideEffect(
                                                        GraphTraversal2.__()
                                                            .OutE("_val")
                                                            .Has("_key", "syrup_level")
                                                            .InV()
                                                            .SideEffect(
                                                                GraphTraversal2.__().Union(
                                                                    GraphTraversal2.__()
                                                                        .Properties()
                                                                        .Drop(),
                                                                    GraphTraversal2.__()
                                                                        .Repeat(
                                                                            GraphTraversal2
                                                                                .__()
                                                                                .Out(
                                                                                    "_val"))
                                                                        .Emit()
                                                                        .Barrier()
                                                                        .Drop()))
                                                            .Drop())
                                                    .Property("syrup_level", 2.3))),
                                GraphTraversal2.__()
                                    .SideEffect(
                                        GraphTraversal2.__()
                                            .OutE("_val")
                                            .Has("_key", "co2_level")
                                            .InV()
                                            .SideEffect(GraphTraversal2.__().Union(
                                                GraphTraversal2.__().Properties().Drop(),
                                                GraphTraversal2.__()
                                                    .Repeat(
                                                        GraphTraversal2.__().Out("_val"))
                                                    .Emit()
                                                    .Barrier()
                                                    .Drop()))
                                            .Drop())
                                    .Property("co2_level", 0.5),
                                GraphTraversal2.__()
                                    .SideEffect(
                                        GraphTraversal2.__()
                                            .OutE("_val")
                                            .Has("_key", "name")
                                            .InV()
                                            .SideEffect(GraphTraversal2.__().Union(
                                                GraphTraversal2.__().Properties().Drop(),
                                                GraphTraversal2.__()
                                                    .Repeat(
                                                        GraphTraversal2.__().Out("_val"))
                                                    .Emit()
                                                    .Barrier()
                                                    .Drop()))
                                            .Drop())
                                    .Property("name", "Soda Mixer #987456"),
                                GraphTraversal2.__()
                                    .SideEffect(
                                        GraphTraversal2.__()
                                            .OutE("_val")
                                            .Has("_key", "serial_number")
                                            .InV()
                                            .SideEffect(GraphTraversal2.__().Union(
                                                GraphTraversal2.__().Properties().Drop(),
                                                GraphTraversal2.__()
                                                    .Repeat(
                                                        GraphTraversal2.__().Out("_val"))
                                                    .Emit()
                                                    .Barrier()
                                                    .Drop()))
                                            .Drop())
                                    .Property("serial_number", "4444-987456"),
                                GraphTraversal2.__()
                                    .SideEffect(
                                        GraphTraversal2.__()
                                            .OutE("_val")
                                            .Has("_key", "firmware_version")
                                            .InV()
                                            .SideEffect(GraphTraversal2.__().Union(
                                                GraphTraversal2.__().Properties().Drop(),
                                                GraphTraversal2.__()
                                                    .Repeat(
                                                        GraphTraversal2.__().Out("_val"))
                                                    .Emit()
                                                    .Barrier()
                                                    .Drop()))
                                            .Drop())
                                    .Property("firmware_version", "1.1.2"),
                                GraphTraversal2.__()
                                    .SideEffect(GraphTraversal2.__().OutE("mdl").Drop())
                                    .AddE("mdl")
                                    .To(GraphTraversal2.__()
                                        .V()
                                        .Has("_app", "test-app")
                                        .Has("__id", "device:soda-mixer")
                                        .HasLabel("device-model")))))
                        .As("#v")
                        .Union(GraphTraversal2.__()
                            .Select("#v")
                            .Union(GraphTraversal2.__().Select("#v0").As("#a").Constant(
                                new List<string>() {"syrup_level"}),
                                GraphTraversal2.__().Select("#v1").As("#a").Constant(
                                    new List<string>()
                                    {
                                        "syrup_level",
                                        "co2_level",
                                        "name",
                                        "serial_number",
                                        "firmware_version"
                                    }),
                                GraphTraversal2.__().Select("#v2").As("#a").Constant(
                                    new List<string>()
                                    {
                                        "syrup_level",
                                        "co2_level",
                                        "name",
                                        "serial_number",
                                        "firmware_version"
                                    }),
                                GraphTraversal2.__().Select("#v3").As("#a").Constant(
                                    new List<string>()
                                    {
                                        "syrup_level",
                                        "co2_level",
                                        "name",
                                        "serial_number",
                                        "firmware_version"
                                    }))
                            .As("#p"))
                        .Select("#a")
                        .Union(
                            GraphTraversal2.__().Identity(),
                            GraphTraversal2.__()
                                .As("@v")
                                .FlatMap(GraphTraversal2.__()
                                    .Optional(GraphTraversal2.__().Out("mdl"))
                                    .InE("ref")
                                    .And(GraphTraversal2.__().Values("_key").Where(
                                        Predicate.within("#p"))))
                                .Repeat(GraphTraversal2.__()
                                    .As("@e")
                                    .FlatMap(GraphTraversal2.__()
                                        .OutV()
                                        .As("mdl")
                                        .Select(GremlinKeyword.Pop.Last, "@v")
                                        .Both()
                                        .Dedup()
                                        .And(GraphTraversal2.__()
                                            .Optional(GraphTraversal2.__()
                                                .Out("mdl"))
                                            .Where(Predicate.eq("mdl"))))
                                    .As("@v")
                                    .Optional(GraphTraversal2.__().FlatMap(
                                        GraphTraversal2.__()
                                            .Select(GremlinKeyword.Pop.Last, "@e")
                                            .Values("_key")
                                            .As("key")
                                            .Select(GremlinKeyword.Pop.Last, "@v")
                                            .Optional(GraphTraversal2.__().Out("mdl"))
                                            .InE("ref")
                                            .And(GraphTraversal2.__()
                                                .Values("_ref")
                                                .Where(Predicate.eq("key"))))))
                                .Until(GraphTraversal2.__().FlatMap(
                                    GraphTraversal2.__()
                                        .As("res")
                                        .Select(GremlinKeyword.Pop.Last, "@v")
                                        .Where(Predicate.eq("res"))))
                                .Select("@v")
                                .Unfold())
                        .Dedup()
                        .Project("id", "type", "etag")
                        .By(GraphTraversal2.__().Values("__id"))
                        .By(GraphTraversal2.__().Label())
                        .By(GraphTraversal2.__().Values("__etag"))).Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void Test12DeleteProductProperty_validation_error()
        {
            GraphViewConnection connection = GetGraphViewConnection();
            GraphViewCommand graph = GetGraphViewCommand(connection);
            graph.OutputFormat = OutputFormat.GraphSON;

            //connection.ResetCollection();

            var pre_fetch =
                graph.g()
                    .V()
                    .Has("_app", "test-app")
                    .Has("__id", "test-app")
                    .HasLabel("application")
                    .Coalesce(
                        GraphTraversal2.__().Union(GraphTraversal2.__()
                            .Not(GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "test-app")
                                .HasLabel("application"))
                            .Constant("~0"),
                            GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "test-app")
                                .HasLabel("application")
                                .Has("_provisioningState", 0)
                                .Constant("~1"),
                            GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "test-app")
                                .HasLabel("application")
                                .Has("_provisioningState", 2)
                                .Constant("~2"),
                            GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "test-app")
                                .HasLabel("application")
                                .Has("_deleted", true)
                                .Constant("~3")),
                        GraphTraversal2.__()
                            .FlatMap(
                                GraphTraversal2.__()
                                    .Project("nodes", "edges")
                                    .By(GraphTraversal2.__()
                                        .Union(GraphTraversal2.__()
                                            .V()
                                            .Has("_app", "test-app")
                                            .Has("__id",
                                                "uber-product:soda-machine:shop-3")
                                            .HasLabel("product"))
                                        .Fold())
                                    .By(GraphTraversal2.__().Union().Fold())
                                    .SideEffect(
                                        GraphTraversal2.__()
                                            .Select("edges")
                                            .Unfold()
                                            .Project("name", "source", "target", "properties")
                                            .By(GraphTraversal2.__().Label())
                                            .By(GraphTraversal2.__()
                                                .OutV()
                                                .Project("id", "type", "etag")
                                                .By(GraphTraversal2.__().Values("__id"))
                                                .By(GraphTraversal2.__().Label())
                                                .By(GraphTraversal2.__().Values("__etag")))
                                            .By(GraphTraversal2.__()
                                                .InV()
                                                .Project("id", "type", "etag")
                                                .By(GraphTraversal2.__().Values("__id"))
                                                .By(GraphTraversal2.__().Label())
                                                .By(GraphTraversal2.__().Values("__etag")))
                                            .By(GraphTraversal2.__()
                                                .Properties()
                                                .Group()
                                                .By(GraphTraversal2.__().Key())
                                                .By(GraphTraversal2.__().Value()))
                                            .Store("^edges"))
                                    .Select("nodes")
                                    .Unfold()
                                    .Union(GraphTraversal2.__().Identity().SideEffect(
                                        GraphTraversal2.__().Id().Store("^ids")),
                                        GraphTraversal2.__()
                                            .As("@v")
                                            .FlatMap(GraphTraversal2.__()
                                                .Optional(
                                                    GraphTraversal2.__().Out("mdl"))
                                                .OutE("ref"))
                                            .Repeat(
                                                GraphTraversal2.__()
                                                    .As("@e")
                                                    .FlatMap(
                                                        GraphTraversal2.__()
                                                            .InV()
                                                            .As("mdl")
                                                            .Select(GremlinKeyword.Pop.Last,
                                                                "@v")
                                                            .Both()
                                                            .Dedup()
                                                            .And(GraphTraversal2.__()
                                                                .Optional(
                                                                    GraphTraversal2.__()
                                                                        .Out("mdl"))
                                                                .Where(Predicate.eq(
                                                                    "mdl"))))
                                                    .As("@v")
                                                    .Optional(GraphTraversal2.__().FlatMap(
                                                        GraphTraversal2.__()
                                                            .Select(GremlinKeyword.Pop.Last,
                                                                "@e")
                                                            .Values("_ref")
                                                            .As("key")
                                                            .Select(GremlinKeyword.Pop.Last,
                                                                "@v")
                                                            .Optional(GraphTraversal2.__()
                                                                .Out("mdl"))
                                                            .OutE("ref")
                                                            .And(GraphTraversal2.__()
                                                                .Values("_key")
                                                                .Where(Predicate.eq(
                                                                    "key"))))))
                                            .Until(GraphTraversal2.__().FlatMap(
                                                GraphTraversal2.__()
                                                    .As("res")
                                                    .Select(GremlinKeyword.Pop.Last, "@v")
                                                    .Where(Predicate.eq("res"))))
                                            .SideEffect(
                                                GraphTraversal2.__()
                                                    .Project("data", "info")
                                                    .By(GraphTraversal2.__()
                                                        .Select("@e")
                                                        .Unfold()
                                                        .Project("key", "ref")
                                                        .By(GraphTraversal2.__().Values(
                                                            "_key"))
                                                        .By(GraphTraversal2.__().Values(
                                                            "_ref"))
                                                        .Fold())
                                                    .By(GraphTraversal2.__()
                                                        .Select("@v")
                                                        .Unfold()
                                                        .Project("id", "type", "etag")
                                                        .By(GraphTraversal2.__().Values(
                                                            "__id"))
                                                        .By(GraphTraversal2.__().Label())
                                                        .By(GraphTraversal2.__().Values(
                                                            "__etag"))
                                                        .Fold())
                                                    .Store("^refs")))
                                    .Dedup()
                                    .Union(GraphTraversal2.__().Identity().SideEffect(
                                        GraphTraversal2.__()
                                            .Group("^mdls")
                                            .By(GraphTraversal2.__().Id())
                                            .By(GraphTraversal2.__().Coalesce(
                                                GraphTraversal2.__().Out("mdl").Values(
                                                    "__id"),
                                                GraphTraversal2.__().Constant("")))),
                                        GraphTraversal2.__().Out("mdl"))
                                    .Dedup())
                            .Union(GraphTraversal2.__()
                                .Emit()
                                .Repeat(GraphTraversal2.__().OutE("_val").As("_").InV())
                                .Tree(),
                                GraphTraversal2.__().Cap("^ids"),
                                GraphTraversal2.__().Cap("^mdls"),
                                GraphTraversal2.__().Cap("^refs"))
                            .Fold()
                            .Union(GraphTraversal2.__().Identity(),
                                GraphTraversal2.__().Cap("^edges"))).Next();

            foreach (var result in pre_fetch)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void Test13GetProduct_format_resolved()
        {
            GraphViewConnection connection = GetGraphViewConnection();
            //connection.ResetCollection();

            GraphViewCommand graph = GetGraphViewCommand(connection);
            graph.OutputFormat = OutputFormat.GraphSON;

            var results =
                graph.g()
                    .V()
                    .Has("_app", "test-app")
                    .Has("__id", "test-app")
                    .HasLabel("application")
                    .Coalesce(
                        GraphTraversal2.__().Union(GraphTraversal2.__()
                            .Not(GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "test-app")
                                .HasLabel("application"))
                            .Constant("~0"),
                            GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "test-app")
                                .HasLabel("application")
                                .Has("_provisioningState", 0)
                                .Constant("~1"),
                            GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "test-app")
                                .HasLabel("application")
                                .Has("_provisioningState", 2)
                                .Constant("~2"),
                            GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "test-app")
                                .HasLabel("application")
                                .Has("_deleted", true)
                                .Constant("~3")),
                        GraphTraversal2.__()
                            .FlatMap(
                                GraphTraversal2.__()
                                    .V()
                                    .Has("_app", "test-app")
                                    .Has("__id", "uber-product:soda-machine:shop-3")
                                    .HasLabel("product")
                                    .Range(0, 100)
                                    .Union(GraphTraversal2.__().Identity().SideEffect(
                                        GraphTraversal2.__().Id().Store("^ids")),
                                        GraphTraversal2.__()
                                            .As("@v")
                                            .FlatMap(GraphTraversal2.__()
                                                .Optional(
                                                    GraphTraversal2.__().Out("mdl"))
                                                .OutE("ref"))
                                            .Repeat(
                                                GraphTraversal2.__()
                                                    .As("@e")
                                                    .FlatMap(
                                                        GraphTraversal2
                                                            .__()
                                                            .InV()
                                                            .As("mdl")
                                                            .Select(GremlinKeyword.Pop.Last,
                                                                "@v")
                                                            .Both()
                                                            .Dedup()
                                                            .And(GraphTraversal2.__()
                                                                .Optional(
                                                                    GraphTraversal2.__()
                                                                        .Out("mdl"))
                                                                .Where(Predicate.eq(
                                                                    "mdl"))))
                                                    .As("@v")
                                                    .Optional(GraphTraversal2.__().FlatMap(
                                                        GraphTraversal2.__()
                                                            .Select(GremlinKeyword.Pop.Last,
                                                                "@e")
                                                            .Values("_ref")
                                                            .As("key")
                                                            .Select(GremlinKeyword.Pop.Last,
                                                                "@v")
                                                            .Optional(GraphTraversal2.__()
                                                                .Out("mdl"))
                                                            .OutE("ref")
                                                            .And(GraphTraversal2.__()
                                                                .Values("_key")
                                                                .Where(Predicate.eq(
                                                                    "key"))))))
                                            .Until(GraphTraversal2.__().FlatMap(
                                                GraphTraversal2.__()
                                                    .As("res")
                                                    .Select(GremlinKeyword.Pop.Last, "@v")
                                                    .Where(Predicate.eq("res"))))
                                            .SideEffect(
                                                GraphTraversal2.__()
                                                    .Project("data", "info")
                                                    .By(GraphTraversal2.__()
                                                        .Select("@e")
                                                        .Unfold()
                                                        .Project("key", "ref")
                                                        .By(GraphTraversal2.__().Values(
                                                            "_key"))
                                                        .By(GraphTraversal2.__().Values(
                                                            "_ref"))
                                                        .Fold())
                                                    .By(GraphTraversal2.__()
                                                        .Select("@v")
                                                        .Unfold()
                                                        .Project("id", "type", "etag")
                                                        .By(GraphTraversal2.__().Values(
                                                            "__id"))
                                                        .By(GraphTraversal2.__().Label())
                                                        .By(GraphTraversal2.__().Values(
                                                            "__etag"))
                                                        .Fold())
                                                    .Store("^refs")))
                                    .Dedup()
                                    .Union(GraphTraversal2.__().Identity().SideEffect(
                                        GraphTraversal2.__()
                                            .Group("^mdls")
                                            .By(GraphTraversal2.__().Id())
                                            .By(GraphTraversal2.__().Coalesce(
                                                GraphTraversal2.__().Out("mdl").Values(
                                                    "__id"),
                                                GraphTraversal2.__().Constant("")))),
                                        GraphTraversal2.__().Out("mdl"))
                                    .Dedup())
                            .Union(GraphTraversal2.__()
                                .Emit()
                                .Repeat(GraphTraversal2.__().OutE("_val").As("_").InV())
                                .Tree(),
                                GraphTraversal2.__().Cap("^ids"),
                                GraphTraversal2.__().Cap("^mdls"),
                                GraphTraversal2.__().Cap("^refs"))
                            .Fold()).Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void Test14DeleteProduct()
        {
            GraphViewConnection connection = GetGraphViewConnection();
            //connection.ResetCollection();

            GraphViewCommand graph = GetGraphViewCommand(connection);
            graph.OutputFormat = OutputFormat.GraphSON;

            var results =
                graph.g().Inject(0).Coalesce(
                    GraphTraversal2.__().Union(GraphTraversal2.__()
                        .Not(GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "test-app")
                            .HasLabel("application"))
                        .Constant("~0"),
                        GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "test-app")
                            .HasLabel("application")
                            .Has("_provisioningState", 0)
                            .Constant("~1"),
                        GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "test-app")
                            .HasLabel("application")
                            .Has("_provisioningState", 2)
                            .Constant("~2"),
                        GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "test-app")
                            .HasLabel("application")
                            .Has("_deleted", true)
                            .Constant("~3")),
                    GraphTraversal2.__()
                        .Project("#v0")
                        .By(GraphTraversal2.__().Coalesce(
                            GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "uber-product:soda-machine:shop-3")
                                .HasLabel("product")
                                .SideEffect(GraphTraversal2.__().Union(
                                    GraphTraversal2.__().Properties().Drop(),
                                    GraphTraversal2.__()
                                        .Repeat(GraphTraversal2.__().Out("_val"))
                                        .Emit()
                                        .Barrier()
                                        .Drop())),
                            GraphTraversal2.__().Constant("")))
                        .As("#v")
                        .Map(
                            GraphTraversal2.__()
                                .Union(GraphTraversal2.__()
                                    .Select("#v")
                                    .Union(GraphTraversal2.__()
                                        .Select("#v0")
                                        .Is(Predicate.neq(""))
                                        .As("#a")
                                        .Constant(""))
                                    .As("#p"))
                                .Select("#a")
                                .Union(
                                    GraphTraversal2.__().Identity(),
                                    GraphTraversal2.__()
                                        .As("@v")
                                        .FlatMap(GraphTraversal2.__()
                                            .Optional(GraphTraversal2.__().Out("mdl"))
                                            .InE("ref")
                                            .Or
                                            (GraphTraversal2.__().Select("#p").Is(""),
                                                GraphTraversal2.__().Values("_key").Where(
                                                    Predicate.within("#p"))))
                                        .Repeat(
                                            GraphTraversal2.__()
                                                .As("@e")
                                                .FlatMap(
                                                    GraphTraversal2.__()
                                                        .OutV()
                                                        .As("mdl")
                                                        .Select(GremlinKeyword.Pop.Last, "@v")
                                                        .Both()
                                                        .Dedup()
                                                        .And(GraphTraversal2.__()
                                                            .Optional(GraphTraversal2.__()
                                                                .Out("mdl"))
                                                            .Where(Predicate.eq("mdl"))))
                                                .As("@v")
                                                .Optional(GraphTraversal2.__().FlatMap(
                                                    GraphTraversal2.__()
                                                        .Select(GremlinKeyword.Pop.Last, "@e")
                                                        .Values("_key")
                                                        .As("key")
                                                        .Select(GremlinKeyword.Pop.Last, "@v")
                                                        .Optional(
                                                            GraphTraversal2.__().Out("mdl"))
                                                        .InE("ref")
                                                        .And(GraphTraversal2.__()
                                                            .Values("_ref")
                                                            .Where(Predicate.eq("key"))))))
                                        .Until(GraphTraversal2.__().FlatMap(
                                            GraphTraversal2.__()
                                                .As("res")
                                                .Select(GremlinKeyword.Pop.Last, "@v")
                                                .Where(Predicate.eq("res"))))
                                        .Select("@v")
                                        .Unfold())
                                .Dedup()
                                .Fold())
                        .As("#r")
                        .Map(GraphTraversal2.__()
                            .Union(GraphTraversal2.__().Select("#v").Union(
                                GraphTraversal2.__().Select("#v0").Is(Predicate.neq(""))))
                            .Fold())
                        .As("#d")
                        .Map(GraphTraversal2.__()
                            .Select("#r")
                            .Unfold()
                            .Where(Predicate.without("#d"))
                            .Fold())
                        .SideEffect(GraphTraversal2.__().Select("#d").Unfold().Drop())
                        .Unfold()
                        .Project("id", "type", "etag")
                        .By(GraphTraversal2.__().Values("__id"))
                        .By(GraphTraversal2.__().Label())
                        .By(GraphTraversal2.__().Values("__etag"))).Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void Test15DeleteProductModel()
        {
            GraphViewConnection connection = GetGraphViewConnection();
            //connection.ResetCollection();

            GraphViewCommand graph = GetGraphViewCommand(connection);
            graph.OutputFormat = OutputFormat.GraphSON;

            var results =
                graph.g().Inject(0).Coalesce(
                    GraphTraversal2.__().Union(GraphTraversal2.__()
                        .Not(GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "test-app")
                            .HasLabel("application"))
                        .Constant("~0"),
                        GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "test-app")
                            .HasLabel("application")
                            .Has("_provisioningState", 0)
                            .Constant("~1"),
                        GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "test-app")
                            .HasLabel("application")
                            .Has("_provisioningState", 2)
                            .Constant("~2"),
                        GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "test-app")
                            .HasLabel("application")
                            .Has("_deleted", true)
                            .Constant("~3"),
                        GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("__id", "uber-product:soda-machine")
                            .HasLabel("product-model")
                            .In("mdl")
                            .Constant("~4")),
                    GraphTraversal2.__()
                        .Project("#v0")
                        .By(GraphTraversal2.__().Coalesce(
                            GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "uber-product:soda-machine")
                                .HasLabel("product-model")
                                .SideEffect(GraphTraversal2.__().Union(
                                    GraphTraversal2.__().Properties().Drop(),
                                    GraphTraversal2.__()
                                        .Repeat(GraphTraversal2.__().Out("_val"))
                                        .Emit()
                                        .Barrier()
                                        .Drop())),
                            GraphTraversal2.__().Constant("")))
                        .As("#v")
                        .Map(
                            GraphTraversal2.__()
                                .Union(GraphTraversal2.__()
                                    .Select("#v")
                                    .Union(GraphTraversal2.__()
                                        .Select("#v0")
                                        .Is(Predicate.neq(""))
                                        .As("#a")
                                        .Constant(""))
                                    .As("#p"))
                                .Select("#a")
                                .Union(
                                    GraphTraversal2.__().Identity(),
                                    GraphTraversal2.__()
                                        .As("@v")
                                        .FlatMap(GraphTraversal2.__()
                                            .Optional(GraphTraversal2.__().Out("mdl"))
                                            .InE("ref")
                                            .Or(GraphTraversal2.__().Select("#p").Is(""),
                                                GraphTraversal2.__().Values("_key").Where(
                                                    Predicate.within("#p"))))
                                        .Repeat(
                                            GraphTraversal2.__()
                                                .As("@e")
                                                .FlatMap(
                                                    GraphTraversal2.__()
                                                        .OutV()
                                                        .As("mdl")
                                                        .Select(GremlinKeyword.Pop.Last, "@v")
                                                        .Both()
                                                        .Dedup()
                                                        .And(GraphTraversal2.__()
                                                            .Optional(GraphTraversal2.__()
                                                                .Out("mdl"))
                                                            .Where(Predicate.eq("mdl"))))
                                                .As("@v")
                                                .Optional(GraphTraversal2.__().FlatMap(
                                                    GraphTraversal2.__()
                                                        .Select(GremlinKeyword.Pop.Last, "@e")
                                                        .Values("_key")
                                                        .As("key")
                                                        .Select(GremlinKeyword.Pop.Last, "@v")
                                                        .Optional(
                                                            GraphTraversal2.__().Out("mdl"))
                                                        .InE("ref")
                                                        .And(GraphTraversal2.__()
                                                            .Values("_ref")
                                                            .Where(Predicate.eq("key"))))))
                                        .Until(GraphTraversal2.__().FlatMap(
                                            GraphTraversal2.__()
                                                .As("res")
                                                .Select(GremlinKeyword.Pop.Last, "@v")
                                                .Where(Predicate.eq("res"))))
                                        .Select("@v")
                                        .Unfold())
                                .Dedup()
                                .Fold())
                        .As("#r")
                        .Map(GraphTraversal2.__()
                            .Union(GraphTraversal2.__().Select("#v").Union(
                                GraphTraversal2.__().Select("#v0").Is(Predicate.neq(""))))
                            .Fold())
                        .As("#d")
                        .Map(GraphTraversal2.__()
                            .Select("#r")
                            .Unfold()
                            .Where(Predicate.without("#d"))
                            .Fold())
                        .SideEffect(GraphTraversal2.__().Select("#d").Unfold().Drop())
                        .Unfold()
                        .Project("id", "type", "etag")
                        .By(GraphTraversal2.__().Values("__id"))
                        .By(GraphTraversal2.__().Label())
                        .By(GraphTraversal2.__().Values("__etag"))).Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void Test16GetProduct_notfound()
        {
            GraphViewConnection connection = GetGraphViewConnection();
            //connection.ResetCollection();

            GraphViewCommand graph = GetGraphViewCommand(connection);
            graph.OutputFormat = OutputFormat.GraphSON;

            var results =
                graph.g()
                    .V()
                    .Has("_app", "test-app")
                    .Has("__id", "test-app")
                    .HasLabel("application")
                    .Coalesce(
                        GraphTraversal2.__().Union(GraphTraversal2.__()
                            .Not(GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "test-app")
                                .HasLabel("application"))
                            .Constant("~0"),
                            GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "test-app")
                                .HasLabel("application")
                                .Has("_provisioningState", 0)
                                .Constant("~1"),
                            GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "test-app")
                                .HasLabel("application")
                                .Has("_provisioningState", 2)
                                .Constant("~2"),
                            GraphTraversal2.__()
                                .V()
                                .Has("_app", "test-app")
                                .Has("__id", "test-app")
                                .HasLabel("application")
                                .Has("_deleted", true)
                                .Constant("~3")),
                        GraphTraversal2.__()
                            .FlatMap(
                                GraphTraversal2.__()
                                    .V()
                                    .Has("_app", "test-app")
                                    .Has("__id", "uber-product:soda-machine:shop-3")
                                    .HasLabel("product")
                                    .Range(0, 100)
                                    .Union(GraphTraversal2.__().Identity().SideEffect(
                                        GraphTraversal2.__().Id().Store("^ids")),
                                        GraphTraversal2.__()
                                            .As("@v")
                                            .FlatMap(GraphTraversal2.__()
                                                .Optional(
                                                    GraphTraversal2.__().Out("mdl"))
                                                .OutE("ref"))
                                            .Repeat(
                                                GraphTraversal2.__()
                                                    .As("@e")
                                                    .FlatMap(
                                                        GraphTraversal2
                                                            .__()
                                                            .InV()
                                                            .As("mdl")
                                                            .Select(GremlinKeyword.Pop.Last,
                                                                "@v")
                                                            .Both()
                                                            .Dedup()
                                                            .And(GraphTraversal2.__()
                                                                .Optional(
                                                                    GraphTraversal2.__()
                                                                        .Out("mdl"))
                                                                .Where(Predicate.eq(
                                                                    "mdl"))))
                                                    .As("@v")
                                                    .Optional(GraphTraversal2.__().FlatMap(
                                                        GraphTraversal2.__()
                                                            .Select(GremlinKeyword.Pop.Last,
                                                                "@e")
                                                            .Values("_ref")
                                                            .As("key")
                                                            .Select(GremlinKeyword.Pop.Last,
                                                                "@v")
                                                            .Optional(GraphTraversal2.__()
                                                                .Out("mdl"))
                                                            .OutE("ref")
                                                            .And(GraphTraversal2.__()
                                                                .Values("_key")
                                                                .Where(Predicate.eq(
                                                                    "key"))))))
                                            .Until(GraphTraversal2.__().FlatMap(
                                                GraphTraversal2.__()
                                                    .As("res")
                                                    .Select(GremlinKeyword.Pop.Last, "@v")
                                                    .Where(Predicate.eq("res"))))
                                            .SideEffect(
                                                GraphTraversal2.__()
                                                    .Project("data", "info")
                                                    .By(GraphTraversal2.__()
                                                        .Select("@e")
                                                        .Unfold()
                                                        .Project("key", "ref")
                                                        .By(GraphTraversal2.__().Values(
                                                            "_key"))
                                                        .By(GraphTraversal2.__().Values(
                                                            "_ref"))
                                                        .Fold())
                                                    .By(GraphTraversal2.__()
                                                        .Select("@v")
                                                        .Unfold()
                                                        .Project("id", "type", "etag")
                                                        .By(GraphTraversal2.__().Values(
                                                            "__id"))
                                                        .By(GraphTraversal2.__().Label())
                                                        .By(GraphTraversal2.__().Values(
                                                            "__etag"))
                                                        .Fold())
                                                    .Store("^refs")))
                                    .Dedup()
                                    .Union(GraphTraversal2.__().Identity().SideEffect(
                                        GraphTraversal2.__()
                                            .Group("^mdls")
                                            .By(GraphTraversal2.__().Id())
                                            .By(GraphTraversal2.__().Coalesce(
                                                GraphTraversal2.__().Out("mdl").Values(
                                                    "__id"),
                                                GraphTraversal2.__().Constant("")))),
                                        GraphTraversal2.__().Out("mdl"))
                                    .Dedup())
                            .Union(GraphTraversal2.__()
                                .Emit()
                                .Repeat(GraphTraversal2.__().OutE("_val").As("_").InV())
                                .Tree(),
                                GraphTraversal2.__().Cap("^ids"),
                                GraphTraversal2.__().Cap("^mdls"),
                                GraphTraversal2.__().Cap("^refs"))
                            .Fold()).Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void Test17DeleteApp()
        {
            GraphViewConnection connection = GetGraphViewConnection();
            //connection.ResetCollection();

            GraphViewCommand graph = GetGraphViewCommand(connection);

            var results =
                graph.g().V().Has("_app", "test-app").Drop().Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }
    }
}
