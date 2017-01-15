using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewUnitTest
{
    [TestClass]
    public class GremlinFunctionalTestSuite
    {
        [TestMethod]
        public void Test1CreateApplication()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "GremlinFunctionalTestSuite");
            //connection.ResetCollection();

            GraphViewCommand graph = new GraphViewCommand(connection);
            var results = graph.g()
                .AddV("application")
                .Property("_app", "test-app")
                .Property("_id", "test-app")
                .Property("_provisioningState", 1)
                .Property("_deleted", false)
                .Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void Test2ImportModels()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "GremlinFunctionalTestSuite");
            //connection.ResetCollection();

            GraphViewCommand graph = new GraphViewCommand(connection);

            var results = graph
                .g()
                .Inject(0)
                .Coalesce(
                    GraphTraversal2.__().Union(
                        GraphTraversal2.__()
                            .Not(GraphTraversal2.__()
                                     .V()
                                     .Has("_app", "test-app")
                                     .Has("_id", "test-app")
                                     .HasLabel("application"))
                            .Constant("~0"),
                        GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("_id", "test-app")
                            .HasLabel("application")
                            .Has("_provisioningState", 0)
                            .Constant("~1"),
                        GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("_id", "test-app")
                            .HasLabel("application")
                            .Has("_provisioningState", 2)
                            .Constant("~2"),
                        GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("_id", "test-app")
                            .HasLabel("application")
                            .Has("_deleted", true)
                            .Constant("~3"),
                        GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("_id", "product:soda-machine")
                            .HasLabel("product-model")
                            .Constant("~4"),
                        GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("_id", "uber-product:soda-machine")
                            .HasLabel("product-model")
                            .Constant("~5"),
                        GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("_id", "device:ice-machine")
                            .HasLabel("device-model")
                            .Constant("~6"),
                        GraphTraversal2.__()
                            .V()
                            .Has("_app", "test-app")
                            .Has("_id", "device:soda-mixer")
                            .HasLabel("device-model")
                            .Constant("~7")),
                    GraphTraversal2.__()
                        .Project("#v0", "#v1", "#v2", "#v3")
                        .By(GraphTraversal2.__()
                                .AddV("product-model")
                                .Property("_app", "test-app")
                                .Property("_id", "product:soda-machine")
                                .Property("_etag", "B0vDw1xnS/agXzX9F7wxHg==")
                                .SideEffect(GraphTraversal2.__().Union(
                                    GraphTraversal2.__().Property("_name", "SodaMachine"),
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
                                                    .Property("id", "location")
                                                    .Property("name", "Sodamachinelocation")
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
                                                    .Property("id", "installer")
                                                    .Property("name",
                                                              "Sodamachineinstaller")
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
                                                    .Property("id", "syrup_level")
                                                    .Property("name", "Syruplevel")
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
                                                            .Property("id",
                                                                      "device:soda-mixer")
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
                                                    .Property("id", "ice_level")
                                                    .Property("name", "Icelevel")
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
                                                            .Property("id",
                                                                      "device:ice-machine")
                                                            .Property("type",
                                                                      "device")))))))
                        .By(GraphTraversal2.__()
                                .AddV("product-model")
                                .Property("_app", "test-app")
                                .Property("_id", "uber-product:soda-machine")
                                .Property("_etag", "SkYTpr1hSkCL4NkpsfNwvQ==")
                                .SideEffect(GraphTraversal2.__().Union(
                                    GraphTraversal2.__().Property("_name",
                                                                  "UberSodaMachine"),
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
                                                    .Property("id", "location")
                                                    .Property("name", "Sodamachinelocation")
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
                                                    .Property("id", "installer")
                                                    .Property("name",
                                                              "Sodamachineinstaller")
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
                                                    .Property("id", "syrup_level")
                                                    .Property("name", "SyrupLevel")
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
                                                            .Property(
                                                                "id",
                                                                "product:soda-machine")
                                                            .Property("type",
                                                                      "product")))))))
                        .By(GraphTraversal2.__()
                                .AddV("device-model")
                                .Property("_app", "test-app")
                                .Property("_id", "device:ice-machine")
                                .Property("_etag", "SWnFiMWDTVGOWUJvcqCbtg==")
                                .SideEffect(GraphTraversal2.__().Union(
                                    GraphTraversal2.__().Property("_name", "IceMachine"),
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
                                                    .Property("id", "firmware_version")
                                                    .Property("name", "FirmwareVersion")
                                                    .Property("kind", "desired")
                                                    .Property("type", "string")
                                                    .Property("path", "/firmware_version"))
                                            .SideEffect(
                                                GraphTraversal2.__()
                                                    .AddE("_val")
                                                    .To(GraphTraversal2.__()
                                                            .AddV("_val")
                                                            .Property("_app", "test-app"))
                                                    .Property("_key", "1")
                                                    .Property("_ary", false)
                                                    .InV()
                                                    .Property("id", "serial_number")
                                                    .Property("name", "SerialNumber")
                                                    .Property("kind", "desired")
                                                    .Property("type", "string")
                                                    .Property("path", "/serial_number"))
                                            .SideEffect(
                                                GraphTraversal2.__()
                                                    .AddE("_val")
                                                    .To(GraphTraversal2.__()
                                                            .AddV("_val")
                                                            .Property("_app", "test-app"))
                                                    .Property("_key", "2")
                                                    .Property("_ary", false)
                                                    .InV()
                                                    .Property("id", "ice_level")
                                                    .Property("name", "IceLevel")
                                                    .Property("kind", "reported")
                                                    .Property("type", "number")
                                                    .Property("path", "/ice_level"))))))
                        .By(GraphTraversal2.__()
                                .AddV("device-model")
                                .Property("_app", "test-app")
                                .Property("_id", "device:soda-mixer")
                                .Property("_etag", "lsRrd7JWSBqW9kiBVPS7aQ==")
                                .SideEffect(GraphTraversal2.__().Union(
                                    GraphTraversal2.__().Property("_name", "SodaMixer"),
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
                                                    .Property("id", "firmware_version")
                                                    .Property("name", "FirmwareVersion")
                                                    .Property("kind", "desired")
                                                    .Property("type", "string")
                                                    .Property("path", "/firmware_version"))
                                            .SideEffect(
                                                GraphTraversal2.__()
                                                    .AddE("_val")
                                                    .To(GraphTraversal2.__()
                                                            .AddV("_val")
                                                            .Property("_app", "test-app"))
                                                    .Property("_key", "1")
                                                    .Property("_ary", false)
                                                    .InV()
                                                    .Property("id", "serial_number")
                                                    .Property("name", "SerialNumber")
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
                                                            .Property("id", "co2_level")
                                                            .Property("name", "CO2Level")
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
                                                    .Property("id", "syrup_level")
                                                    .Property("name", "SyrupLevel")
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
                                           new List<Object> { "_name", "_properties" }),
                                       GraphTraversal2.__().Select("#v1").As("#a").Constant(
                                           new List<Object> { "_name", "_properties" }),
                                       GraphTraversal2.__().Select("#v2").As("#a").Constant(
                                           new List<Object> { "_name", "_properties" }),
                                       GraphTraversal2.__().Select("#v3").As("#a").Constant(
                                           new List<Object> { "_name", "_properties" }))
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
                                .Union(
                                    GraphTraversal2.__().InV().As("#a").Select("#f").OutV(),
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
                                .Repeat(
                                    GraphTraversal2.__()
                                        .As("@e")
                                        .FlatMap(
                                            GraphTraversal2.__()
                                                .OutV()
                                                .As("mdl")
                                                .Select(GremlinKeyword.Pop.last, "@v")
                                                .Both()
                                                .Dedup()
                                                .And
                                            (GraphTraversal2.__()
                                                 .Optional(GraphTraversal2.__().Out("mdl"))
                                                 .Where(Predicate.eq("mdl"))))
                                        .As("@v")
                                        .Optional(GraphTraversal2.__().FlatMap(
                                            GraphTraversal2.__()
                                                .Select(GremlinKeyword.Pop.last, "@e")
                                                .Values("_key")
                                                .As("key")
                                                .Select(GremlinKeyword.Pop.last, "@v")
                                                .Optional(GraphTraversal2.__().Out("mdl"))
                                                .InE("ref")
                                                .And
                                            (GraphTraversal2.__().Values("_ref").Where(
                                                Predicate.eq("key"))))))
                                .Until(GraphTraversal2.__().FlatMap(
                                    GraphTraversal2.__()
                                        .As("res")
                                        .Select(GremlinKeyword.Pop.last, "@v")
                                        .Where(Predicate.eq("res"))))
                                .Select("@v")
                                .Unfold())
                        .Dedup()
                        .Project("id", "type", "etag")
                        .By(GraphTraversal2.__().Values("_id"))
                        .By(GraphTraversal2.__().Label())
                        .By(GraphTraversal2.__().Values("_etag")))
                .Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }
    }
}
