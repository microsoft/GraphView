using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.CodeDom.Compiler;
using System.Reflection;
using Microsoft.CSharp;

namespace GremlinTranslationOperator.Tests
{
    [TestClass()]
    public class GremlinTranslationOperator
    {
        [TestMethod]
        public void ExecutingGraphTraversalString()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "Modern");

            
            GraphTraversal2 graph = new GraphTraversal2(connection);
            string traversalStr = "graph.g().V().In().Out()";

            var result = graph.EvalGraphTraversal(traversalStr);
        }


        [TestMethod]
        public void TestModernGraph()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "Modern");
            connection.ResetCollection();

            GraphViewCommand graph = new GraphViewCommand(connection);

            graph.g().AddV("person").Property("age", "27").Property("name", "vadas").Next();
            graph.g().AddV("person").Property("age", "29").Property("name", "marko").Next();
            graph.g().AddV("person").Property("age", "35").Property("name", "peter").Next();
            graph.g().AddV("person").Property("age", "32").Property("name", "josh").Next();
            graph.g().AddV("software").Property("lang", "java").Property("name", "lop").Next();
            graph.g().AddV("software").Property("lang", "java").Property("name", "ripple").Next();

            graph.g().V().Has("name", "marko").AddE("knows").To(graph.g().V().Has("name", "vadas")).Next();
            graph.g().V().Has("name", "marko").AddE("knows").To(graph.g().V().Has("name", "josh")).Next();
            graph.g().V().Has("name", "marko").AddE("knows").To(graph.g().V().Has("name", "lop")).Next();
            graph.g().V().Has("name", "peter").AddE("created").To(graph.g().V().Has("name", "lop")).Next();
            graph.g().V().Has("name", "josh").AddE("created").To(graph.g().V().Has("name", "lop")).Next();
            graph.g().V().Has("name", "josh").AddE("created").To(graph.g().V().Has("name", "ripple")).Next();

        }

        [TestMethod]
        public void TestExecuteCommandText()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "Modern");
            GraphViewCommand graph = new GraphViewCommand(connection);

            graph.CommandText =
                "g.inject(0).coalesce(__.union(__.not(__.V().has('_app','test-app').has('_id','test-app').hasLabel('application')).constant('~0'),__.V().has('_app','test-app').has('_id','test-app').hasLabel('application').has('_provisioningState',0).constant('~1'),__.V().has('_app','test-app').has('_id','test-app').hasLabel('application').has('_provisioningState',2).constant('~2'),__.V().has('_app','test-app').has('_id','test-app').hasLabel('application').has('_deleted',true).constant('~3'),__.V().has('_app','test-app').has('_id','product:soda-machine').hasLabel('product-model').constant('~4'),__.V().has('_app','test-app').has('_id','uber-product:soda-machine').hasLabel('product-model').constant('~5'),__.V().has('_app','test-app').has('_id','device:ice-machine').hasLabel('device-model').constant('~6'),__.V().has('_app','test-app').has('_id','device:soda-mixer').hasLabel('device-model').constant('~7')),__.project('#v0','#v1','#v2','#v3').by(__.addV('product-model').property('_app','test-app').property('_id','product:soda-machine').property('_etag','B0vDw1xnS/agXzX9F7wxHg==').sideEffect(__.union(__.property('_name','Soda Machine'),__.sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','_properties').property('_ary',true).inV().sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','0').property('_ary',false).inV().property('id','location').property('name','Soda machine location').property('kind','property').property('type','string')).sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','1').property('_ary',false).inV().property('id','installer').property('name','Soda machine installer').property('kind','property').property('type','string')).sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','2').property('_ary',false).inV().property('id','syrup_level').property('name','Syrup level').property('kind','reference').sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','target').property('_ary',false).inV().property('id','device:soda-mixer').property('type','device'))).sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','3').property('_ary',false).inV().property('id','ice_level').property('name','Ice level').property('kind','reference').sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','target').property('_ary',false).inV().property('id','device:ice-machine').property('type','device'))))))).by(__.addV('product-model').property('_app','test-app').property('_id','uber-product:soda-machine').property('_etag','SkYTpr1hSkCL4NkpsfNwvQ==').sideEffect(__.union(__.property('_name','Uber Soda Machine'),__.sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','_properties').property('_ary',true).inV().sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','0').property('_ary',false).inV().property('id','location').property('name','Soda machine location').property('kind','property').property('type','string')).sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','1').property('_ary',false).inV().property('id','installer').property('name','Soda machine installer').property('kind','property').property('type','string')).sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','2').property('_ary',false).inV().property('id','syrup_level').property('name','Syrup Level').property('kind','reference').sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','target').property('_ary',false).inV().property('id','product:soda-machine').property('type','product'))))))).by(__.addV('device-model').property('_app','test-app').property('_id','device:ice-machine').property('_etag','SWnFiMWDTVGOWUJvcqCbtg==').sideEffect(__.union(__.property('_name','Ice Machine'),__.sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','_properties').property('_ary',true).inV().sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','0').property('_ary',false).inV().property('id','firmware_version').property('name','Firmware Version').property('kind','desired').property('type','string').property('path','/firmware_version')).sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','1').property('_ary',false).inV().property('id','serial_number').property('name','Serial Number').property('kind','desired').property('type','string').property('path','/serial_number')).sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','2').property('_ary',false).inV().property('id','ice_level').property('name','Ice Level').property('kind','reported').property('type','number').property('path','/ice_level')))))).by(__.addV('device-model').property('_app','test-app').property('_id','device:soda-mixer').property('_etag','lsRrd7JWSBqW9kiBVPS7aQ==').sideEffect(__.union(__.property('_name','Soda Mixer'),__.sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','_properties').property('_ary',true).inV().sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','0').property('_ary',false).inV().property('id','firmware_version').property('name','Firmware Version').property('kind','desired').property('type','string').property('path','/firmware_version')).sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','1').property('_ary',false).inV().property('id','serial_number').property('name','Serial Number').property('kind','desired').property('type','string').property('path','/serial_number')).sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','2').property('_ary',false).inV().property('id','co2_level').property('name','CO2 Level').property('kind','reported').property('type','number').property('path','/co2_level')).sideEffect(__.addE('_val').to(__.addV('_val').property('_app','test-app')).property('_key','3').property('_ary',false).inV().property('id','syrup_level').property('name','Syrup Level').property('kind','reported').property('type','number').property('path','/syrup_level')))))).as('#v').project('#e0','#e1','#e2','#e3','#e4','#e5').by(__.select('#v2').addE('device-product').to(__.select('#v').select('#v0'))).by(__.select('#v3').addE('device-product').to(__.select('#v').select('#v0'))).by(__.select('#v0').addE('product-product').to(__.select('#v').select('#v1'))).by(__.select('#v0').addE('ref').to(__.select('#v').select('#v3')).property('_key','syrup_level').property('_ref','syrup_level')).by(__.select('#v0').addE('ref').to(__.select('#v').select('#v2')).property('_key','ice_level').property('_ref','ice_level')).by(__.select('#v1').addE('ref').to(__.select('#v').select('#v0')).property('_key','syrup_level').property('_ref','syrup_level')).as('#e').union(__.select('#v').union(__.select('#v0').as('#a').constant(['_name','_properties']),__.select('#v1').as('#a').constant(['_name','_properties']),__.select('#v2').as('#a').constant(['_name','_properties']),__.select('#v3').as('#a').constant(['_name','_properties'])).as('#p'),__.select('#e').union(__.select('#e0'),__.select('#e1'),__.select('#e2'),__.select('#e3'),__.select('#e4'),__.select('#e5')).as('#f').union(__.inV().as('#a').select('#f').outV(),__.outV().as('#a').select('#f').inV()).map(__.optional(__.out('mdl')).as('#m').select('#a').optional(__.out('mdl')).inE('ref').and(__.outV().where(eq('#m'))).values('_key').fold()).as('#p')).select('#a').union(__.identity(),__.as('@v').flatMap(__.optional(__.out('mdl')).inE('ref').and(__.values('_key').where(within('#p')))).repeat(__.as('@e').flatMap(__.outV().as('mdl').select(last,'@v').both().dedup().and(__.optional(__.out('mdl')).where(eq('mdl')))).as('@v').optional(__.flatMap(__.select(last,'@e').values('_key').as('key').select(last,'@v').optional(__.out('mdl')).inE('ref').and(__.values('_ref').where(eq('key')))))).until(__.flatMap(__.as('res').select(last,'@v').where(eq('res')))).select('@v').unfold()).dedup().project('id','type','etag').by(__.values('_id')).by(__.label()).by(__.values('_etag')))";
            var results = graph.Execute();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void TestStep()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "Modern");
            GraphViewCommand graph = new GraphViewCommand(connection);

            graph.g().V().Group().By(GraphTraversal2.__().Label()).By("id").Next();

            
            //var results = graph.g().V().Out().Union(GraphTraversal2.__().Out().As("a").In()).Select("a").Out().Next();
            //var results = graph.g().V().Out().As("b").Union(GraphTraversal2.__().Out().As("a")).In().Union(GraphTraversal2.__().Out().Select("a").Out()).Next();

            //var results = graph.g()
            //    .V()
            //    .As("node")
            //    .Union(GraphTraversal2.__().Constant(new List<object> { "HAWK", "AVF 4" }))
            //    .As("list")
            //    .Values("name")
            //    .Where(Predicate.within("list"));
            //var result = graph.g().V().As("a").Out().In().Select("a").Next();
            //var result = graph.g().V().And(GraphTraversal2.__().Values("age").Is(Predicate.lt(30))).Values("name").Next();
            //var result = graph.g().V().As("a").Constant(new List<object>() {"name", "properties"}).As("p").Select("a").Values("name").Where(Predicate.within("p")).Next();

            //connection.ResetCollection();
            //graph.g().V().Has("comicbook", "AVF 4").InE().OutV().Property("test", "123").Next();
            //graph.g().V().OutE().Property("test", "name").Next();
            //var results = graph.g().V().HasLabel("Stanford").Union(GraphTraversal2.__().Out()).path().Next();
            //var results = graph.g().V().Has("type", "University").Project("info", "edge_label").By(GraphTraversal2.__().Properties("label", "type")).By(GraphTraversal2.__().Optional(GraphTraversal2.__().InE().Properties("label"))).Next();

            //var result = graph.g().V().Out().Inject(1).Next();
            //var result = graph.g().V().Out().Union(GraphTraversal2.__().In().Out(), GraphTraversal2.__().In()).Out().path().Tree().Next();
            //var result = graph.g().V().Out().Union(GraphTraversal2.__().In().Out(), GraphTraversal2.__().In()).Out().Tree().Next();

            //graph.g().AddV().Property("name", "a").Property("type", "start").Next();
            //graph.g().AddV().Property("name", "b").Next();
            //graph.g().AddV().Property("name", "c").Next();
            //graph.g().AddV().Property("name", "d").Next();
            //graph.g().AddV().Property("name", "e").Next();
            //graph.g().AddV().Property("name", "f").Next();
            //graph.g().AddV().Property("name", "g").Property("type", "start").Next();
            //graph.g().AddV().Property("name", "h").Next();
            //graph.g().AddV().Property("name", "i").Next();

            //graph.g().V().Has("name", "a").AddE("val").To(graph.g().V().Has("name", "b")).Next();
            //graph.g().V().Has("name", "a").AddE("val").To(graph.g().V().Has("name", "d")).Next();
            //graph.g().V().Has("name", "b").AddE("fail").To(graph.g().V().Has("name", "c")).Next();
            //graph.g().V().Has("name", "d").AddE("val").To(graph.g().V().Has("name", "e")).Next();
            //graph.g().V().Has("name", "d").AddE("fail").To(graph.g().V().Has("name", "f")).Next();
            //graph.g().V().Has("name", "g").AddE("val").To(graph.g().V().Has("name", "h")).Next();
            //graph.g().V().Has("name", "g").AddE("fail").To(graph.g().V().Has("name", "i")).Next();

            //var results = graph.g().V().Has("type", "start").Emit().Repeat(GraphTraversal2.__().OutE("val").InV()).Next();

            //a, g, b, d, e, h


            //graph.g().V().In().Out().Next();
            //graph.g().V().OutE().As("a").FlatMap(GraphTraversal2.__().InV()).Select("a").InV().Next();
            //graph.g().V().Out().In().Next();
            //graph.g().V().InE().InV().Next();
            //graph.g().V().InE().OutV().Next();
            //graph.g().V().OutE().OutV().Next();
            //graph.g().V().OutE().InV().Next();
            //graph.g().V().BothE().InV().Next();
            //graph.g().V().BothE().OutV().Next();
            //graph.g().V().BothE().OtherV().Next();
            //graph.g().V().Both().InE().Next();
            //graph.g().V().OutE().BothV().Next();
            //graph.g().V().InE().BothV().Next();
            //graph.g().V().BothE().BothV().Next();
            //graph.g().V().InE().FlatMap(GraphTraversal2.__().BothV().InE()).Next();
            //graph.g().V().InE().FlatMap(GraphTraversal2.__().BothV().OutE()).Next();
            //graph.g().V().InE().FlatMap(GraphTraversal2.__().OutV().OutE()).Next();
            //graph.g().V().InE().FlatMap(GraphTraversal2.__().InV().OutE()).Next();
            //graph.g().V().FlatMap(GraphTraversal2.__().InE().OutV()).Next();
            //graph.g().V().FlatMap(GraphTraversal2.__().InE().InV()).Next();
            //graph.g().V().FlatMap(GraphTraversal2.__().InE().BothV()).Next();
            //graph.g().V().FlatMap(GraphTraversal2.__().InE().OtherV()).Next();
            //graph.g().V().FlatMap(GraphTraversal2.__().OutE().InV()).Next();
            //graph.g().V().FlatMap(GraphTraversal2.__().OutE().OutV()).Next();
            //graph.g().V().FlatMap(GraphTraversal2.__().OutE().BothV()).Next();
            //graph.g().V().FlatMap(GraphTraversal2.__().OutE().OtherV()).Next();
            //graph.g().V().FlatMap(GraphTraversal2.__().In().OutE()).Next();
            //graph.g().V().FlatMap(GraphTraversal2.__().OutE().OutE()).Next();
            //graph.g().V().FlatMap(GraphTraversal2.__().Both().OutE()).Next();

            //graph.g().V().OutE().FlatMap(GraphTraversal2.__().InV().InE().Drop()).Next();
            //graph.g().V().BothE().FlatMap(GraphTraversal2.__().InV().InE().OutV()).Next();
            //graph.g().V().BothE().FlatMap(GraphTraversal2.__().InV().InE().InV()).Next();
            ////graph.g().V().BothE().Drop().Next();
            //graph.g().V().FlatMap(GraphTraversal2.__().Both().In()).Next();

            //graph.g().V().HasLabel("person").Optional(GraphTraversal2.__().Out()).Next();
            //graph.g().V().OutE().FlatMap(GraphTraversal2.__().InV().InE().OutV()).Next();
            //graph.g().V().HasLabel("test").OutE().InV().HasLabel("test1").Next();
            //var results = graph.g().V().Out().Has("type", "University").InE("between").Drop().Next();
            //graph.g().V().Out().Optional(GraphTraversal2.__().Out().In()).Values("name").Next();
            //graph.g().V().OutE().FlatMap(GraphTraversal2.__().InV().Out()).Next();

            //graph.g().V().Dedup().Next();

            //var results = graph.g().V().Has("type", "University")
            //    .Project("info", "edge_label")
            //    .By(GraphTraversal2.__().Properties("label", "type"))
            //    .By(GraphTraversal2.__().OutE().Properties("label")).Next();

            //var results =
            //    graph.g()
            //        .V()
            //        .HasLabel("Stanford")
            //        .Union(graph.g().V().OutE(), GraphTraversal2.__().V().In())
            //        .Next();

            //graph.g().V().AddE().From(graph.g().V()).Next();
            //graph.g().V().AddE().To(graph.g().V()).From(graph.g().V()).Next();

            //graph.g().V().As("@v")
            //    .FlatMap(GraphTraversal2.__().Out("mdl").OutE("ref"))
            //    .Repeat(GraphTraversal2.__().As("@e")
            //        .FlatMap(GraphTraversal2.__().InV()
            //            .As("mdl")
            //            .Select(GremlinKeyword.Pop.last, "@v")
            //            .Both()
            //            .Where(GraphTraversal2.__().Out("mdl")
            //                .Where(Predicate.eq("mdl"))))
            //        .As("@v")
            //        .Optional(GraphTraversal2.__().FlatMap(
            //            GraphTraversal2.__().Select(GremlinKeyword.Pop.last, "@e")
            //                .Values("_ref")
            //                .As("key")
            //                .Select(GremlinKeyword.Pop.last, "@v")
            //                .Out("mdl")
            //                .OutE("ref")
            //                .Where(GraphTraversal2.__().Values("_key")
            //                    .Where(Predicate.eq("key")))))
            //                    )
            //    .Until(GraphTraversal2.__().FlatMap(
            //        GraphTraversal2.__().As("res").Select(GremlinKeyword.Pop.last, "@v").Where(Predicate.eq("res"))))
            //    .Next();

            graph.g().V()
                .Project("vertex", "parents", "references", "model")
                .By(GraphTraversal2.__().Emit().Repeat(GraphTraversal2.__().OutE("_val").As("_").InV()).Tree())
                .By(GraphTraversal2.__().OutE().Label().Dedup().Fold())
                .By(GraphTraversal2.__().As("@v")
                    .FlatMap(GraphTraversal2.__().Out("mdl").OutE("ref"))
                    .Repeat(GraphTraversal2.__().As("@e")
                        .FlatMap(GraphTraversal2.__().InV()
                            .As("mdl")
                            .Select(GremlinKeyword.Pop.last, "@v")
                            .Both()
                            .Where(GraphTraversal2.__().Out("mdl")
                                .Where(Predicate.eq("mdl"))))
                        .As("@v")
                        .Optional(GraphTraversal2.__().FlatMap(
                            GraphTraversal2.__().Select(GremlinKeyword.Pop.last, "@e")
                                .Values("_ref")
                                .As("key")
                                .Select(GremlinKeyword.Pop.last, "@v")
                                .Out("mdl")
                                .OutE("ref")
                                .Where(GraphTraversal2.__().Values("_key")
                                    .Where(Predicate.eq("key"))))))
                    .Until(GraphTraversal2.__().FlatMap(
                        GraphTraversal2.__().As("res").Select(GremlinKeyword.Pop.last, "@v").Where(Predicate.eq("res"))))
                    .Union(GraphTraversal2.__().Dedup()
                            .Emit()
                            .Repeat(GraphTraversal2.__().OutE("_val").As("_").InV())
                            .Tree(),
                        GraphTraversal2.__().Project("id", "key", "ref")
                            .By(GraphTraversal2.__().Id())
                            .By(GraphTraversal2.__().Select(GremlinKeyword.Pop.first, "@e").Values("_key"))
                            .By(GraphTraversal2.__().Select(GremlinKeyword.Pop.last, "@e").Values("_ref"))
                            .Fold())
                    .Fold())
                .By(GraphTraversal2.__().Out("mdl").Project("vertex").By(GraphTraversal2.__().Tree())).Next();


            //graph.g().V()
            //    .has("_app", "test-app")
            //    .has("_id", "product:soda-machine:shop-2")
            //    .hasLabel("product")
            //    .flatMap(GraphTraversal2.__().As("src")
            //        .flatMap(graph.g().V()
            //            .has("_app", "test-app")
            //            .has("_id", "device:soda-mixer:shop-1")
            //            .hasLabel("device"))
            //        .As("tgt")
            //        .select("src")
            //        .coalesce(GraphTraversal2.__().inE("device-product"),
            //                     GraphTraversal2.__().inE("device-product")
            //            //GraphTraversal2.__().addE("device-product").from("tgt")
            //            )
            //    )
            //    .count()
            //    .next();
        }


        [TestMethod]
        public void InsertMarvelData()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelUniverse");

            GraphViewCommand graph = new GraphViewCommand(connection);

            var results = graph.g().V();
            foreach (var result in results)
            {
                Console.WriteLine(result);
            }

            //var results = graph.g().V().Has("type", "University").Union(GraphTraversal2.__().Properties("label", "type"), GraphTraversal2.__().OutE().Properties("label")).next();

            //Insert character
            //string[] characterLines = File.ReadAllLines(@"C:\Users\v-jinjl\Desktop\GraphView-Development\GraphView\data\character.txt");
            //foreach (string line in characterLines)
            //{
            //    var arr = line.Split('\"');
            //    var id = arr[0].Substring(0, arr[0].Length - 1);
            //    var name = arr[1];
            //    graph.g().AddV("character").Property("id", id).Property("name", name).next();
            //}

            //Insert comicbook
            //string[] comicbookLines = File.ReadAllLines(@"C:\Users\v-jinjl\Desktop\GraphView-Development\GraphView\data\comicbook.txt");
            //foreach (string line in comicbookLines)
            //{
            //    var arr = line.Split('\"');
            //    var id = arr[0].Substring(0, arr[0].Length - 1);
            //    var name = arr[1];
            //    graph.g().AddV("comicbook").Property("id", id).Property("name", name).next();
            //}

            //Insert Edge
            //string[] edgeLines = File.ReadAllLines(@"C:\Users\v-jinjl\Desktop\GraphView-Development\GraphView\data\edge.txt");
            //foreach (string line in edgeLines)
            //{
            //    var arr = line.Split(' ');
            //    var sourceId = arr[0];
            //    for (var i = 1; i < arr.Length; i++)
            //    {
            //        var sinkId = arr[i];
            //        graph.g().V().Has("id", sourceId).AddE("appeared").To(graph.g().V().Has("id", sinkId)).next();
            //    }
            //}
        }
    }
}