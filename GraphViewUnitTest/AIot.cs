using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewUnitTest
{
    internal class Info
    {
        internal string id;
        internal string type;
        internal Dictionary<string, string> properties;
    }

    internal class NodeInfo:Info
    {
        internal List<EdgeInfo> edges;
    }

    internal enum direction
    {
        In,
        Out
    }
    internal class EdgeInfo : Info
    {
        internal string name;
        internal direction dir;
        internal NodeInfo target;
    }

    [TestClass]
    public class AIot
    {
        [TestMethod]
        public void ResetCollection(string collection)
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", collection);
            connection.SetupClient();
            connection.DocDB_finish = false;
            connection.BuildUp();

            while (!connection.DocDB_finish)
                System.Threading.Thread.Sleep(10);

            connection.ResetCollection();
            connection.DocDB_finish = false;
            connection.BuildUp();

            while (!connection.DocDB_finish)
                System.Threading.Thread.Sleep(10);
        }

        [TestMethod]
        internal GraphTraversal _find(GraphViewConnection connection, Info info, GraphTraversal source = null)
        {
            GraphTraversal t;
            if (source != null) t = source;
            else
            {
                t = new GraphTraversal(ref connection);
                t = t.V();
            }
            if (info.id != null)
                t = t.has("id", info.id);
            if (info.type != null)
                t = t.has("type", info.type);
            if (info.properties != null && info.id == null)
                t = info.properties.Aggregate(t, (current, prop) => current.has(prop.Key, prop.Value));
            return t;
        }

        [TestMethod]
        internal GraphTraversal _node(GraphViewConnection connection, NodeInfo info)
        {
            GraphTraversal g = new GraphTraversal(ref connection);

            List<string> PropList = new List<string>();
            PropList.Add("type");
            PropList.Add(info.type);
            foreach(var x in info.properties)
            {
                PropList.Add(x.Key);
                PropList.Add(x.Value);
            }
            var source = g.coalesce(_find(connection, info), GraphTraversal._underscore().addV(PropList));
            foreach(var x in source) { }
            foreach (var edge in info.edges)
            {
                PropList = new List<string>();
                PropList.Add("type");
                PropList.Add(edge.target.type);
                foreach (var x in edge.target.properties)
                {
                    PropList.Add(x.Key);
                    PropList.Add(x.Value);
                }
                var sink = g.coalesce(_find(connection, edge.target), GraphTraversal._underscore().addV(PropList));
            foreach(var x in sink) { }
                GraphTraversal EdgeInsert = null;
                if (edge.dir == direction.In)
                    EdgeInsert = _find(connection, info).addE("name", edge.name).@from(_find(connection, edge.target));
                else EdgeInsert = _find(connection, info).addE("name", edge.name).to(_find(connection, edge.target));
            }
            return null;
        }

        [TestMethod]
        internal GraphTraversal _edge(GraphViewConnection connection, NodeInfo Src, NodeInfo Dest)
        {
            GraphTraversal g = new GraphTraversal(ref connection);
            _node(connection,Src);
            _node(connection,Dest);
            g = g.V().As("V");
            if (Src.id != null)
                g = g.has("id", Src.id);
            if (Src.type != null)
                g = g.has("type", Src.type);
            if (Src.properties != null && Src.id == null)
                g = Src.properties.Aggregate(g, (current, prop) => current.has(prop.Key, prop.Value));
            g = g.As("a").@select("v");
            if (Dest.id != null)
                g = g.has("id", Dest.id);
            if (Dest.type != null)
                g = g.has("type", Dest.type);
            if (Dest.properties != null && Dest.id == null)
                g = Src.properties.Aggregate(g, (current, prop) => current.has(prop.Key, prop.Value));
            return g;
        }

        [TestMethod]
        internal GraphTraversal _root(GraphViewConnection connection, NodeInfo info)
        {
            GraphTraversal g = new GraphTraversal(ref connection);
            GraphTraversal t = null;
            return t = _find(connection, info).repeat(GraphTraversal._underscore().Out());
        }

        [TestMethod]
        internal GraphTraversal Delete(GraphViewConnection connection, Info info)
        {
            return _find(connection,info).drop();
        }

        [TestMethod]
        internal GraphTraversal getDeviceInformation(GraphViewConnection connection, string DeviceID)
        {
            GraphTraversal g = new GraphTraversal(ref connection);
            return
                g.V()
                    .has("label", "DeviceTwin")
                    .has("id", DeviceID)
                    .As("device")
                    .Out("type_of")
                    .As("DeviceModel")
                    .In("extends")
                    .@select("device", "DeviceModel");
        }

        [TestMethod]
        internal GraphTraversal getDeviceModelInformation(GraphViewConnection connection, string manufacturer,
            string modelNumber)
        {
            var modelInfo = new NodeInfo()
            {
                type = "DeviceModel",
                properties = new Dictionary<string, string>()
                {
                    {"manufacturer", manufacturer},
                    {"modelNumber", modelNumber}
                }
            };


            return
                _find(connection, modelInfo)
                    .As("deviceModel")
                    .In("extends")
                    .As("telemetryDataModel")
                    .@select("deviceModel", "telemetryDataModel");
        }

        [TestMethod]
        internal GraphTraversal _path(GraphViewConnection connection, NodeInfo src, NodeInfo target)
        {
            GraphTraversal g = new GraphTraversal(ref connection);
            return _find(connection, src).repeat(_find(connection, target, GraphTraversal._underscore().Out())).path();
        }
        //    [TestMethod]

        //    public void Test1()
        //    {
        //        GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
        //"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
        //"GroupMatch", "IoT2");
        //        ResetCollection("IoT2");
        //        NodeInfo A = new NodeInfo() {type = "Model", properties = new Dictionary<string, string>() {{"name", "A"}}};
        //        NodeInfo B = new NodeInfo() { type = "Model", properties = new Dictionary<string, string>() { { "name", "B" } } };
        //        NodeInfo C = new NodeInfo()
        //        {
        //            type = "Prototype",
        //            edges =
        //                new List<EdgeInfo>()
        //                {
        //                    new EdgeInfo() {dir = direction.Out, type = "derived", target = A},
        //                    new EdgeInfo() {dir = direction.Out, type = "derived", target = B}
        //                },
        //            properties = new Dictionary<string, string>() { { "name", "C" } }
        //        };
        //        _node(connection,C);
        //        var root = _root(connection,C);
        //        foreach (var x in root)
        //        {
        //            var y = x;
        //        }
        //    }
        //    [TestMethod]

        //    public void Test2()
        //    {
        //        GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
        //"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
        //"GroupMatch", "IoT3");
        //        ResetCollection("IoT3");
        //        GraphTraversal g = new GraphTraversal(ref connection);
        //        var A = g.V().addV("name", "A", "gender","man");
        //        var B = g.V().addV("name", "B", "gender", "man");
        //        var C = g.V().addV("name", "C", "gender", "woman");
        //        var D = g.V().addV("name", "D", "gender", "man");
        //        g.V().has("name", "A").addE("type", "father").to(g.V().has("name", "B"));
        //        g.V().has("name", "B").addE("type", "father").to(g.V().has("name", "C"));
        //        g.V().has("name", "C").addE("type", "father").to(g.V().has("name", "D"));
        //        NodeInfo a = new NodeInfo() {properties = new Dictionary<string, string>() {{"name", "A"}}};
        //        NodeInfo c = new NodeInfo() {properties = new Dictionary<string, string>() {{"gender", "man"}}};
        //        var root = _path(connection, a, c);
        //        foreach (var x in root)
        //        {
        //            var y = x;
        //        }
        //    }
    }
    }
