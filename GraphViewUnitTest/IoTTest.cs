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
    public class IoTTest
    {
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

        internal GraphTraversal _root(GraphViewConnection connection, NodeInfo src ,NodeInfo target = null)
        {
            GraphTraversal g = new GraphTraversal(ref connection);
            GraphTraversal t = null;
            if (target == null) return _find(connection, src).repeat(GraphTraversal._underscore().Out());
            else return  _find(connection, src).repeat(_find(connection, target, GraphTraversal._underscore().Out()));
        }

        internal GraphTraversal _delete(GraphViewConnection connection, Info info)
        {
            return _find(connection,info).drop();
        }

        internal GraphTraversal getDeviceInformation(GraphViewConnection connection, string DeviceID)
        {
            GraphTraversal g = new GraphTraversal(ref connection);
            return
                g.V()
                    .has("label", "DeviceTwin")
                    .has("id", DeviceID)
                    .As("device")
                    .In("outE.type_of")
                    .As("DeviceModel")
                    .Out("outE.extends")
                    .@select("device", "DeviceModel");
        }


        internal GraphTraversal getDeviceModelInformation(GraphViewConnection connection, string manufacturer,
            string modelNumber)
        {
            var modelInfo = new NodeInfo()
            {
                properties = new Dictionary<string, string>()
                {
                    {"label","DeviceModel" },
                    {"properties_manufacturer_value", manufacturer}
                }
            };
            return
                _find(connection, modelInfo)
                    .As("deviceModel")
                    .Out("outE.extends")
                    .As("telemetryDataModel")
                    .@select("deviceModel", "telemetryDataModel");
        }

        internal GraphTraversal _path(GraphViewConnection connection, GraphTraversal src)
        {
            return src.path();
        }


        /// <summary>
        /// Insert a Node whose name is "A", with model name "M1" and system "S1", then find and delete it. 
        /// </summary>
        [TestMethod]
        public void IoTDeleteNodeTest()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
"GroupMatch", "IoTDeleteInsert");
            ResetCollection("IoTDeleteInsert");
            GraphTraversal g = new GraphTraversal(ref connection);
            var A = g.V().addV("name", "A", "Model", "M1", "System", "S1");
            NodeInfo node = new NodeInfo()
            {
                properties = new Dictionary<string, string>()
                {
                    {"name","A"}
                }
            };
            _delete(connection, node);
        }

        /// <summary>
        /// Print a device with given ID, its model and extension.
        /// </summary>
        [TestMethod]
        public void IoTGetDeviceInformationTest()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
"GroupMatch", "IOTTest");
            string DeviceID = "70646";
            var device = getDeviceInformation(connection, DeviceID);
            foreach (var x in device)
                Console.WriteLine(x);
        }

        /// <summary>
        /// Print the NodeId of the device model with given manufacturer, and its extend telemetry data model. 
        /// </summary>
        [TestMethod]
        public void IoTGetDeviceModelInformationTest()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
"GroupMatch", "IoTTest");
            string Manufacturer = "DeviceModel-90794721-59a2-11e6-8cd0-3717b83c0677";
            string ModelNumber = null;
            var device = getDeviceModelInformation(connection,Manufacturer,ModelNumber);
            foreach (var x in device)
                Console.WriteLine(x);
        }

        /// <summary>
        /// Insert four nodes with different name/model/system, and connected as A->B->C->D
        /// Start from A, finding the last node within system S1. 
        /// </summary>
        [TestMethod]
        public void IoTFindRootTest()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
    "GroupMatch", "IoTRoot");
            ResetCollection("IoTRoot");
            GraphTraversal g = new GraphTraversal(ref connection);
            var A = g.V().addV("name", "A", "Model", "M1", "System", "S1");
            var B = g.V().addV("name", "B", "Model", "M2", "System", "S1");
            var C = g.V().addV("name", "C", "Model", "M3", "System", "S1");
            var D = g.V().addV("name", "D", "Model", "M4", "System", "S2");
            g.V().has("name", "A").addE("type", "develop").to(g.V().has("name", "B"));
            g.V().has("name", "B").addE("type", "develop").to(g.V().has("name", "C"));
            g.V().has("name", "C").addE("type", "develop").to(g.V().has("name", "D"));
            NodeInfo a = new NodeInfo() { properties = new Dictionary<string, string>() { { "name", "A" } } };
            NodeInfo c = new NodeInfo() { properties = new Dictionary<string, string>() { { "System", "S1" } } };
            var root = _root(connection, a, c).values("name");
            foreach (var x in root)
            {
                var y = x[0];
            }
        }
        /// <summary>
        /// Insert four nodes with different name/model/system, and connected as A->B->C->D
        /// Start from A, finding the last node within system S1, and giving the path from A to this node. 
        /// </summary>
        [TestMethod]
        public void IoTFindRootWithPathTest()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
"GroupMatch", "IoTDeleteInsert");
            ResetCollection("IoTDeleteInsert");
            GraphTraversal g = new GraphTraversal(ref connection);
            var A = g.V().addV("name", "A", "Model", "M1", "System", "S1");
            var B = g.V().addV("name", "B", "Model", "M2", "System", "S1");
            var C = g.V().addV("name", "C", "Model", "M3", "System", "S1");
            var D = g.V().addV("name", "D", "Model", "M4", "System", "S2");
            g.V().has("name", "A").addE("type", "develop").to(g.V().has("name", "B"));
            g.V().has("name", "B").addE("type", "develop").to(g.V().has("name", "C"));
            g.V().has("name", "C").addE("type", "develop").to(g.V().has("name", "D"));
            NodeInfo a = new NodeInfo() { properties = new Dictionary<string, string>() { { "name", "A" } } };
            NodeInfo c = new NodeInfo() { properties = new Dictionary<string, string>() { { "System", "S1" } } };
            var root = _root(connection, a, c).path();
            foreach (var x in root)
            {
                var y = x[0];
            }
        }
        [TestMethod]

        /// <summary>
        /// Add node C to the collection with all its edges and the nodes that it connected with.
        /// </summary>
        public void IoTAddNodeTest()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
    "GroupMatch", "IoT2");
            ResetCollection("IoT2");
            NodeInfo A = new NodeInfo() { type = "Model", properties = new Dictionary<string, string>() { { "name", "A" } } };
            NodeInfo B = new NodeInfo() { type = "Model", properties = new Dictionary<string, string>() { { "name", "B" } } };
            NodeInfo C = new NodeInfo()
            {
                type = "Prototype",
                edges =
                    new List<EdgeInfo>()
                    {
                            new EdgeInfo() {dir = direction.Out, type = "derived", target = A},
                            new EdgeInfo() {dir = direction.Out, type = "derived", target = B}
                    },
                properties = new Dictionary<string, string>() { { "name", "C" } }
            };
            _node(connection, C);
        }

    }
    }
