using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewUnitTest
{


    [TestClass]
    public class IoTTest
    {
        //public class Info
        //{
        //    internal string id;
        //    internal string type;
        //    internal Dictionary<string, string> properties;
        //}

        //public class NodeInfo : Info
        //{
        //    internal List<EdgeInfo> edges;
        //}

        //public enum direction
        //{
        //    In,
        //    Out
        //}
        //public class EdgeInfo : Info
        //{
        //    internal string name;
        //    internal direction dir;
        //    internal NodeInfo target;
        //}

        //public static GraphTraversal _find(GraphViewConnection connection, Info info, GraphTraversal source = null)
        //{
        //    GraphTraversal t;
        //    if (source != null) t = source;
        //    else
        //    {
        //        t = new GraphTraversal(connection);
        //        t = t.V();
        //    }
        //    if (info.id != null)
        //        t = t.has("id", info.id);
        //    if (info.type != null)
        //        t = t.has("type", info.type);
        //    if (info.properties != null && info.id == null)
        //        t = info.properties.Aggregate(t, (current, prop) => current.has(prop.Key, prop.Value));
        //    return t;
        //}

        //public static GraphTraversal _node(GraphViewConnection connection, NodeInfo info)
        //{
        //    GraphTraversal g = new GraphTraversal(connection);

        //    List<string> PropList = new List<string>();
        //    PropList.Add("type");
        //    PropList.Add(info.type);
        //    foreach (var x in info.properties)
        //    {
        //        PropList.Add(x.Key);
        //        PropList.Add(x.Value);
        //    }
        //    var source = g.coalesce(_find(connection, info), GraphTraversal._underscore().addV(PropList));
        //    foreach (var x in source) { }
        //    foreach (var edge in info.edges)
        //    {
        //        PropList = new List<string>();
        //        PropList.Add("type");
        //        PropList.Add(edge.target.type);
        //        foreach (var x in edge.target.properties)
        //        {
        //            PropList.Add(x.Key);
        //            PropList.Add(x.Value);
        //        }
        //        var sink = g.coalesce(_find(connection, edge.target), GraphTraversal._underscore().addV(PropList));
        //        foreach (var x in sink) { }
        //        GraphTraversal EdgeInsert = null;
        //        PropList.Clear();
        //        if (edge.type != null)
        //        {
        //            PropList.Add("type");
        //            PropList.Add(edge.type);
        //        }
        //        if (edge.properties != null)
        //            foreach (var x in edge.properties)
        //            {
        //                PropList.Add(x.Key);
        //                PropList.Add(x.Value);
        //            }
        //        if (edge.dir == direction.In)
        //            EdgeInsert = _find(connection, info).addE(PropList).@from(_find(connection, edge.target));
        //        else EdgeInsert = _find(connection, info).addE(PropList).to(_find(connection, edge.target));
        //    }
        //    return null;
        //}

        //public static GraphTraversal _edge(GraphViewConnection connection, NodeInfo Src, NodeInfo Dest)
        //{
        //    GraphTraversal g = new GraphTraversal(connection);
        //    _node(connection, Src);
        //    _node(connection, Dest);
        //    g = g.V().As("V");
        //    if (Src.id != null)
        //        g = g.has("id", Src.id);
        //    if (Src.type != null)
        //        g = g.has("type", Src.type);
        //    if (Src.properties != null && Src.id == null)
        //        g = Src.properties.Aggregate(g, (current, prop) => current.has(prop.Key, prop.Value));
        //    g = g.As("a").@select("v");
        //    if (Dest.id != null)
        //        g = g.has("id", Dest.id);
        //    if (Dest.type != null)
        //        g = g.has("type", Dest.type);
        //    if (Dest.properties != null && Dest.id == null)
        //        g = Src.properties.Aggregate(g, (current, prop) => current.has(prop.Key, prop.Value));
        //    return g;
        //}

        //public static GraphTraversal _root(GraphViewConnection connection, NodeInfo src, NodeInfo target = null)
        //{
        //    if (target == null) return _find(connection, src).repeat(GraphTraversal._underscore().Out());
        //    else return _find(connection, src).repeat(_find(connection, target, GraphTraversal._underscore().Out()));
        //}

        //public static GraphTraversal _delete(GraphViewConnection connection, Info info)
        //{
        //    return _find(connection, info).drop();
        //}

        //public static GraphTraversal getDeviceInformation(GraphViewConnection connection, string DeviceID)
        //{
        //    GraphTraversal g = new GraphTraversal(connection);
        //    return
        //        g.V()
        //            .has("label", "DeviceModel")
        //            .has("id", DeviceID)
        //            .As("DeviceModel")
        //            .Out("type_of")
        //            .has("label", "DeviceTwin")
        //            .As("device")
        //            .@select("device", "DeviceModel");
        //}


        //public static GraphTraversal getDeviceModelInformation(GraphViewConnection connection, string manufacturer,
        //    string modelNumber)
        //{
        //    var modelInfo = new NodeInfo()
        //    {
        //        properties = new Dictionary<string, string>()
        //        {
        //            {"label","DeviceModel" },
        //            {"properties_manufacturer_value", manufacturer}
        //        }
        //    };
        //    return
        //        _find(connection, modelInfo)
        //            .As("deviceModel")
        //            .Out("extends")
        //            .As("telemetryDataModel")
        //            .@select("deviceModel", "telemetryDataModel");
        //}

        //public static GraphTraversal _path(GraphViewConnection connection, GraphTraversal src)
        //{
        //    return src.path();
        //}

        /// <summary>
        /// Insert a Node whose name is "A", with model name "M1" and system "S1", then find and delete it. 
        /// </summary>
//        [TestMethod]
//        public void IoTDeleteNodeTest()
//        {
//            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
//"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
//"GroupMatch", "IoTDeleteInsert");
//            connection.ResetCollection();
//            GraphViewCommand graph = new GraphViewCommand(connection);

//            var A = graph.g().V().AddV("name", "A", "Model", "M1", "System", "S1").Next();
//            graph.g().V().Has("name", "A").Drop().Next();
//        }

        /// <summary>
        /// Print a device with given ID, its model and extension.
        /// </summary>
//        [TestMethod]
//        public void IoTGetDeviceInformationTest()
//        {
//            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
//"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
//"GroupMatch", "IOTTest");
//            string DeviceID = "25015";
//            var device = getDeviceInformation(connection, DeviceID);
//            foreach (var x in device)
//                Console.WriteLine(x);
//        }

        /// <summary>
        /// Print the NodeId of the device model with given manufacturer, and its extend telemetry data model. 
//        /// </summary>
//        [TestMethod]
//        public void IoTGetDeviceModelInformationTest()
//        {
//            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
//"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
//"GroupMatch", "IOTTest");
//            string Manufacturer = "DeviceModel-90792012-59a2-11e6-8cd0-3717b83c0677";
//            string ModelNumber = null;
//            var device = getDeviceModelInformation(connection, Manufacturer, ModelNumber);
//            foreach (var x in device)
//                Console.WriteLine(x);
//        }

        /// <summary>
        /// Start from Node 26419, finding all the root node of it.
        /// </summary>
//        [TestMethod]
//        public void IoTFindRootTest()
//        {
//            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
//"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
//"GroupMatch", "NewTest1");
//            //connection.ResetCollection();
//            GraphViewCommand graph = new GraphViewCommand(connection);

//            var A26419 = graph.g().V().AddV("id", "26419", "label", "DeviceModel", "manufacturer", "DeviceModel-907d3ece-59a2-11e6-8cd0-3717b83c0677").Next();
//            var A102 = graph.g().V().AddV("id", "102", "label", "TelemetryDataModel", "name", "DataModel-906e98e6-59a2-11e6-8cd0-3717b83c0677").Next();
//            var A104 = graph.g().V().AddV("id", "104", "label", "Measure", "name", "Measure-906e98e7-59a2-11e6-8cd0-3717b83c0677").Next();
//            var E102_104 = graph.g().V().Has("id", "102").AddE("shown_as").To(graph.g().V().Has("id", "104")).Next();
//            var E26419_102 = graph.g().V().Has("id", "26419").AddE("extends").To(graph.g().V().Has("id", "102")).Next();
//            var A12807 = graph.g().V().AddV("id", "12807", "label", "TelemetryDataModel", "name", "DataModel-90721bf2-59a2-11e6-8cd0-3717b83c0677").Next();
//            var A12809 = graph.g().V().AddV("id", "12809", "label", "Measure", "name", "Measure-906e98e7-59a2-11e6-8cd0-3717b83c0677").Next();
//            var E12807_12809 = graph.g().V().Has("id", "12807").AddE("shown_as").To(graph.g().V().Has("id", "12809")).Next();
//            var E26419_12807 = graph.g().V().Has("id", "26419").AddE("extends").To(graph.g().V().Has("id", "12807")).Next();

//            var root = graph.g().V().Has("name", "A").Repeat(GraphTraversal2.__().Out()).Next();
//            //var root = g.V().has("name", "A").repeat(GraphTraversal._underscore().Out()).until(GraphTraversal._underscore().has("name", "E")).path();
//            foreach (var x in root)
//            {
//                var y = x[0];
//                Console.WriteLine(y);
//            }
//        }

        /// <summary>
        /// Start from Node 26419, finding all the root node of it with path.
        /// </summary>
//        [TestMethod]
//        public void IoTFindRootWithPathTest()
//        {
//            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
//"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
//"GroupMatch", "IoTRoot");
//            //ResetCollection("IoTRoot");
//            GraphViewCommand graph = new GraphViewCommand(connection);

//            var A26419 = graph.g().V().AddV("id", "26419", "label", "DeviceModel", "manufacturer", "DeviceModel-907d3ece-59a2-11e6-8cd0-3717b83c0677").Next();
//            var A102 = graph.g().V().AddV("id", "102", "label", "TelemetryDataModel", "name", "DataModel-906e98e6-59a2-11e6-8cd0-3717b83c0677").Next();
//            var A104 = graph.g().V().AddV("id", "104", "label", "Measure", "name", "Measure-906e98e7-59a2-11e6-8cd0-3717b83c0677").Next();
//            var E102_104 = graph.g().V().Has("id", "102").AddE("shown_as").To(graph.g().V().Has("id", "104")).Next();
//            var E26419_102 = graph.g().V().Has("id", "26419").AddE("extends").To(graph.g().V().Has("id", "102")).Next();
//            var A12807 = graph.g().V().AddV("id", "12807", "label", "TelemetryDataModel", "name", "DataModel-90721bf2-59a2-11e6-8cd0-3717b83c0677").Next();
//            var A12809 = graph.g().V().AddV("id", "12809", "label", "Measure", "name", "Measure-906e98e7-59a2-11e6-8cd0-3717b83c0677").Next();
//            var E12807_12809 = graph.g().V().Has("id", "12807").AddE("shown_as").To(graph.g().V().Has("id", "12809")).Next();
//            var E26419_12807 = graph.g().V().Has("id", "26419").AddE("extends").To(graph.g().V().Has("id", "12807")).Next();

//            var root = graph.g().V().Has("name", "A").Repeat(GraphTraversal2.__().Out()).path().Next();
//            foreach (var x in root)
//            {
//                var y = x[0];
//            }
//        }
    //    [TestMethod]
    //    /// <summary>
    //    /// Add node N232 to the collection with all its edges and the nodes that it connected with.
    //    /// </summary>
    //    public void IoTAddNodeTest()
    //    {
    //        GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
    //"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
    //"GroupMatch", "IoTDeleteInsert");
    //        connection.ResetCollection();

    //        NodeInfo N234 = new NodeInfo() { properties = new Dictionary<string, string>() { { "label", "Measure" }, { "id", "234" }, { "name", "Measure-906e991b-59a2-11e6-8cd0-3717b83c0677" } } };
    //        NodeInfo N29477 = new NodeInfo() { properties = new Dictionary<string, string>() { { "label", "DeviceModel" }, { "id", "29477" }, { "manufacturer", "DeviceModel-90810f5d-59a2-11e6-8cd0-3717b83c0677" } } };

    //        NodeInfo N232 = new NodeInfo()
    //        {
    //            edges =
    //                new List<EdgeInfo>()
    //                {
    //                        new EdgeInfo() {dir = direction.Out, type = "shown_as", target = N234},
    //                        new EdgeInfo() {dir = direction.In, type = "extends", target = N29477}
    //                },
    //            properties = new Dictionary<string, string>() { { "label", "TelemetryDataModel" }, { "id", "232" }, { "name", "DataModel-906e991a-59a2-11e6-8cd0-3717b83c0677" } }
    //        };

    //        GraphTraversal g = new GraphTraversal(connection);

    //        List<string> PropList = new List<string>();
    //        PropList.Add("type");
    //        PropList.Add(N232.type);
    //        foreach (var x in N232.properties)
    //        {
    //            PropList.Add(x.Key);
    //            PropList.Add(x.Value);
    //        }
    //        var source = g.coalesce(_find(connection, N232), GraphTraversal._underscore().addV(PropList));
    //        foreach (var x in source) { }
    //        foreach (var edge in N232.edges)
    //        {
    //            PropList = new List<string>();
    //            PropList.Add("type");
    //            PropList.Add(edge.target.type);
    //            foreach (var x in edge.target.properties)
    //            {
    //                PropList.Add(x.Key);
    //                PropList.Add(x.Value);
    //            }
    //            var sink = g.coalesce(_find(connection, edge.target), GraphTraversal._underscore().addV(PropList));
    //            foreach (var x in sink) { }
    //            GraphTraversal EdgeInsert = null;
    //            PropList.Clear();
    //            if (edge.type != null)
    //            {
    //                PropList.Add("type");
    //                PropList.Add(edge.type);
    //            }
    //            if (edge.properties != null)
    //                foreach (var x in edge.properties)
    //                {
    //                    PropList.Add(x.Key);
    //                    PropList.Add(x.Value);
    //                }
    //            if (edge.dir == direction.In)
    //                EdgeInsert = _find(connection, N232).addE(PropList).@from(_find(connection, edge.target));
    //            else EdgeInsert = _find(connection, N232).addE(PropList).to(_find(connection, edge.target));
    //        }
    //    }
    }
}
