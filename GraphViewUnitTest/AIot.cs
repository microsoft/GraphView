using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
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
            else t = new GraphTraversal(ref connection);
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
            List<string> Prop = new List<string>();
            Prop.Add("type");
            Prop.Add(info.type);
            foreach (var x in info.properties)
            {
                Prop.Add(x.Key);
                Prop.Add(x.Value);
            }
            var vertex = g.V().coalesce(_find(connection, info), GraphTraversal._underscore().addV(Prop));
            return vertex;
        }

        [TestMethod]
        internal GraphTraversal _edge(GraphViewConnection connection, NodeInfo Src, NodeInfo Dest)
        {
            GraphTraversal g = new GraphTraversal(ref connection);
            _node(connection,Src).Invoke();
            _node(connection,Dest).Invoke();
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
        internal GraphTraversal Delete(GraphViewConnection connection, Info info)
        {
            return _find(connection,info).drop();
        }

        [TestMethod]
        internal GraphTraversal getDeviceModelInformation(GraphViewConnection connection, string DeviceID)
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
                    .As("telemetryDataModel")
                    .@select("device", "DeviceModel", "telemetryDataModel");
        }

    }
}
