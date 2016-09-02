using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Newtonsoft.Json;
using System.Text;
using Newtonsoft.Json.Linq;
using System.IO;
namespace GraphViewUnitTest
{
    [TestClass]
    public class TinkPopJsonParser
    {
        [TestMethod]
        public void ResetCollection(String collectionName)
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", collectionName);
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
        public void parseJson()
        {
            int i = 0;
            var lines = File.ReadLines(@"D:\dataset\AzureIOT\graphson-dataset.json");
            int index = 0;
            var nodePropertiesHashMap = new Dictionary<string, Dictionary<string, string>>();
            var outEdgePropertiesHashMap = new Dictionary<string, Dictionary<string, string>>();
            var inEdgePropertiesHashMap = new Dictionary<string, Dictionary<string, string>>();

            foreach (var line in lines)
            {
                JObject root = JObject.Parse(line);
                var nodeIdJ = root["id"];
                var nodeLabelJ = root["label"];
                var nodePropertiesJ = root["properties"];
                var nodeOutEJ = root["outE"];
                var nodeInEJ = root["inE"];

                // parse nodeId
                var nodeIdV = nodeIdJ.First.Next.ToString();
                // parse label
                var nodeLabelV = nodeLabelJ.ToString();
                // parse node properties
                foreach (var property in nodePropertiesJ.Children())
                {
                    if (property.HasValues && property.First.HasValues && property.First.First.Next != null)
                    {
                        var tempPChild = property.First.First.Next.Children();
                        foreach (var child1Properties in tempPChild)
                        {
                            // As no API to get the properties name, make it not general
                            var id = child1Properties["id"].Last;
                            if (id != null)
                            {
                                if (id != null)
                                {
                                    var node = new Dictionary<String, String>();
                                    nodePropertiesHashMap[id.ToString()] = node;
                                    nodePropertiesHashMap[id.ToString()]["id"] = id.ToString();
                                }
                                var value = child1Properties["value"];
                                if (value != null)
                                {
                                    nodePropertiesHashMap[id.ToString()]["value"] = value.ToString();
                                }
                                var label = nodeLabelJ.ToString();
                                if (label != null)
                                {
                                    nodePropertiesHashMap[id.ToString()]["label"] = label.ToString();
                                }
                            }
                        }
                    }
                }
                // parse outE
                var nString = nodeOutEJ.ToString();
                if (nodeOutEJ.HasValues && nodeOutEJ.ToString().Contains("extends"))
                {
                    var tempE = nodeOutEJ.First.Root;
                    foreach (var outEdge in nodeOutEJ.First.First.Last.Children())
                    {
                        var id = outEdge["id"].First.Next;
                        var inV = outEdge["inV"].First.Next;
                        var edgeString = inV + "_" + nodeIdJ.Last();
                        var dic = new Dictionary<string, string>();
                        outEdgePropertiesHashMap[edgeString] = dic;
                        outEdgePropertiesHashMap[edgeString].Add("id", id.ToString());
                    }
                }
                // parse inE
                var inString = nodeInEJ.ToString();
                if (nodeInEJ.HasValues && nodeInEJ.ToString().Contains("shown_as"))
                {
                    var tempE = nodeInEJ.First.Root;
                    foreach (var inEdge in nodeInEJ.First.First.Last.Children())
                    {
                        var id = inEdge["id"].First.Next;
                        var outV = inEdge["outV"].First.Next;
                        var edgeString = outV + "_" + nodeIdJ.Last();
                        var dic = new Dictionary<string, string>();
                        inEdgePropertiesHashMap[edgeString] = dic;
                        inEdgePropertiesHashMap[edgeString].Add("id", id.ToString());
                    }
                }
            }

            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            ResetCollection("MarvelTest");
            // Insert node from collections
            foreach (var node in nodePropertiesHashMap)
            {
                StringBuilder tempSQL = new StringBuilder("g.addV(");
                tempSQL.Append("\'id\',");
                tempSQL.Append("\'" + node.Key + "\',");
                tempSQL.Append("\'" + "value" + "\',");
                tempSQL.Append("\'" + node.Value["value"] + "\',");
                tempSQL.Append("\'" + "label" + "\',");
                tempSQL.Append("\'" + node.Value["label"] + "\'");
                tempSQL.Append(")");
                parser.Parse(tempSQL.ToString()).Generate(connection).Next();
            }
            // Insert out edge from collections
            foreach (var edge in outEdgePropertiesHashMap)
            {
                String[] nodeIds = edge.Key.Split('_');
                String srcId = nodeIds[0];
                String desId = nodeIds[1];
                if(nodePropertiesHashMap.ContainsKey(srcId) && nodePropertiesHashMap.ContainsKey(desId))
                {
                    //// Insert Src Node
                    //StringBuilder tempSQLSrc = new StringBuilder("g.addV(");
                    //tempSQLSrc.Append("\'id\',");
                    //tempSQLSrc.Append("\'" + srcId + "\',");
                    //tempSQLSrc.Append("\'" + "value" + "\',");
                    //tempSQLSrc.Append("\'" + nodePropertiesHashMap[srcId]["value"] + "\'");
                    //tempSQLSrc.Append("\'" + "label" + "\',");
                    //tempSQLSrc.Append("\'" + nodePropertiesHashMap[srcId]["label"] + "\'");
                    //tempSQLSrc.Append(")");
                    //parser.Parse(tempSQLSrc.ToString()).Generate(connection).Next();
                    //// Insert Des Node
                    //StringBuilder tempSQLDes = new StringBuilder("g.addV(");
                    //tempSQLDes.Append("\'id\',");
                    //tempSQLDes.Append("\'" + desId + "\',");
                    //tempSQLDes.Append("\'" + "value" + "\',");
                    //tempSQLDes.Append("\'" + nodePropertiesHashMap[srcId]["value"] + "\'");
                    //tempSQLSrc.Append("\'" + "label" + "\',");
                    //tempSQLSrc.Append("\'" + nodePropertiesHashMap[srcId]["label"] + "\'");
                    //tempSQLDes.Append(")");
                    //parser.Parse(tempSQLDes.ToString()).Generate(connection).Next();
                    // Inset Edge
                    StringBuilder edgePropertyList = new StringBuilder(",");
                    edgePropertyList.Append("'id',");
                    edgePropertyList.Append("'" + edge.Value["id"].ToString() + "'");
                    String tempInsertSQL = "g.V.as('v').has('id','" + srcId + "').as('a').select('v').has('id','" + desId + "').as('b').select('a','b').addOutE('a','extends','b'" + edgePropertyList.ToString() + ")";
                    parser.Parse(tempInsertSQL).Generate(connection).Next();
                }
            }
            // Insert in edge from collections
            foreach (var edge in inEdgePropertiesHashMap)
            {
                String[] nodeIds = edge.Key.Split('_');
                String srcId = nodeIds[0];
                String desId = nodeIds[1];
                if (nodePropertiesHashMap.ContainsKey(srcId) && nodePropertiesHashMap.ContainsKey(desId))
                {
                    //// Insert Src Node
                    //StringBuilder tempSQLSrc = new StringBuilder("g.addV(");
                    //tempSQLSrc.Append("\'id\',");
                    //tempSQLSrc.Append("\'" + srcId + "\',");
                    //tempSQLSrc.Append("\'" + "value" + "\',");
                    //tempSQLSrc.Append("\'" + nodePropertiesHashMap[srcId]["value"] + "\'");
                    //tempSQLSrc.Append("\'" + "label" + "\',");
                    //tempSQLSrc.Append("\'" + nodePropertiesHashMap[srcId]["label"] + "\'");
                    //tempSQLSrc.Append(")");
                    //parser.Parse(tempSQLSrc.ToString()).Generate(connection).Next();
                    //// Insert Des Node
                    //StringBuilder tempSQLDes = new StringBuilder("g.addV(");
                    //tempSQLDes.Append("\'id\',");
                    //tempSQLDes.Append("\'" + desId + "\',");
                    //tempSQLDes.Append("\'" + "value" + "\',");
                    //tempSQLDes.Append("\'" + nodePropertiesHashMap[srcId]["value"] + "\'");
                    //tempSQLSrc.Append("\'" + "label" + "\',");
                    //tempSQLSrc.Append("\'" + nodePropertiesHashMap[srcId]["label"] + "\'");
                    //tempSQLDes.Append(")");
                    //parser.Parse(tempSQLDes.ToString()).Generate(connection).Next();
                    // Inset Edge
                    StringBuilder edgePropertyList = new StringBuilder(",");
                    edgePropertyList.Append("'id',");
                    edgePropertyList.Append("'" + edge.Value["id"].ToString() + "'");
                    String tempInsertSQL = "g.V.as('v').has('id','" + srcId + "').as('a').select('v').has('id','" + desId + "').as('b').select('a','b').addInE('a','shown_as','b'" + edgePropertyList.ToString() + ")";
                    parser.Parse(tempInsertSQL).Generate(connection).Next();
                }
            }
        }

    }
}
