using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json.Linq;
using System.Collections.ObjectModel;

// Add DocumentDB references

namespace GraphView
{
    internal abstract class FieldObject
    {
        public static VertexPropertyField GetVertexPropertyField(JProperty property)
        {
            return new VertexPropertyField(property.Name, property.Value.ToString());
        }

        public static EdgePropertyField GetEdgePropertyField(JProperty property)
        {
            return new EdgePropertyField(property.Name, property.Value.ToString());
        }

        public static VertexField GetVertexField(JObject vertexObject)
        {
            VertexField vertexField = new VertexField();

            string vertexId = null; 
            string vertexLabel = null;

            JArray forwardAdjList = null;
            JArray backwardAdjList = null;

            foreach (var property in vertexObject.Properties())
            {
                switch (property.Name.ToLower())
                {
                    // DocumentDB-reserved JSON properties
                    case "_rid":
                    case "_self":
                    case "_etag":
                    case "_attachments":
                    case "_ts":
                        continue;
                    case GremlinKeyword.NodeID:
                        vertexId = property.Value.ToString();
                        vertexField.VertexProperties.Add(property.Name, GetVertexPropertyField(property));
                        break;
                    case GremlinKeyword.Label:
                        vertexLabel = property.Value.ToString();
                        vertexField.VertexProperties.Add(property.Name, GetVertexPropertyField(property));
                        break;
                    case GremlinKeyword.EdgeAdj:
                        // vertexField.AdjacencyList = GetAdjacencyListField((JArray)property.Value);
                        forwardAdjList = (JArray)property.Value;
                        break;
                    case GremlinKeyword.ReverseEdgeAdj:
                        //vertexField.RevAdjacencyList = GetAdjacencyListField((JArray)property.Value);
                        backwardAdjList = (JArray)property.Value;
                        break;
                    default:
                        vertexField.VertexProperties.Add(property.Name, GetVertexPropertyField(property));
                        break;
                }
            }

            if (forwardAdjList != null)
            {
                vertexField.AdjacencyList = GetForwardAdjacencyListField(vertexId, vertexLabel, forwardAdjList);
            }
            if (backwardAdjList != null)
            {
                vertexField.RevAdjacencyList = GetBackwardAdjacencyListField(vertexId, vertexLabel, backwardAdjList);
            }

            return vertexField;
        }

        public static EdgeField GetEdgeField(JObject edgeObject)
        {
            EdgeField edgeField = new EdgeField();

            foreach (JProperty property in edgeObject.Properties())
            {
                edgeField.EdgeProperties.Add(property.Name, GetEdgePropertyField(property));
            }

            return edgeField;
        }

        public static EdgeField GetForwardEdgeField(string inVId, string inVLabel, JObject edgeObject)
        {
            EdgeField edgeField = new EdgeField();

            edgeField.InV = inVId;
            edgeField.InVLabel = inVLabel;

            foreach (JProperty property in edgeObject.Properties())
            {
                edgeField.EdgeProperties.Add(property.Name, GetEdgePropertyField(property));

                if (property.Name == "_sink")
                {
                    edgeField.OutV = property.Value.ToString();
                }
                else if (property.Name == "_sinkLabel")
                {
                    edgeField.OutVLabel = property.Value.ToString();
                }
            }

            return edgeField;
        }

        public static EdgeField GetBackwardEdgeField(string outVId, string outVLabel, JObject edgeObject)
        {
            EdgeField edgeField = new EdgeField();

            edgeField.OutV = outVId;
            edgeField.OutVLabel = outVLabel;

            foreach (JProperty property in edgeObject.Properties())
            {
                edgeField.EdgeProperties.Add(property.Name, GetEdgePropertyField(property));

                if (property.Name == "_sink")
                {
                    edgeField.InV = property.Value.ToString();
                }
                else if (property.Name == "_sinkLabel")
                {
                    edgeField.InVLabel = property.Value.ToString();
                }
            }

            return edgeField;
        }

        public static AdjacencyListField GetAdjacencyListField(JArray adjArray)
        {
            AdjacencyListField adjListField = new AdjacencyListField();

            foreach (var edgeObject in adjArray.Children<JObject>())
            {
                adjListField.Edges.Add(edgeObject["_ID"].ToString(), GetEdgeField(edgeObject));
            }

            return adjListField;
        }

        public static AdjacencyListField GetForwardAdjacencyListField(
            string inVId, string inVLabel, JArray adjArray)
        {
            AdjacencyListField adjListField = new AdjacencyListField();

            foreach (var edgeObject in adjArray.Children<JObject>())
            {
                adjListField.Edges.Add(edgeObject["_ID"].ToString(), GetForwardEdgeField(inVId, inVLabel, edgeObject));
            }

            return adjListField;
        }

        public static AdjacencyListField GetBackwardAdjacencyListField(
            string outVId, string outVLabel, JArray adjArray)
        {
            AdjacencyListField adjListField = new AdjacencyListField();

            foreach (var edgeObject in adjArray.Children<JObject>())
            {
                adjListField.Edges.Add(edgeObject["_ID"].ToString(), GetBackwardEdgeField(outVId, outVLabel, edgeObject));
            }

            return adjListField;
        }


        public virtual string ToGraphSON()
        {
            return ToString();
        }

        public virtual string ToValue
        {
            get
            {
                return ToString();
            }
        }
    }

    internal class StringField : FieldObject
    {
        public string Value { get; set; }

        public StringField(string value)
        {
            Value = value;
        }

        public override string ToString()
        {
            return Value;
        }

        public override int GetHashCode()
        {
            return Value.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            if (Object.ReferenceEquals(this, obj)) return true;

            StringField stringField = obj as StringField;
            if (stringField == null)
            {
                return false;
            }

            return Value.Equals(stringField.Value);
        }

        public override string ToValue
        {
            get
            {
                return Value;
            }
        }
    }

    internal class CollectionField : FieldObject
    {
        public List<FieldObject> Collection { get; set; }

        public CollectionField()
        {
            Collection = new List<FieldObject>();
        }

        public CollectionField(List<FieldObject> collection)
        {
            Collection = collection;
        }

        public override string ToString()
        {
            if (Collection.Count == 0) return "[]";

            var collectionStringBuilder = new StringBuilder("[");
            collectionStringBuilder.Append(Collection[0].ToString());

            for (var i = 1; i < Collection.Count; i++)
                collectionStringBuilder.Append(", ").Append(Collection[i].ToString());

            collectionStringBuilder.Append(']');
            
            return collectionStringBuilder.ToString();
        }

        public override string ToGraphSON()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("[");

            for (int i = 0; i < Collection.Count; i++)
            {
                if (i > 0)
                {
                    sb.Append(", ");
                }
                sb.Append(Collection[i].ToGraphSON());
            }

            sb.Append("]");

            return sb.ToString();
        }

        public override bool Equals(object obj)
        {
            if (Object.ReferenceEquals(this, obj)) return true;

            CollectionField colField = obj as CollectionField;
            if (colField == null || Collection.Count != colField.Collection.Count)
            {
                return false;
            }

            for (int i = 0; i < Collection.Count; i++)
            {
                if (!Collection[i].Equals(colField.Collection[i]))
                {
                    return false;
                }
            }

            return true;
        }

        public override int GetHashCode()
        {
            return ToString().GetHashCode();
        }
    }

    internal class MapField : FieldObject
    {
        public Dictionary<FieldObject, FieldObject> Map { get; set; }

        public MapField()
        {
            Map = new Dictionary<FieldObject, FieldObject>();
        }

        public MapField(Dictionary<FieldObject, FieldObject> map)
        {
            Map = map;
        }

        public override string ToString()
        {
            if (Map.Count == 0) return "[]";

            var mapStringBuilder = new StringBuilder("[");
            var i = 0;

            foreach (var pair in Map)
            {
                var key = pair.Key;
                var value = pair.Value;

                if (i++ > 0)
                    mapStringBuilder.Append(", ");
                mapStringBuilder.Append(key.ToString()).Append(":[").Append(value.ToString()).Append(']');
            }

            mapStringBuilder.Append(']');

            return mapStringBuilder.ToString();
        }

        public override string ToGraphSON()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("[");

            bool firstEntry = true;
            foreach (var entry in Map)
            {
                if (firstEntry)
                {
                    firstEntry = false;
                }
                else
                {
                    sb.Append(", ");
                }

                sb.AppendFormat("{0}: [{1}]", entry.Key.ToGraphSON(), entry.Value.ToGraphSON());
            }

            sb.Append("]");
            return sb.ToString();
        }

        public override bool Equals(object obj)
        {
            if (Object.ReferenceEquals(this, obj)) return true;

            MapField mapField = obj as MapField;
            if (mapField == null || Map.Count != mapField.Map.Count)
            {
                return false;
            }

            foreach (var kvp in Map)
            {
                var key = kvp.Key;
                FieldObject value2;
                if (!mapField.Map.TryGetValue(key, out value2))
                    return false;
                if (!kvp.Value.Equals(value2))
                    return false;
            }

            return true;
        }

        public override int GetHashCode()
        {
            return ToString().GetHashCode();
        }
    }

    internal class PropertyField : FieldObject
    {
        public string PropertyName { get; private set; }
        public string PropertyValue { get; set; }

        public PropertyField(string propertyName, string propertyValue)
        {
            PropertyName = propertyName;
            PropertyValue = propertyValue;
        }

        public override string ToString()
        {
            return string.Format("{0}->{1}", PropertyName, PropertyValue);
        }

        public override bool Equals(object obj)
        {
            if (Object.ReferenceEquals(this, obj)) return true;

            PropertyField pf = obj as PropertyField;
            if (pf == null)
            {
                return false;
            }

            return PropertyName == pf.PropertyName && PropertyValue == pf.PropertyValue;
        }

        public override int GetHashCode()
        {
            return ToString().GetHashCode();
        }

        public override string ToValue
        {
            get
            {
                return PropertyValue;
            }
        }

        public override string ToGraphSON()
        {
            return string.Format("{\"{0}\": \"{1}\"}", PropertyName, PropertyValue);
        }
    }

    internal class VertexPropertyField : PropertyField
    {
        public VertexPropertyField(string propertyName, string propertyValue) 
            : base(propertyName, propertyValue)
        {
        }

        public override string ToString()
        {
            return string.Format("vp[{0}]", base.ToString());
        }
    }

    internal class EdgePropertyField : PropertyField
    {
        public EdgePropertyField(string propertyName, string propertyValue) 
            : base(propertyName, propertyValue)
        {
        }

        public override string ToString()
        {
            return string.Format("p[{0}]", base.ToString());
        }
    }


    internal class EdgeField : FieldObject
    {
        // <PropertyName, EdgePropertyField>
        public Dictionary<string, EdgePropertyField> EdgeProperties;

        public string Label;
        public string InVLabel;
        public string OutVLabel;
        public string InV;
        public string OutV;

        public EdgeField()
        {
            EdgeProperties = new Dictionary<string, EdgePropertyField>();
        }

        public FieldObject this[string propertyName]
        {
            get
            {
                if (propertyName.Equals("*", StringComparison.OrdinalIgnoreCase))
                    return this;
                EdgePropertyField propertyField;
                EdgeProperties.TryGetValue(propertyName, out propertyField);
                return propertyField;
            }
        }

        public void UpdateEdgeProperty(string propertyName, string propertyValue)
        {
            EdgePropertyField propertyField;
            if (EdgeProperties.TryGetValue(propertyName, out propertyField))
                propertyField.PropertyValue = propertyValue;
            else
                EdgeProperties.Add(propertyName, new EdgePropertyField(propertyName, propertyValue));
        }

        public override string ToString()
        {
            return string.Format("e[{0}]{1}({2})-{3}->{4}({5})", EdgeProperties[GremlinKeyword.EdgeID].ToValue, InV, InVLabel, Label, OutV, OutVLabel);
        }

        public override string ToGraphSON()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("{{\"id\": {0}", EdgeProperties[GremlinKeyword.EdgeID].ToValue);
            if (Label != null)
            {
                sb.AppendFormat(", \"label\": \"{0}\"", Label);
            }

            sb.Append(", \"type\": \"edge\"");

            if (OutVLabel != null)
            {
                sb.AppendFormat(", \"inVLabel\": \"{0}\"", OutVLabel);
            }
            if (InVLabel != null)
            {
                sb.AppendFormat(", \"outVLabel\": \"{0}\"", InVLabel);
            }
            if (OutV != null)
            {
                sb.AppendFormat(", \"inV\": \"{0}\"", OutV);
            }
            if (InV != null)
            {
                sb.AppendFormat(", \"outV\": \"{0}\"", InV);
            }

            bool firstProperty = true;
            foreach (string propertyName in EdgeProperties.Keys)
            {
                switch(propertyName)
                {
                    case GremlinKeyword.Label:
                    case GremlinKeyword.SinkLabel:
                    case GremlinKeyword.EdgeID:
                    case GremlinKeyword.EdgeReverseID:
                    case GremlinKeyword.EdgeSourceV:
                    case GremlinKeyword.EdgeSinkV:
                    case GremlinKeyword.EdgeOtherV:
                        continue;
                    default:
                        break;
                }

                if (firstProperty)
                {
                    sb.Append(", \"properties\": {");
                    firstProperty = false;
                }
                else
                {
                    sb.Append(", ");
                }

                sb.AppendFormat("\"{0}\": \"{1}\"", propertyName, EdgeProperties[propertyName].PropertyValue);
            }
            if (!firstProperty)
            {
                sb.Append("}");
            }
            sb.Append("}");

            return sb.ToString();
        }
    }

    internal class AdjacencyListField : FieldObject
    {
        // <edgeOffset, EdgeField>
        public Dictionary<string, EdgeField> Edges { get; set; }

        public AdjacencyListField()
        {
            Edges = new Dictionary<string, EdgeField>();
        }

        public void InsertEdgeField(string edgeOffset, EdgeField edgeField)
        {
            Edges.Add(edgeOffset, edgeField);
        }

        public EdgeField GetEdgeFieldByOffset(string edgeOffset)
        {
            EdgeField edgeField;
            Edges.TryGetValue(edgeOffset, out edgeField);
            return edgeField;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();

            foreach (string offset in Edges.Keys.OrderBy(e => long.Parse(e)))
            {
                if (sb.Length > 0)
                {
                    sb.Append(", ");
                }
                sb.Append(Edges[offset].ToString());
            }

            return string.Format("[{0}]", sb.ToString());
        }

        public override string ToGraphSON()
        {
            StringBuilder sb = new StringBuilder();

            foreach (string offset in Edges.Keys.OrderBy(e => long.Parse(e)))
            {
                if (sb.Length > 0)
                {
                    sb.Append(", ");
                }
                sb.Append(Edges[offset].ToGraphSON());
            }

            return string.Format("[{0}]", sb.ToString());
        }
    }

    internal class VertexField : FieldObject
    {
        // <Property Name, VertexPropertyField>
        public Dictionary<string, VertexPropertyField> VertexProperties { get; set; }
        public AdjacencyListField AdjacencyList { get; set; }
        public AdjacencyListField RevAdjacencyList { get; set; }

        public FieldObject this[string propertyName]
        {
            get
            {
                if (propertyName.Equals("*", StringComparison.OrdinalIgnoreCase))
                    return this;
                else if (propertyName.Equals("_edge", StringComparison.OrdinalIgnoreCase))
                    return AdjacencyList;
                else if (propertyName.Equals("_reverse_edge", StringComparison.OrdinalIgnoreCase))
                    return RevAdjacencyList;
                else
                {
                    VertexPropertyField propertyField;
                    VertexProperties.TryGetValue(propertyName, out propertyField);
                    return propertyField;
                }
            }
        }

        public void UpdateVertexProperty(string propertyName, string propertyValue)
        {
            VertexPropertyField propertyField;
            if (VertexProperties.TryGetValue(propertyName, out propertyField))
                propertyField.PropertyValue = propertyValue;
            else
                VertexProperties.Add(propertyName, new VertexPropertyField(propertyName, propertyValue));
        }

        public VertexField()
        {
            VertexProperties = new Dictionary<string, VertexPropertyField>();
            AdjacencyList = new AdjacencyListField();
            RevAdjacencyList = new AdjacencyListField();
        }

        public override string ToString()
        {
            VertexPropertyField idProperty;
            string id;
            if (VertexProperties.TryGetValue("id", out idProperty))
            {
                id = idProperty.ToValue;
            }
            else
            {
                id = "";
            }
            return string.Format("v[{0}]", id);
        }

        public override string ToValue
        {
            get
            {
                VertexPropertyField idProperty;
                if (VertexProperties.TryGetValue("id", out idProperty))
                {
                    return idProperty.ToValue;
                }
                else
                {
                    return "";
                }
            }
        }

        public override string ToGraphSON()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("{");
            sb.AppendFormat("\"id\": \"{0}\"", VertexProperties["id"].PropertyValue);

            if (VertexProperties.ContainsKey("label"))
            {
                sb.Append(", ");
                sb.AppendFormat("\"label\": \"{0}\"", VertexProperties["label"].PropertyValue);
            }

            if (RevAdjacencyList != null && RevAdjacencyList.Edges.Count > 0)
            {
                sb.Append(", inE: {");
                // Groups incoming edges by their labels
                var groupByLabel = RevAdjacencyList.Edges.Values.GroupBy(e => e["label"].ToValue);
                bool firstInEGroup = true;
                foreach (var g in groupByLabel)
                {
                    if (firstInEGroup)
                    {
                        firstInEGroup = false;
                    }
                    else
                    {
                        sb.Append(", ");
                    }

                    string edgelLabel = g.Key;
                    sb.AppendFormat("\"{0}\": [", edgelLabel);

                    bool firstInEdge = true;
                    foreach (EdgeField edgeField in g)
                    {
                        if (firstInEdge)
                        {
                            firstInEdge = false;
                        }
                        else
                        {
                            sb.Append(", ");
                        }

                        sb.Append("{");
                        sb.AppendFormat("\"id\": {0}, ", 
                            edgeField.EdgeProperties[GremlinKeyword.EdgeID].ToValue);
                        sb.AppendFormat("\"outV\": \"{0}\"", edgeField.InV);

                        bool firstInEProperty = true;
                        foreach (string propertyName in edgeField.EdgeProperties.Keys)
                        {
                            switch(propertyName)
                            {
                                case GremlinKeyword.EdgeID:
                                case GremlinKeyword.EdgeReverseID:
                                case GremlinKeyword.EdgeSourceV:
                                case GremlinKeyword.EdgeSinkV:
                                case GremlinKeyword.EdgeOtherV:
                                case GremlinKeyword.Label:
                                case GremlinKeyword.SinkLabel:
                                    continue;
                                default:
                                    break;
                            }

                            if (firstInEProperty)
                            {
                                sb.Append(", \"properties\": {");
                                firstInEProperty = false;
                            }
                            else
                            {
                                sb.Append(", ");
                            }

                            sb.AppendFormat("\"{0}\": \"{1}\"", 
                                propertyName, 
                                edgeField.EdgeProperties[propertyName].PropertyValue);
                        }
                        if (!firstInEProperty)
                        {
                            sb.Append("}");
                        }
                        sb.Append("}");
                    }
                    sb.Append("]");
                }
                sb.Append("}");
            }

            if (AdjacencyList != null && AdjacencyList.Edges.Count > 0)
            {
                sb.Append(", outE: {");
                // Groups outgoing edges by their labels
                var groupByLabel = AdjacencyList.Edges.Values.GroupBy(e => e["label"].ToValue);
                bool firstOutEGroup = true;
                foreach (var g in groupByLabel)
                {
                    if (firstOutEGroup)
                    {
                        firstOutEGroup = false;
                    }
                    else
                    {
                        sb.Append(", ");
                    }

                    string edgelLabel = g.Key;
                    sb.AppendFormat("\"{0}\": [", edgelLabel);

                    bool firstOutEdge = true;
                    foreach (EdgeField edgeField in g)
                    {
                        if (firstOutEdge)
                        {
                            firstOutEdge = false;
                        }
                        else
                        {
                            sb.Append(", ");
                        }

                        sb.Append("{");
                        sb.AppendFormat("\"id\": {0}, ", 
                            edgeField.EdgeProperties[GremlinKeyword.EdgeID].ToValue);
                        sb.AppendFormat("\"inV\": \"{0}\"", edgeField.OutV);

                        bool firstOutEProperty = true;
                        foreach (string propertyName in edgeField.EdgeProperties.Keys)
                        {
                            switch (propertyName)
                            {
                                case GremlinKeyword.EdgeID:
                                case GremlinKeyword.EdgeReverseID:
                                case GremlinKeyword.EdgeSourceV:
                                case GremlinKeyword.EdgeSinkV:
                                case GremlinKeyword.EdgeOtherV:
                                case GremlinKeyword.Label:
                                case GremlinKeyword.SinkLabel:
                                    continue;
                                default:
                                    break;
                            }

                            if (firstOutEProperty)
                            {
                                sb.Append(", \"properties\": {");
                                firstOutEProperty = false;
                            }
                            else
                            {
                                sb.Append(", ");
                            }

                            sb.AppendFormat("\"{0}\": \"{1}\"",
                                propertyName,
                                edgeField.EdgeProperties[propertyName].PropertyValue);
                        }
                        if (!firstOutEProperty)
                        {
                            sb.Append("}");
                        }
                        sb.Append("}");
                    }
                    sb.Append("]");
                }
                sb.Append("}");
            }

            bool firstVertexProperty = true;
            foreach (string propertyName in VertexProperties.Keys)
            {
                switch(propertyName)
                {
                    case GremlinKeyword.EdgeAdj:
                    case GremlinKeyword.ReverseEdgeAdj:
                    case "_nextEdgeOffset":
                    case "_nextReverseEdgeOffset":
                    case "id":
                    case "label":
                        continue;
                    default:
                        break;
                }

                if (firstVertexProperty)
                {
                    sb.Append(", \"properties\": {");
                    firstVertexProperty = false;
                }
                else
                {
                    sb.Append(", ");
                }

                VertexPropertyField vp = VertexProperties[propertyName];
                sb.AppendFormat("\"{0}\": [{{\"value\": \"{0}\"}}]", propertyName, vp.PropertyValue);
            }
            if (!firstVertexProperty)
            {
                sb.Append("}");
            }

            sb.Append("}");

            return sb.ToString();
        }
    }

    /// <summary>
    /// RawRecord is a data sturcture representing data records flowing from one execution operator to another. 
    /// A data record is a multi-field blob. Each field is currently represented as a string.
    /// The interpretation of a record, i.e., the names of the fields/columns of the record, 
    /// is specified in the data operator producing them.  
    /// 
    /// The fields of a record produced by an execution operator are in two parts: 
    /// the first part contains k triples, each representing a node processed so far. 
    /// A triple describes: 1) the node ID, 2) the node's adjacency list, and 3) the node's reverse adjacency list.
    /// The second part is a list of node/edge properties of the processed nodes, projected by the SELECT clause. 
    /// 
    /// | node1 | node1_adjacency_list | node1_rev_adjacency_list |...| nodeK | nodeK_adjacency_list | nodeK_rev_adjacency_list | property1 | property2 |......
    /// </summary>
    internal class RawRecord
    {
        internal RawRecord()
        {
            fieldValues = new List<FieldObject>();
        }
        internal RawRecord(RawRecord rhs)
        {
            fieldValues = new List<FieldObject>(rhs.fieldValues);
        }
        internal RawRecord(int num)
        {
            fieldValues = new List<FieldObject>();
            for (int i = 0; i < num; i++)
            {
                fieldValues.Add(new StringField(""));
            }
        }

        public void Append(FieldObject fieldValue)
        {
            fieldValues.Add(fieldValue);
        }

        public void Append(RawRecord record)
        {
            fieldValues.AddRange(record.fieldValues);
        }

        public int Length
        {
            get
            {
                return fieldValues.Count;
            }
        }

        internal FieldObject RetriveData(List<string> header,string FieldName)
        {
            if (header.IndexOf(FieldName) == -1) return null;
            else if (fieldValues.Count <= header.IndexOf(FieldName)) return null;
            else return fieldValues[header.IndexOf(FieldName)];
        }
        internal FieldObject RetriveData(int index)
        {
            return fieldValues[index];
        }

        internal FieldObject this[int index]
        {
            get
            {
                return fieldValues[index];
            }
        }

        //internal int RetriveIndex(string value)
        //{
        //    if (fieldValues.IndexOf(value) == -1) return -1;
        //    else return fieldValues.IndexOf(value);
        //}
        //internal String RetriveRow()
        //{
        //    String row = "";
        //    if (fieldValues == null) return row;
        //    for(int i = 0; i < fieldValues.Count; i++)
        //    {
        //        row += fieldValues[i].ToString() + ",";
        //    }
        //    return row;
        //}
        internal List<FieldObject> fieldValues;
    }

    /// <summary>
    /// Record differs from RawRecord in that the field names of the blob is annotated. 
    /// It is hence comprehensible to external data readers.  
    /// </summary>
    //public class Record
    //{
    //    RawRecord rawRecord;

    //    internal Record(RawRecord rhs, List<string> pHeader)
    //    {
    //        if (rhs != null)
    //        {
    //            rawRecord = rhs;
    //            header = pHeader;
    //        }
    //    }
    //    internal List<string> header { get; set; }
    //    public string this[int index]
    //    {
    //        get
    //        {
    //            if (index >= rawRecord.fieldValues.Count)
    //                throw new IndexOutOfRangeException("Out of range," + "the Record has only " + rawRecord.fieldValues.Count + " fields");
    //            else return rawRecord.fieldValues[index].ToString();
    //        }
    //    }

    //    public string this[string FieldName]
    //    {
    //        get
    //        {
    //            if (header == null || header.IndexOf(FieldName) == -1) 
    //                throw new IndexOutOfRangeException("Out of range," + "the Record has no field \"" + FieldName + "\".");
    //            else return rawRecord.fieldValues[header.IndexOf(FieldName)].ToString();
    //        }
    //    }
    //}

    internal enum GraphViewEdgeTableReferenceEnum
    {
        BothE,
        OutE,
        InE
    }

    internal enum GraphViewVertexTableReferenceEnum
    {
        Both,
        OutV,
        InV
    }

    internal class GraphViewReservedProperties
    {
        internal static readonly ReadOnlyCollection<string> ReservedNodeProperties = 
            new ReadOnlyCollection<string>(new List<string> { "id", "label", "_edge", "_reverse_edge", "*" });

        internal static readonly ReadOnlyCollection<string> ReservedEdgeProperties =
            new ReadOnlyCollection<string>(new List<string> {"_source", "_sink", "_other", "_ID", "*"});
    }

    /// <summary>
    /// The interface of query execution operators.
    /// An operator is in one of the states: open or closed. 
    /// By implementing Next(), a query execution operator implements its own computation logic 
    /// and returns result iteratively. 
    /// </summary>
    internal interface IGraphViewExecution
    {
        bool State();
        void Open();
        void Close();
        RawRecord Next();
    }
    /// <summary>
    /// The base class for all query execution operators. 
    /// The class implements the execution interface and specifies the field names of 
    /// the raw records produced by this operator. 
    /// </summary>
    internal abstract class GraphViewExecutionOperator : IGraphViewExecution
    {
        private bool state;
        public bool State()
        {
            return state;
        }
        public void Open()
        {
            state = true;
        }
        public void Close()
        {
            state = false;
        }
        public virtual void ResetState()
        {
            this.Open();
        }
        public abstract RawRecord Next();

        protected Dictionary<WColumnReferenceExpression, int> privateRecordLayout;

        // Number of vertices processed so far
        internal int NumberOfProcessedVertices;

        internal static IQueryable<dynamic> SendQuery(string script, GraphViewConnection connection)
        {
            FeedOptions QueryOptions = new FeedOptions { MaxItemCount = -1 };
            IQueryable<dynamic> Result = connection.DocDBclient.CreateDocumentQuery(
                UriFactory.CreateDocumentCollectionUri(connection.DocDB_DatabaseId, connection.DocDB_CollectionId), 
                script, QueryOptions);
            return Result;
        }
    }
}
