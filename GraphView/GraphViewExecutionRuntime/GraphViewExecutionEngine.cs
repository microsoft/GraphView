using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Azure.Documents.Client;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Newtonsoft.Json.Linq;

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

            foreach (var property in vertexObject.Properties())
            {
                switch (property.Name.ToLower())
                {
                    // Reversed properties for DocumentDB meta-data
                    case "_rid":
                    case "_self":
                    case "_etag":
                    case "_attachments":
                    case "_ts":
                        continue;
                    case "_edge":
                        vertexField.AdjacencyList = GetAdjacencyListField((JArray)property.Value);
                        break;
                    case "_reverse_edge":
                        vertexField.RevAdjacencyList = GetAdjacencyListField((JArray)property.Value);
                        break;
                    default:
                        vertexField.VertexProperties.Add(property.Name, GetVertexPropertyField(property));
                        break;
                }
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

        public static AdjacencyListField GetAdjacencyListField(JArray adjArray)
        {
            AdjacencyListField adjListField = new AdjacencyListField();

            foreach (var edgeObject in adjArray.Children<JObject>())
            {
                adjListField.Edges.Add(edgeObject["_ID"].ToString(), GetEdgeField(edgeObject));
            }

            return adjListField;
        }
    }

    //internal class VertexField : FieldObject
    //{
    //    public string VertexJsonString { get; private set; }

    //    public VertexField(string vertexJsonString)
    //    {
    //        VertexJsonString = vertexJsonString;
    //    }

    //    public override string ToString()
    //    {
    //        JObject vertex = JObject.Parse(VertexJsonString);
    //        return string.Format("v[{0}]", vertex["id"].ToString());
    //    }

    //    // TODO: Documents' meta fields like _etag shouldn't be included when executing GetHashCode()
    //    public override int GetHashCode()
    //    {
    //        return VertexJsonString.GetHashCode();
    //    }

    //    // TODO: Documents' meta fields like _etag shouldn't be included when executing Equals()
    //    public override bool Equals(object obj)
    //    {
    //        if (Object.ReferenceEquals(this, obj)) return true;

    //        VertexField vertexField = obj as VertexField;
    //        if (vertexField == null)
    //        {
    //            return false;
    //        }

    //        return VertexJsonString.Equals(vertexField.VertexJsonString);
    //    }
    //}

    //internal class EdgeField : FieldObject
    //{
    //    public string SourceId { get; private set; }
    //    public string SinkId { get; private set; }
    //    public string EdgeId { get; private set; }
    //    public string EdgeJsonString { get; private set; }

    //    public EdgeField(string sourceId, string sinkId, string edgeId, string edgeJsonString)
    //    {
    //        SourceId = sourceId;
    //        SinkId = sinkId;
    //        EdgeId = edgeId;
    //        EdgeJsonString = edgeJsonString;
    //    }

    //    public override string ToString()
    //    {
    //        JObject edge = JObject.Parse(EdgeJsonString);
    //        return string.Format("e[{0}][{1}-{2}->{3}]", 
    //            EdgeId, SourceId, edge["label"] ?? "", SinkId);
    //    }

    //    public override int GetHashCode()
    //    {
    //        return EdgeJsonString.GetHashCode();
    //    }

    //    public override bool Equals(object obj)
    //    {
    //        if (Object.ReferenceEquals(this, obj)) return true;

    //        EdgeField edgeField = obj as EdgeField;
    //        if (edgeField == null)
    //        {
    //            return false;
    //        }

    //        return EdgeJsonString.Equals(edgeField.EdgeJsonString);
    //    }
    //}

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
    }

    internal class VertexPropertyField : PropertyField
    {
        public VertexPropertyField(string propertyName, string propertyValue) 
            : base(propertyName, propertyValue)
        {
        }

        public override string ToString()
        {
            return PropertyValue;
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
            return PropertyValue;
        }
    }


    internal class EdgeField : FieldObject
    {
        // <PropertyName, EdgePropertyField>
        public Dictionary<string, EdgePropertyField> EdgeProperties;

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
            return string.Format("e[{0}]-{1}->{2}", EdgeProperties["_ID"].ToString(), EdgeProperties["label"].ToString(),
                EdgeProperties["_sink"].ToString());
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

        public EdgeField GetEdgeFieldByOffset(string edgeOffset)
        {
            EdgeField edgeField;
            Edges.TryGetValue(edgeOffset, out edgeField);
            return edgeField;
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
            return string.Format("v[{0}]", VertexProperties["id"].ToString());
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
    public class Record
    {
        RawRecord rawRecord;

        internal Record(RawRecord rhs, List<string> pHeader)
        {
            if (rhs != null)
            {
                rawRecord = rhs;
                header = pHeader;
            }
        }
        internal List<string> header { get; set; }
        public string this[int index]
        {
            get
            {
                if (index >= rawRecord.fieldValues.Count)
                    throw new IndexOutOfRangeException("Out of range," + "the Record has only " + rawRecord.fieldValues.Count + " fields");
                else return rawRecord.fieldValues[index].ToString();
            }
        }

        public string this[string FieldName]
        {
            get
            {
                if (header == null || header.IndexOf(FieldName) == -1) 
                    throw new IndexOutOfRangeException("Out of range," + "the Record has no field \"" + FieldName + "\".");
                else return rawRecord.fieldValues[header.IndexOf(FieldName)].ToString();
            }
        }
    }

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

        public List<string> header;     // To be removed. 

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
