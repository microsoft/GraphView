using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json.Linq;
using System.Collections.ObjectModel;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Globalization;
using Newtonsoft.Json;
using static GraphView.GraphViewKeywords;

// Add DocumentDB references

namespace GraphView
{
    internal abstract class FieldObject
    {
        public static VertexField ConstructVertexField(GraphViewConnection connection, JObject vertexObject, Dictionary<string, JObject> edgeDocDict)
        {
            VertexField vertexField = new VertexField(connection);

            string vertexId = (string)vertexObject["id"];
            string vertexLabel = (string)vertexObject[KW_VERTEX_LABEL];

            //
            // "_edge" & KW_VERTEX_REV_EDGE could be either JObject or JArray:
            // - For vertexes that have numerous edges (too large to be filled in one document),
            //     they are JObject indicating the documents storing their in/out edges.
            //     The schema is defined in Schema.txt
            // - For small vertexes, they are JArray directly showing all the edges.
            //
            JToken forwardAdjList = null;
            JToken backwardAdjList = null;

            foreach (JProperty property in vertexObject.Properties()) {

                // For meta-properties
                // "id", "label", "nextEdgeOffset", "partition"
                if (VertexField.IsVertexMetaProperty(property.Name)) {
                    vertexField.VertexMetaProperties.Add(property.Name, new ValuePropertyField(property, vertexField));
                    continue;
                }

                // For other properties
                switch (property.Name) {
                case "_rid":
                case "_self":
                case "_etag":
                case "_attachments":
                case "_ts":
                    continue;

                case KW_VERTEX_EDGE: // "_edge"
                    forwardAdjList = property.Value;
                    break;
                case KW_VERTEX_REV_EDGE: // "_reverse_edge"
                    backwardAdjList = property.Value;
                    break;

                default: // user-defined properties
                    vertexField.VertexProperties.Add(property.Name, new VertexPropertyField(property, vertexField));
                    break;
                }
            }

            Debug.Assert(forwardAdjList != null);
            if (forwardAdjList is JArray) {
                Debug.Assert(edgeDocDict == null, "Small vertexes should not have spilled edge-document");
                vertexField.AdjacencyList = GetForwardAdjacencyListField(vertexId, vertexLabel, (JArray)forwardAdjList);
            }
            else if (forwardAdjList is JObject) {
                Debug.Assert(edgeDocDict != null, "Large vertexes must have spilled edge-document");
                vertexField.AdjacencyList = GetForwardAdjacencyListField(vertexId, vertexLabel, connection, (JObject)forwardAdjList, edgeDocDict);
            }
            else {
                Debug.Assert(false, $"Should not get here! forwardAdjList is: {forwardAdjList.GetType()}");
            }


            Debug.Assert(backwardAdjList != null);
            if (backwardAdjList is JArray) {
                Debug.Assert(edgeDocDict == null, "Small vertexes should not have spilled edge-document");
                vertexField.RevAdjacencyList = GetBackwardAdjacencyListField(vertexId, vertexLabel, (JArray)backwardAdjList);
            }
            else if (backwardAdjList is JObject) {
                Debug.Assert(edgeDocDict != null, "Large vertexes must have spilled edge-document");
                vertexField.RevAdjacencyList = GetBackwardAdjacencyListField(vertexId, vertexLabel, connection, (JObject)backwardAdjList, edgeDocDict);
            }
            else {
                Debug.Assert(false, $"Should not get here! backwardAdjList is: {backwardAdjList.GetType()}");
            }

            return vertexField;
        }



        public static AdjacencyListField GetForwardAdjacencyListField(
            string outVId, string outVLabel, JArray edgeArray)
        {
            AdjacencyListField result = new AdjacencyListField();

            foreach (JObject edgeObject in edgeArray.Children<JObject>()) {
                result.AddEdgeField(outVId, (long)edgeObject[KW_EDGE_OFFSET],
                                    EdgeField.ConstructForwardEdgeField(outVId, outVLabel, null, edgeObject));
            }

            return result;
        }

        public static AdjacencyListField GetBackwardAdjacencyListField(
            string inVId, string inVLabel, JArray edgeArray)
        {
            AdjacencyListField result = new AdjacencyListField();

            foreach (JObject edgeObject in edgeArray.Children<JObject>()) {
                result.AddEdgeField((string)edgeObject[KW_EDGE_SRCV],  // for backward edge, this is the srcVertexId
                                    (long)edgeObject[KW_EDGE_OFFSET],
                                    EdgeField.ConstructBackwardEdgeField(inVId, inVLabel, null, edgeObject));
            }

            return result;
        }


        /// <summary>
        /// For a vertex with lots of edges (thus can't be filled into one document), 
        /// "_edge" is JObject indicating the documents storing its (forward) edges.
        /// For the json schema of <paramref name="edgeContainer"/>, see Schema.txt
        /// </summary>
        /// <param name="outVId"></param>
        /// <param name="outVLabel"></param>
        /// <param name="connection"></param>
        /// <param name="edgeContainer"></param>
        /// <returns></returns>
        public static AdjacencyListField GetForwardAdjacencyListField(
            string outVId, string outVLabel, GraphViewConnection connection, JObject edgeContainer, Dictionary<string, JObject> edgeDocDict)
        {
            AdjacencyListField result = new AdjacencyListField();

            JArray edgeDocuments = (JArray)edgeContainer["_edges"];
            Debug.Assert(edgeDocuments != null, "edgeDocuments != null");

            foreach (JObject edgeDocument in edgeDocuments.Children<JObject>()) {
                string edgeDocID = (string)(JValue)edgeDocument[KW_DOC_ID];
                Debug.Assert(!string.IsNullOrEmpty(edgeDocID), "!string.IsNullOrEmpty(edgeDocID)");

                //
                // Retreive edges from input dictionary: "id" == edgeDocID
                // Check: the metadata is right, and the "_edge" should not be null or empty 
                // (otherwise this edge-document should have been removed)
                //
                JObject edgeDocObject = edgeDocDict[edgeDocID];
                Debug.Assert(edgeDocObject != null, "edgeDocObject != null");
                Debug.Assert((bool)edgeDocObject[KW_EDGEDOC_ISREVERSE] == false, $"(bool)edgeDocObject['{KW_EDGEDOC_ISREVERSE}'] == false");
                Debug.Assert(((string)edgeDocObject[KW_EDGEDOC_VERTEXID]).Equals(outVId), $"((string)edgeDocObject['{KW_EDGEDOC_VERTEXID}']).Equals(outVId)");

                JArray edgesArray = (JArray)edgeDocObject["_edge"];
                Debug.Assert(edgesArray != null, "edgesArray != null");
                Debug.Assert(edgesArray.Count > 0, "edgesArray.Count > 0");
                foreach (JObject edgeObject in edgesArray.Children<JObject>()) {
                    result.AddEdgeField(outVId,
                                        (long)edgeObject[KW_EDGE_OFFSET],
                                        EdgeField.ConstructForwardEdgeField(outVId, outVLabel, edgeDocID, edgeObject));
                }
            }

            return result;
        }


        /// <summary>
        /// For a vertex with lots of edges (thus can't be filled into one document), 
        /// KW_VERTEX_REV_EDGE is JObject indicating the documents storing its (backward) edges.
        /// For the json schema of <paramref name="edgeContainer"/>, see Schema.txt
        /// </summary>
        /// <param name="inVId"></param>
        /// <param name="inVLabel"></param>
        /// <param name="connection"></param>
        /// <param name="edgeContainer"></param>
        /// <param name="edgeDocDict">Set of reverse-edge-documents for spilled vertexes</param>
        /// <returns></returns>
        public static AdjacencyListField GetBackwardAdjacencyListField(
            string inVId, string inVLabel, GraphViewConnection connection, JObject edgeContainer, Dictionary<string, JObject> edgeDocDict)
        {
            AdjacencyListField result = new AdjacencyListField();

            JArray edgeDocuments = (JArray)edgeContainer["_edges"];
            Debug.Assert(edgeDocuments != null, "edgeDocuments != null");

            foreach (JObject edgeDocument in edgeDocuments.Children<JObject>()) {
                string edgeDocID = (string)(JValue)edgeDocument[KW_DOC_ID];
                Debug.Assert(!string.IsNullOrEmpty(edgeDocID), "!string.IsNullOrEmpty(edgeDocID)");

                //
                // Retreive edges from input dictionary: "id" == edgeDocID
                // Check: the metadata is right, and the "_edge" should not be null or empty 
                // (otherwise this edge-document should have been removed)
                //
                JObject edgeDocObject = edgeDocDict[edgeDocID];
                Debug.Assert(edgeDocObject != null, "edgeDocObject != null");
                Debug.Assert((bool)edgeDocObject[KW_EDGEDOC_ISREVERSE] == true, $"(bool)edgeDocObject['{KW_EDGEDOC_ISREVERSE}'] == true");
                Debug.Assert(((string)edgeDocObject[KW_EDGEDOC_VERTEXID]).Equals(inVId), $"((string)edgeDocObject['{KW_EDGEDOC_VERTEXID}']).Equals(outVId)");

                JArray edgesArray = (JArray)edgeDocObject["_edge"];
                Debug.Assert(edgesArray != null, "edgesArray != null");
                Debug.Assert(edgesArray.Count > 0, "edgesArray.Count > 0");
                foreach (JObject edgeObject in edgesArray.Children<JObject>()) {
                    result.AddEdgeField((string)edgeObject[KW_EDGE_SRCV],
                                        (long)edgeObject[KW_EDGE_OFFSET],
                                        EdgeField.ConstructBackwardEdgeField(inVId, inVLabel, edgeDocID, edgeObject));
                }
            }

            return result;
        }

        public virtual string ToGraphSON() => ToString();

        public virtual string ToValue => ToString();

        public virtual Object ToObject() { return this; }

        public static string ToLiteral(string inputString)
        {
            return JsonConvert.ToString(inputString);
        }
    }

    internal class StringField : FieldObject
    {
        public string Value { get; set; }
        public JsonDataType JsonDataType { get; set; }

        public StringField(string value, JsonDataType jsonDataType = JsonDataType.String)
        {
            Value = value;
            JsonDataType = jsonDataType;
        }

        public override string ToString()
        {
            return Value;
        }

        public override string ToGraphSON()
        {
            if (JsonDataType == JsonDataType.String)
                return FieldObject.ToLiteral(this.Value);
            else if (JsonDataType == JsonDataType.Boolean)
                return this.Value.ToLowerInvariant();
            return this.Value;
        }

        public override object ToObject()
        {
            switch (JsonDataType)
            {
                case JsonDataType.Boolean:
                    return bool.Parse(Value);
                case JsonDataType.Int:
                    return int.Parse(Value);
                case JsonDataType.Long:
                    return long.Parse(Value);
                case JsonDataType.Double:
                    return double.Parse(Value);
                case JsonDataType.Float:
                    return float.Parse(Value);
                case JsonDataType.Null:
                    return null;
                default:
                    return Value;
            }
        }

        public override int GetHashCode()
        {
            return Value.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            if (Object.ReferenceEquals(this, obj)) return true;

            StringField rhs = obj as StringField;
            if (rhs == null) {
                return false;
            }

            JsonDataType type1 = this.JsonDataType;
            JsonDataType type2 = rhs.JsonDataType;
            JsonDataType targetType = type1 > type2 ? type1 : type2;

            return ComparisonFunction.Compare(this.Value, rhs.Value, targetType, BooleanComparisonType.Equals);
        }

        public override string ToValue
        {
            get
            {
                return Value;
            }
        }
    }

    internal class PathStepField : FieldObject
    {
        private FieldObject step { get; set; }
        private HashSet<string> labels { get; set; }

        public FieldObject StepFieldObject
        {
            get { return this.step; }
            set { this.step = value; }
        }

        public HashSet<string> Labels => this.labels;

        public PathStepField(FieldObject step)
        {
            this.step = step;
            this.labels = new HashSet<string>();
        }

        public bool AddLabel(string label)
        {
            return this.labels.Add(label);
        }

        public override string ToString()
        {
            return this.step.ToString();
        }

        public override string ToGraphSON()
        {
            return this.step.ToGraphSON();
        }

        public override object ToObject()
        {
            return this.step.ToObject();
        }

        public override int GetHashCode()
        {
            return this.step.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            if (Object.ReferenceEquals(this, obj)) return true;

            PathStepField rhs = obj as PathStepField;
            if (rhs == null || rhs.Labels.Count != this.labels.Count) {
                return false;
            }

            foreach (string label in this.labels) {
                if (!rhs.Labels.Contains(label)) {
                    return false;
                }
            }

            return this.step.Equals(rhs.StepFieldObject);
        }

        public override string ToValue => this.step.ToValue;
    }

    internal class PathField : FieldObject
    {
        public List<FieldObject> Path { get; set; }

        public PathField(List<FieldObject> path)
        {
            this.Path = path;
        }

        public override string ToString()
        {
            if (Path.Count == 0) return "[]";

            StringBuilder pathStringBuilder = new StringBuilder("[");
            pathStringBuilder.Append(Path[0].ToString());

            for (int i = 1; i < Path.Count; i++)
                pathStringBuilder.Append(", ").Append(Path[i].ToString());

            pathStringBuilder.Append(']');

            return pathStringBuilder.ToString();
        }

        public override string ToGraphSON()
        {
            StringBuilder labelsStrBuilder = new StringBuilder();
            StringBuilder objectsStrBuilder = new StringBuilder();

            labelsStrBuilder.Append("\"labels\":[");
            objectsStrBuilder.Append("\"objects\":[");

            bool firstPathStep = true;

            foreach (PathStepField pathStep in this.Path.Cast<PathStepField>())
            {
                HashSet<string> labels = pathStep.Labels;
                FieldObject stepFieldObject = pathStep.StepFieldObject;

                if (firstPathStep) {
                    firstPathStep = false;
                }
                else
                {
                    labelsStrBuilder.Append(", ");
                    objectsStrBuilder.Append(", ");
                }

                StringBuilder labelStringBuilder = new StringBuilder();
                labelStringBuilder.Append("[");
                bool firstLabel = true;
                foreach (string label in labels)
                {
                    if (firstLabel) {
                        firstLabel = false;
                    }
                    else {
                        labelStringBuilder.Append(", ");
                    }
                    labelStringBuilder.Append(string.Format("\"{0}\"", label));
                }
                labelStringBuilder.Append("]");

                labelsStrBuilder.Append(labelStringBuilder.ToString());
                objectsStrBuilder.Append(stepFieldObject.ToGraphSON());
            }

            labelsStrBuilder.Append("], ");
            objectsStrBuilder.Append("]");

            StringBuilder result = new StringBuilder();
            result.Append("{").Append(labelsStrBuilder.ToString()).Append(objectsStrBuilder.ToString()).Append("}");
            return result.ToString();
        }

        public override bool Equals(object obj)
        {
            if (Object.ReferenceEquals(this, obj)) return true;

            PathField pathField = obj as PathField;
            if (pathField == null || Path.Count != pathField.Path.Count) {
                return false;
            }

            for (int i = 0; i < Path.Count; i++)
            {
                if (!Path[i].Equals(pathField.Path[i])) {
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

    internal class MapField : FieldObject, IEnumerable<EntryField>
    {
        private Dictionary<FieldObject, FieldObject> map;
        public List<FieldObject> Order { get; set; } 

        public int Count { get { return map.Count; } }

        public MapField()
        {
            this.map = new Dictionary<FieldObject, FieldObject>();
            this.Order = new List<FieldObject>();
        }

        public MapField(int capacity)
        {
            this.map = new Dictionary<FieldObject, FieldObject>(capacity);
            this.Order = new List<FieldObject>(capacity);
        }

        public void Add(FieldObject key, FieldObject value)
        {
            this.map.Add(key, value);
            this.Order.Add(key);
        }

        public bool Remove(FieldObject key)
        {
            bool isRemoved = this.map.Remove(key);
            if (isRemoved) {
                this.Order.Remove(key);
            }

            return isRemoved;
        }

        public bool ContainsKey(FieldObject key)
        {
            return this.map.ContainsKey(key);
        }

        public bool ContainsValue(FieldObject value)
        {
            return this.map.ContainsValue(value);
        }

        public List<EntryField> ToList()
        {
            return map.Select(kvp => new EntryField(kvp)).ToList();
        }

        public bool RemoveAt(int index)
        {
            if (Order.Count == 0 || index >= Order.Count || index < 0) {
                return false;
            }

            this.map.Remove(this.Order[index]);
            this.Order.RemoveAt(index);
            return true;
        }

        public FieldObject this[FieldObject key]
        {
            get
            {
                FieldObject value;
                this.map.TryGetValue(key, out value);
                return value;
            }
            set
            {
                if (!this.map.ContainsKey(key))
                {
                    this.Order.Add(key);
                    this.map.Add(key, value);
                } else {
                    this.map[key] = value;
                }
            }
        }

        public override string ToString()
        {
            if (this.map.Count == 0) return "[]";

            StringBuilder mapStringBuilder = new StringBuilder("[");
            int i = 0;

            foreach (FieldObject key in Order)
            {
                if (i++ > 0)
                    mapStringBuilder.Append(", ");
                mapStringBuilder.Append(key.ToString()).Append(":").Append(this.map[key].ToString());
            }

            mapStringBuilder.Append(']');

            return mapStringBuilder.ToString();
        }

        public override string ToGraphSON()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("{");

            bool firstEntry = true;

            foreach (FieldObject entry in this.Order)
            {
                if (firstEntry) {
                    firstEntry = false;
                }
                else {
                    sb.Append(", ");
                }

                sb.AppendFormat("\"{0}\": {1}", entry.ToValue, this.map[entry].ToGraphSON());
            }

            sb.Append("}");
            return sb.ToString();
        }

        public override bool Equals(object obj)
        {
            if (Object.ReferenceEquals(this, obj)) return true;

            MapField mapField = obj as MapField;
            if (mapField == null || this.map.Count != mapField.map.Count) {
                return false;
            }

            foreach (KeyValuePair<FieldObject, FieldObject> kvp in this.map)
            {
                FieldObject key = kvp.Key;
                FieldObject value2;
                if (!mapField.map.TryGetValue(key, out value2))
                    return false;
                if (!kvp.Value.Equals(value2))
                    return false;
            }

            return true;
        }

        public override int GetHashCode()
        {
            if (this.map.Count == 0) return "[]".GetHashCode();

            StringBuilder mapStringBuilder = new StringBuilder("[");
            int i = 0;

            foreach (KeyValuePair<FieldObject, FieldObject> kvp in this.map)
            {
                FieldObject key = kvp.Key;
                FieldObject value = kvp.Value;

                if (i++ > 0)
                    mapStringBuilder.Append(", ");
                mapStringBuilder.Append(key.ToString()).Append(":").Append(value.ToString());
            }

            mapStringBuilder.Append(']');

            return mapStringBuilder.ToString().GetHashCode();
        }

        public IEnumerator<EntryField> GetEnumerator()
        {
            foreach (KeyValuePair<FieldObject, FieldObject> keyValuePair in map) {
                yield return new EntryField(keyValuePair);
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
    }

    internal class EntryField : FieldObject
    {
        private KeyValuePair<FieldObject, FieldObject> entry;

        public EntryField(KeyValuePair<FieldObject, FieldObject> entry)
        {
            this.entry = entry;
        }

        public FieldObject Key => entry.Key;
        public FieldObject Value => entry.Value;

        public override string ToString()
        {
            return $"{this.entry.Key.ToString()} = {this.entry.Value.ToString()}";
        }

        public override string ToGraphSON()
        {
            return $"{{{FieldObject.ToLiteral(this.entry.Key.ToValue)}:{this.entry.Value.ToGraphSON()}}}";
        }

        public override int GetHashCode()
        {
            return this.ToString().GetHashCode();
        }

        public override bool Equals(object obj)
        {
            if (Object.ReferenceEquals(this, obj)) return true;

            EntryField rhs = obj as EntryField;
            if (rhs == null) {
                return false;
            }

            return this.entry.Key.Equals(rhs.entry.Key) && this.entry.Value.Equals(rhs.entry.Value);
        }
    }

    internal class Compose1Field : FieldObject
    {
        private Dictionary<string, FieldObject> compositeFieldObject { get; set; }
        public string DefaultProjectionKey { get; set; }

        public Compose1Field(Dictionary<string, FieldObject> compositeFieldObject, string defaultProjectionKey)
        {
            this.compositeFieldObject = compositeFieldObject;
            DefaultProjectionKey = defaultProjectionKey;
        }

        public FieldObject this[string key]
        {
            get
            {
                FieldObject value;
                this.compositeFieldObject.TryGetValue(key, out value);
                return value;
            }
            set
            {
                this.compositeFieldObject[key] = value;
            }
        }

        public override string ToString()
        {
            return this[this.DefaultProjectionKey].ToString();
        }

        public override string ToValue => this[DefaultProjectionKey].ToValue;

        public override string ToGraphSON()
        {
            return this[this.DefaultProjectionKey].ToGraphSON();
        }

        public override bool Equals(object obj)
        {
            if (Object.ReferenceEquals(this, obj)) return true;

            Compose1Field rhs = obj as Compose1Field;
            if (rhs == null) {
                return false;
            }

            return this[this.DefaultProjectionKey].Equals(rhs[rhs.DefaultProjectionKey]);
        }

        public override int GetHashCode()
        {
            return this[this.DefaultProjectionKey].GetHashCode();
        }
    }

    internal abstract class PropertyField : FieldObject
    {
        public string PropertyName { get; private set; }
        public virtual string PropertyValue { get; set; }
        public virtual JsonDataType JsonDataType { get; set; }

        protected PropertyField(string propertyName, string propertyValue, JsonDataType jsonDataType)
        {
            Debug.Assert(this is VertexPropertyField || propertyValue != null);

            PropertyName = propertyName;
            PropertyValue = propertyValue;
            JsonDataType = jsonDataType;
        }

        public object ToPropertyValueObject()
        {
            switch (JsonDataType)
            {
                case JsonDataType.Boolean:
                    return bool.Parse(PropertyValue);
                case JsonDataType.Int:
                    return int.Parse(PropertyValue);
                case JsonDataType.Long:
                    return long.Parse(PropertyValue);
                case JsonDataType.Double:
                    return double.Parse(PropertyValue);
                case JsonDataType.Float:
                    return float.Parse(PropertyValue);
                case JsonDataType.Null:
                    return null;
                default:
                    return PropertyValue;
            }
        }

        public override string ToString()
        {
            return string.Format("{0}->{1}", PropertyName, PropertyValue);
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
            if (this.JsonDataType == JsonDataType.String)
                return string.Format("{{{0}: {1}}}", FieldObject.ToLiteral(this.PropertyName), FieldObject.ToLiteral(this.PropertyValue));
            if (this.JsonDataType == JsonDataType.Boolean)
                return string.Format("{{{0}: {1}}}", FieldObject.ToLiteral(this.PropertyName), this.PropertyValue.ToLowerInvariant());
            return string.Format("{{{0}: {1}}}", FieldObject.ToLiteral(this.PropertyName), this.PropertyValue);
        }
    }

    internal class VertexSinglePropertyField : PropertyField
    {
        public readonly Dictionary<string, ValuePropertyField> MetaProperties = new Dictionary<string, ValuePropertyField>();

        public string PropertyId { get; }

        public VertexPropertyField VertexProperty { get; }


        public VertexSinglePropertyField(string propertyName, JObject vertexSinglePropertyObject, VertexPropertyField vertexPropertyField) 
            : base(propertyName, 
                  vertexSinglePropertyObject[KW_PROPERTY_VALUE].ToString(), 
                  JsonDataTypeHelper.GetJsonDataType(vertexSinglePropertyObject[KW_PROPERTY_VALUE].Type))
        {
            Debug.Assert(vertexSinglePropertyObject[GraphViewKeywords.KW_PROPERTY_ID] != null);

            this.VertexProperty = vertexPropertyField;

            this.PropertyId = (string)vertexSinglePropertyObject[GraphViewKeywords.KW_PROPERTY_ID];
            this.Replace(vertexSinglePropertyObject);
        }

        public VertexSinglePropertyField(VertexSinglePropertyField rhs)
            :base(rhs.PropertyName, rhs.PropertyValue, rhs.JsonDataType)
        {
            this.MetaProperties = rhs.MetaProperties;
            this.PropertyId = rhs.PropertyId;
            this.VertexProperty = rhs.VertexProperty;
        }

        //public void UpdateValue(JValue value)
        //{
        //    this.PropertyValue = value.ToString();
        //    this.JsonDataType = JsonDataTypeHelper.GetJsonDataType(value.Type);
        //}

        public FieldObject this[string metapropertyName]
        {
            get
            {
                if (metapropertyName.Equals(KW_PROPERTY_ID))
                    return new ValuePropertyField(KW_PROPERTY_ID, this.PropertyId, JsonDataType.String, this);

                ValuePropertyField propertyField;
                this.MetaProperties.TryGetValue(metapropertyName, out propertyField);
                return propertyField;
            }
        }

        public void Replace(JObject vertexSinglePropertyObject)
        {
            /* Schema of vertexSinglePropertyObject: 
                {
                  KW_PROPERTY_VALUE: ...,
                  KW_PROPERTY_ID: <GUID>
                  KW_PROPERTY_META: { 
                    "K1": "V1", 
                    ...
                  }
                }
            */
            Debug.Assert(vertexSinglePropertyObject[GraphViewKeywords.KW_PROPERTY_ID] != null);
            Debug.Assert((string)vertexSinglePropertyObject[GraphViewKeywords.KW_PROPERTY_ID] == this.PropertyId);

            JValue value = (JValue) vertexSinglePropertyObject[KW_PROPERTY_VALUE];
            this.PropertyValue = value.ToString();
            this.JsonDataType = JsonDataTypeHelper.GetJsonDataType(value.Type);

            HashSet<string> metaPropertyKeysToRemove = new HashSet<string>(this.MetaProperties.Keys);
            foreach (JProperty metaProperty in vertexSinglePropertyObject[KW_PROPERTY_META].Children<JProperty>()) {
                ValuePropertyField valueProp;
                bool found = this.MetaProperties.TryGetValue(metaProperty.Name, out valueProp);
                if (found) {
                    valueProp.Replace(metaProperty);
                    metaPropertyKeysToRemove.Remove(metaProperty.Name);
                }
                else {
                    this.MetaProperties.Add(metaProperty.Name, new ValuePropertyField(metaProperty, this));
                }
            }

            // TODO: Whether to remove them?
            foreach (string metaPropertyToRemove in metaPropertyKeysToRemove) {
                this.MetaProperties.Remove(metaPropertyToRemove);
            }
        }

        public override string ToString()
        {
            return string.Format("vp[{0}]", base.ToString());
        }

        public override string ToGraphSON()
        {
            StringBuilder vpGraphSonBuilder = new StringBuilder();
            if (this.JsonDataType == JsonDataType.String)
                vpGraphSonBuilder.AppendFormat("{{\"id\": {0}, \"value\": {1}, \"label\": {2}", 
                    FieldObject.ToLiteral(this.PropertyId), FieldObject.ToLiteral(this.PropertyValue), FieldObject.ToLiteral(this.PropertyName));
            else if (this.JsonDataType == JsonDataType.Boolean)
                vpGraphSonBuilder.AppendFormat("{{\"id\": {0}, \"value\": {1}, \"label\": {2}",
                    FieldObject.ToLiteral(this.PropertyId), this.PropertyValue.ToLowerInvariant(), FieldObject.ToLiteral(this.PropertyName));
            else
                vpGraphSonBuilder.AppendFormat("{{\"id\": {0}, \"value\": {1}, \"label\": {2}",
                    FieldObject.ToLiteral(this.PropertyId), this.PropertyValue, FieldObject.ToLiteral(this.PropertyName));

            bool isFirstMetaproperty = true;

            foreach (KeyValuePair<string, ValuePropertyField> kvp in this.MetaProperties)
            {
                string key = kvp.Key;
                ValuePropertyField value = kvp.Value;

                if (isFirstMetaproperty)
                {
                    vpGraphSonBuilder.Append(", \"properties\":{");
                    isFirstMetaproperty = false;
                }      
                else
                    vpGraphSonBuilder.Append(", ");

                if (value.JsonDataType == JsonDataType.String)
                    vpGraphSonBuilder.AppendFormat("{0}: {1}", FieldObject.ToLiteral(key), FieldObject.ToLiteral(value.PropertyValue));
                else if (value.JsonDataType == JsonDataType.Boolean)
                    vpGraphSonBuilder.AppendFormat("{0}: {1}", FieldObject.ToLiteral(key), value.PropertyValue.ToLowerInvariant());
                else
                    vpGraphSonBuilder.AppendFormat("{0}: {1}", FieldObject.ToLiteral(key), value.PropertyValue);
            }

            if (!isFirstMetaproperty)
                vpGraphSonBuilder.Append("}");
            
            vpGraphSonBuilder.Append("}");

            return vpGraphSonBuilder.ToString();
        }
    }

    internal class EdgePropertyField : PropertyField
    {
        public EdgeField Edge { get; }

        public EdgePropertyField(string propertyName, string propertyValue, JsonDataType jsonDataType, EdgeField edgeField)
            : base(propertyName, propertyValue, jsonDataType)
        {
            this.Edge = edgeField;
        }

        public EdgePropertyField(JProperty property, EdgeField edgeField)
            : base(property.Name,
                property.Value.ToString(),
                JsonDataTypeHelper.GetJsonDataType(property.Value.Type))
        {
            Debug.Assert(property.Value is JValue);

            this.Edge = edgeField;
        }

        public EdgePropertyField(EdgePropertyField rhs)
            : base(rhs.PropertyName, rhs.PropertyValue, rhs.JsonDataType)
        {
            this.Edge = rhs.Edge;
        }

        public override string ToString()
        {
            return string.Format("p[{0}]", base.ToString());
        }

        public override string ToGraphSON()
        {
            if (this.JsonDataType == JsonDataType.String)
                return string.Format("{{\"key\":{0}, \"value\":{1}}}", FieldObject.ToLiteral(this.PropertyName), FieldObject.ToLiteral(this.PropertyValue));
            if (this.JsonDataType == JsonDataType.Boolean)
                return string.Format("{{\"key\":{0}, \"value\":{1}}}", FieldObject.ToLiteral(this.PropertyName), this.PropertyValue.ToLowerInvariant());
            return string.Format("{{\"key\":{0}, \"value\":{1}}}", FieldObject.ToLiteral(this.PropertyName), this.PropertyValue);
        }

        public void Replace(JProperty property)
        {
            Debug.Assert(this.PropertyName == property.Name);
            Debug.Assert(property.Value is JValue);
            Debug.Assert(((JValue)property.Value).Type != JTokenType.Null);

            this.PropertyValue = ((JValue)property.Value).ToString(CultureInfo.InvariantCulture);
            this.JsonDataType = JsonDataTypeHelper.GetJsonDataType(property.Value.Type);
        }
    }

    internal class ValuePropertyField : PropertyField
    {
        /// <summary>
        /// If this is a vertex meta property (id, label, ...), its parent is VertexField
        /// If this is a vertex-property's meta property, its parent is VertexSinglePropertyField
        /// </summary>
        public FieldObject Parent { get; }

        public ValuePropertyField(string propertyName, string propertyValue, JsonDataType jsonDataType, VertexField vertexField)
            : base(propertyName, propertyValue, jsonDataType)
        {
            Debug.Assert(VertexField.IsVertexMetaProperty(propertyName));

            this.Parent = vertexField;
        }

        public ValuePropertyField(string propertyName, string propertyValue, JsonDataType jsonDataType, VertexSinglePropertyField vertexSingleProperty)
            : base(propertyName, propertyValue, jsonDataType)
        {
            this.Parent = vertexSingleProperty;
        }

        
        public ValuePropertyField(JProperty property, VertexField vertexField)
            : base(property.Name,
                property.Value.ToString(),
                JsonDataTypeHelper.GetJsonDataType(property.Value.Type))
        {
            Debug.Assert(VertexField.IsVertexMetaProperty(property.Name));
            Debug.Assert(property.Value is JValue);

            this.Parent = vertexField;
        }


        public ValuePropertyField(JProperty property, VertexSinglePropertyField vertexSingleProperty)
            : base(property.Name,
                property.Value.ToString(),
                JsonDataTypeHelper.GetJsonDataType(property.Value.Type))
        {
            Debug.Assert(property.Value is JValue);

            this.Parent = vertexSingleProperty;
        }

        public ValuePropertyField(ValuePropertyField rhs)
            : base(rhs.PropertyName, rhs.PropertyValue, rhs.JsonDataType)
        {
            this.Parent = rhs.Parent;
        }

        public override string ToString()
        {
            return string.Format("p[{0}]", base.ToString());
        }

        public override string ToGraphSON()
        {
            if (this.JsonDataType == JsonDataType.String)
                return string.Format("{{\"key\":{0}, \"value\":{1}}}", FieldObject.ToLiteral(this.PropertyName), FieldObject.ToLiteral(this.PropertyValue));
            if (this.JsonDataType == JsonDataType.Boolean)
                return string.Format("{{\"key\":{0}, \"value\":{1}}}", FieldObject.ToLiteral(this.PropertyName), this.PropertyValue.ToLowerInvariant());
            return string.Format("{{\"key\":{0}, \"value\":{1}}}", FieldObject.ToLiteral(this.PropertyName), this.PropertyValue);
        }

        public void Replace(JProperty property)
        {
            Debug.Assert(this.PropertyName == property.Name);
            Debug.Assert(property.Value is JValue);
            Debug.Assert(((JValue)property.Value).Type != JTokenType.Null);

            Debug.Assert(this.Parent is VertexSinglePropertyField);

            this.PropertyValue = ((JValue)property.Value).ToString(CultureInfo.InvariantCulture);
            this.JsonDataType = JsonDataTypeHelper.GetJsonDataType(property.Value.Type);
        }
    }

    internal class VertexPropertyField : PropertyField
    {
        // <id, single_property_field>
        public Dictionary<string, VertexSinglePropertyField> Multiples { get; } = new Dictionary<string, VertexSinglePropertyField>();

        public VertexField Vertex { get; }

        public override string PropertyValue {
            get {
                if (Multiples.Count == 1)
                    return Multiples.Values.First().PropertyValue;
                if (Multiples.Count == 0)
                    return "";
                Debug.Assert(false, "Should not get here.");
                return "";
                //throw new NotSupportedException("Can't get value on a VertexPropertyField with multiple properties");
            }
            set {
                // Do nothing
            }
        }

        public override string ToString()
        {
            if (this.Multiples.Count == 1)
                return Multiples.Values.First().ToString();
            if (this.Multiples.Count == 0)
                return "";
            Debug.Assert(false, "Should not get here.");
            return "";
            //throw new NotSupportedException("Can't get value on a VertexPropertyField with multiple properties");
        }

        public override string ToValue {
            get
            {
                if (this.Multiples.Count == 1)
                    return Multiples.Values.First().ToValue;
                if (this.Multiples.Count == 0)
                    return "";
                Debug.Assert(false, "Should not get here.");
                return "";
                //throw new NotSupportedException("Can't get 'ToValue' on a VertexPropertyField");
            }
        }

        public override JsonDataType JsonDataType
        {
            get
            {
                if (this.Multiples.Count == 1)
                    return Multiples.Values.First().JsonDataType;
                if (this.Multiples.Count == 0)
                    return JsonDataType.Null;

                Debug.Assert(false, "Should not get here.");
                return JsonDataType.String;
            }
        }

        //public VertexPropertyField(string propertyName, string propertyValue, JsonDataType jsonDataType)
        //    : base(propertyName, propertyValue, jsonDataType)
        //{
        //}

        public VertexPropertyField(JProperty multiProperty, VertexField vertexField)
            : base(multiProperty.Name, null, JsonDataType.Array)
        {
            this.Replace(multiProperty);
            this.Vertex = vertexField;
        }

        public override string ToGraphSON()
        {
            StringBuilder vpGraphSonBuilder = new StringBuilder();
            vpGraphSonBuilder.AppendFormat("{0}:[", FieldObject.ToLiteral(this.PropertyName));

            bool isFirstVsp = true;
            foreach (VertexSinglePropertyField vsp in this.Multiples.Values)
            {
                if (isFirstVsp)
                    isFirstVsp = false;
                else
                    vpGraphSonBuilder.Append(", ");

                if (vsp.JsonDataType == JsonDataType.String)
                    vpGraphSonBuilder.AppendFormat("{{\"id\": {0}, \"value\": {1}",
                        FieldObject.ToLiteral(vsp.PropertyId), FieldObject.ToLiteral(vsp.PropertyValue));
                else if (vsp.JsonDataType == JsonDataType.Boolean)
                    vpGraphSonBuilder.AppendFormat("{{\"id\": {0}, \"value\": {1}",
                        FieldObject.ToLiteral(vsp.PropertyId), vsp.PropertyValue.ToLowerInvariant());
                else
                    vpGraphSonBuilder.AppendFormat("{{\"id\": {0}, \"value\": {1}",
                        FieldObject.ToLiteral(vsp.PropertyId), vsp.PropertyValue);

                if (vsp.MetaProperties.Count > 0)
                {
                    vpGraphSonBuilder.Append(", \"properties\":{");
                    bool isFirstMetaproperty = true;

                    foreach (KeyValuePair<string, ValuePropertyField> kvp in vsp.MetaProperties)
                    {
                        string key = kvp.Key;
                        ValuePropertyField value = kvp.Value;

                        if (isFirstMetaproperty)
                            isFirstMetaproperty = false;
                        else
                            vpGraphSonBuilder.Append(", ");

                        if (value.JsonDataType == JsonDataType.String)
                            vpGraphSonBuilder.AppendFormat("{0}: {1}", FieldObject.ToLiteral(key), FieldObject.ToLiteral(value.PropertyValue));
                        else if (value.JsonDataType == JsonDataType.Boolean)
                            vpGraphSonBuilder.AppendFormat("{0}: {1}", FieldObject.ToLiteral(key), value.PropertyValue);
                        else
                            vpGraphSonBuilder.AppendFormat("{0}: {1}", FieldObject.ToLiteral(key), value.PropertyValue);
                    }
                    vpGraphSonBuilder.Append("}");
                }

                vpGraphSonBuilder.Append("}");
            }

            vpGraphSonBuilder.Append("]");

            return vpGraphSonBuilder.ToString();
        }

        public PropertyField ToVertexPropertyFieldIfSingle()
        {
            Debug.Assert(this.Multiples.Count > 0);
            if (this.Multiples.Count == 1) {
                return this.Multiples.Values.First();
            }
            else {
                return this;
            }
        }


        public void Replace(JProperty multiProperty)
        {
            /* multiProperty looks like: 
              <propName>: [
                {
                  KW_PROPERTY_VALUE: "Property Value",
                  KW_PROPERTY_ID: <GUID>
                  KW_PROPERTY_META: { ... }
                }, 
                ...
              ]
            */
            Debug.Assert(multiProperty.Name == this.PropertyName);
            Debug.Assert(multiProperty.Value is JArray);
            this.PropertyValue = null;
            this.JsonDataType = JsonDataType.Array;
            

            HashSet<string> metaPropIdToRemove = new HashSet<string>(this.Multiples.Keys);
            foreach (JObject vertexPropertyObject in ((JArray)multiProperty.Value).Values<JObject>()) {
                Debug.Assert(vertexPropertyObject[KW_PROPERTY_VALUE] is JValue);
                Debug.Assert(vertexPropertyObject[KW_PROPERTY_ID] is JValue);
                Debug.Assert(vertexPropertyObject[KW_PROPERTY_META] is JObject);

                string propId = (string) vertexPropertyObject[KW_PROPERTY_ID];
                if (metaPropIdToRemove.Remove(propId)) {
                    // This single-property should be replaced
                    this.Multiples[propId].Replace(vertexPropertyObject);
                }
                else {
                    // This single-property is newly added
                    this.Multiples.Add(propId, new VertexSinglePropertyField(multiProperty.Name, vertexPropertyObject, this));
                }
            }

            foreach (string propId in metaPropIdToRemove) {
                this.Multiples.Remove(propId);
            }
        }
    }

    internal class EdgeField : FieldObject
    {

        // <PropertyName, EdgePropertyField>
        public Dictionary<string, EdgePropertyField> EdgeProperties;

        public string Label { get; private set; }
        public string InVLabel { get; private set; }
        public string OutVLabel { get; private set; }
        public string InV { get; private set; }
        public string OutV { get; private set; }
        public string EdgeDocID { get; set; }
        public long Offset { get; private set; }

        //
        // This property will only be assigned in the adjacency list decoder
        // since OtherV is not a meta property of an edge.
        // It can only be decided in the runtime.
        //
        public string OtherV { get; private set; }

        private EdgeField()
        {
            this.EdgeProperties = new Dictionary<string, EdgePropertyField>();
        }

        public EdgeField(EdgeField rhs, string otherV)
        {
            this.EdgeProperties = rhs.EdgeProperties;
            this.Label = rhs.Label;
            this.InVLabel = rhs.InVLabel;
            this.OutVLabel = rhs.OutVLabel;
            this.InV = rhs.InV;
            this.OutV = rhs.OutV;
            this.EdgeDocID = rhs.EdgeDocID;
            this.Offset = rhs.Offset;

            this.OtherV = otherV;
        }

        public FieldObject this[string propertyName]
        {
            get
            {
                if (propertyName.Equals("*", StringComparison.OrdinalIgnoreCase))
                    return this;

                switch (propertyName)
                {
                    case GremlinKeyword.EdgeSourceV:
                        return new StringField(this.OutV);
                    case GremlinKeyword.EdgeSinkV:
                        return new StringField(this.InV);
                    case GremlinKeyword.EdgeOtherV:
                        return new StringField(this.OtherV);
                    case GremlinKeyword.EdgeOffset:
                        return new StringField(this.Offset.ToString(), JsonDataType.Long);
                    default:
                        break;
                }

                EdgePropertyField propertyField;
                this.EdgeProperties.TryGetValue(propertyName, out propertyField);
                return propertyField;
            }
        }


        public void UpdateEdgeProperty(JProperty property, EdgeField edgeField)
        {
            EdgePropertyField propertyField;
            if (this.EdgeProperties.TryGetValue(property.Name, out propertyField)) {
                propertyField.Replace(property);
            }
            else {
                this.EdgeProperties.Add(property.Name, new EdgePropertyField(property, this));
            }
        }

        public override string ToString()
        {
            return String.Format("e[{0}]{1}({2})-{3}->{4}({5})", this.EdgeProperties[KW_EDGE_ID].ToValue, this.OutV, this.OutVLabel, this.Label, this.InV, this.InVLabel);
        }

        public override string ToGraphSON()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("{{\"id\": \"{0}\"", this.EdgeProperties[KW_EDGE_ID].ToValue);
            if (this.Label != null) {
                sb.AppendFormat(", \"label\": {0}", FieldObject.ToLiteral(this.Label));
            }

            sb.Append(", \"type\": \"edge\"");

            if (this.InVLabel != null) {
                sb.AppendFormat(", \"inVLabel\": {0}", FieldObject.ToLiteral(this.InVLabel));
            }
            if (this.OutVLabel != null) {
                sb.AppendFormat(", \"outVLabel\": {0}", FieldObject.ToLiteral(this.OutVLabel));
            }
            if (this.InV != null) {
                sb.AppendFormat(", \"inV\": {0}", FieldObject.ToLiteral(this.InV));
            }
            if (this.OutV != null) {
                sb.AppendFormat(", \"outV\": {0}", FieldObject.ToLiteral(this.OutV));
            }

            bool firstProperty = true;
            foreach (string propertyName in this.EdgeProperties.Keys) {
                switch (propertyName) {
                case KW_EDGE_LABEL:
                case KW_EDGE_OFFSET:
                case KW_EDGE_SRCV:
                case KW_EDGE_SRCV_LABEL:
                case KW_EDGE_SINKV:
                case KW_EDGE_SINKV_LABEL:
                case KW_EDGE_ID:

                //case GremlinKeyword.EdgeSourceV:
                //case GremlinKeyword.EdgeSinkV:
                //case GremlinKeyword.EdgeOtherV:
                    continue;
                default:
                    break;
                }

                if (firstProperty) {
                    sb.Append(", \"properties\": {");
                    firstProperty = false;
                }
                else {
                    sb.Append(", ");
                }

                if (this.EdgeProperties[propertyName].JsonDataType == JsonDataType.String) {
                    sb.AppendFormat("{0}: {1}", FieldObject.ToLiteral(propertyName), FieldObject.ToLiteral(this.EdgeProperties[propertyName].PropertyValue));
                }
                else if (this.EdgeProperties[propertyName].JsonDataType == JsonDataType.Boolean) {
                    sb.AppendFormat("{0}: {1}", FieldObject.ToLiteral(propertyName), this.EdgeProperties[propertyName].PropertyValue.ToLowerInvariant());
                }
                else {
                    sb.AppendFormat("{0}: {1}", FieldObject.ToLiteral(propertyName), this.EdgeProperties[propertyName].PropertyValue);
                }
            }
            if (!firstProperty) {
                sb.Append("}");
            }
            sb.Append("}");

            return sb.ToString();
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(this, obj)) return true;

            EdgeField rhs = obj as EdgeField;
            if (rhs == null) {
                return false;
            }

            return this.EdgeProperties[KW_EDGE_ID].ToValue.Equals(rhs.EdgeProperties[KW_EDGE_ID].ToValue);
        }

        public override int GetHashCode()
        {
            return this.EdgeProperties[KW_EDGE_ID].ToValue.GetHashCode();
        }

        public static EdgeField ConstructForwardEdgeField(string outVId, string outVLabel, string edgeDocID, JObject edgeObject)
        {
            EdgeField edgeField = new EdgeField {
                OutV = outVId,
                OutVLabel = outVLabel,
                EdgeDocID = edgeDocID,
                Offset = (long)edgeObject[KW_EDGE_OFFSET],
            };

            foreach (JProperty property in edgeObject.Properties()) {
                edgeField.EdgeProperties.Add(property.Name, new EdgePropertyField(property, edgeField));

                switch (property.Name) {
                case KW_EDGE_SINKV: // "_sinkV"
                    edgeField.InV = property.Value.ToString();
                    break;
                case KW_EDGE_SINKV_LABEL: // "_sinkVLabel"
                    edgeField.InVLabel = property.Value.ToString();
                    break;
                case KW_EDGE_LABEL:
                    edgeField.Label = property.Value.ToString();
                    break;
                }
            }

            return edgeField;
        }

        public static EdgeField ConstructBackwardEdgeField(string inVId, string inVLabel, string edgeDocID, JObject edgeObject)
        {
            EdgeField edgeField = new EdgeField {
                InV = inVId,
                InVLabel = inVLabel,
                EdgeDocID = edgeDocID,
                Offset = (long)edgeObject[KW_EDGE_OFFSET],
            };

            foreach (JProperty property in edgeObject.Properties()) {
                edgeField.EdgeProperties.Add(property.Name, new EdgePropertyField(property, edgeField));

                switch (property.Name) {
                case KW_EDGE_SRCV:
                    edgeField.OutV = property.Value.ToString();
                    break;
                case KW_EDGE_SRCV_LABEL:
                    edgeField.OutVLabel = property.Value.ToString();
                    break;
                case KW_EDGE_LABEL:
                    edgeField.Label = property.Value.ToString();
                    break;
                }
            }

            return edgeField;
        }
    }

    internal class AdjacencyListField : FieldObject
    {
        // <$"{srcVertexId}.{edgeOffset}", EdgeField>
        private Dictionary<string, EdgeField> Edges { get; }

        public IEnumerable<EdgeField> AllEdges => this.Edges.Values;


        public AdjacencyListField()
        {
            this.Edges = new Dictionary<string, EdgeField>();
        }


        private string MakeKey(string srcVertexId, long edgeOffset) => $"{srcVertexId}.{edgeOffset}";


        public void AddEdgeField(string srcVertexId, long edgeOffset, EdgeField edgeField)
        {
            this.Edges.Add(MakeKey(srcVertexId, edgeOffset), edgeField);
        }

        public void RemoveEdgeField(string srcVertexId, long edgeOffset)
        {
            this.Edges.Remove(MakeKey(srcVertexId, edgeOffset));
        }

        public EdgeField GetEdgeField(string srcVertexId, long edgeOffset)
        {
            EdgeField edgeField;
            this.Edges.TryGetValue(MakeKey(srcVertexId, edgeOffset), out edgeField);
            return edgeField;
        }


        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();

            foreach (string offset in Edges.Keys.OrderBy(e => long.Parse(e.Substring(e.IndexOf(".")+1))))
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
        public static bool IsVertexMetaProperty(string propertyName)
        {
            switch (propertyName) {
            case KW_DOC_ID:
            case KW_DOC_PARTITION:
            case KW_VERTEX_LABEL:
            case KW_VERTEX_NEXTOFFSET:
                return true;
            default:
                return false;
            }
        }

        // <Property Name, VertexPropertyField>
        public Dictionary<string, VertexPropertyField> VertexProperties { get; } = new Dictionary<string, VertexPropertyField>();

        /// <summary>
        /// [Property Name, ValuePropertyField] (that is, "id", "_nextEdgeOffset", "label", "_partition")
        /// "_edge" and "_reverse_edge" are not included
        /// </summary>
        public Dictionary<string, ValuePropertyField> VertexMetaProperties { get; } = new Dictionary<string, ValuePropertyField>();

        public string VertexId => this.VertexMetaProperties[KW_DOC_ID].PropertyValue;


        /// <summary>
        /// Return all the properties of a vertex, they can be:
        ///  - ValuePropertyField: for reserved properties: id, label, _nextEdgeOffset, _partition (no meta-properties)
        ///  - VertexSinglePropertyField: for custom properties, single value (may contain meta-properties)
        ///  - VertexPropertyField: for custom properties, multiple values (may contain meta-properties)
        /// </summary>
        public IEnumerable<PropertyField> AllProperties {
            get {
                foreach (KeyValuePair<string, ValuePropertyField> pair in this.VertexMetaProperties) {
                    yield return pair.Value;
                }
                foreach (KeyValuePair<string, VertexPropertyField> pair in this.VertexProperties) {
                    yield return pair.Value.ToVertexPropertyFieldIfSingle();
                }
            }
        }


        public AdjacencyListField AdjacencyList { get; set; }
        public AdjacencyListField RevAdjacencyList { get; set; }

        private GraphViewConnection connection;

        public FieldObject this[string propertyName]
        {
            get
            {
                // "id", "label", "_nextEdgeOffset"
                if (IsVertexMetaProperty(propertyName))
                    return this.VertexMetaProperties[propertyName];

                if (propertyName.Equals("*", StringComparison.OrdinalIgnoreCase))
                    return this;

                if (propertyName.Equals(KW_VERTEX_EDGE, StringComparison.OrdinalIgnoreCase))
                    return AdjacencyList;

                if (propertyName.Equals(KW_VERTEX_REV_EDGE, StringComparison.OrdinalIgnoreCase))
                    return RevAdjacencyList;

                VertexPropertyField propertyField;
                bool found = this.VertexProperties.TryGetValue(propertyName, out propertyField);
                if (!found) {
                    return null;
                }

                Debug.Assert(propertyField.Multiples.Count > 0, "Vertex's property must contains at least one value");
                return propertyField;
                //if (propertyField.Multiples.Count == 1) {
                //    Debug.Assert(propertyField.Multiples[0].PropertyName == propertyName);
                //    return propertyField.Multiples[0];
                //}
                //else {
                //    return propertyField;
                //}
            }
        }

        //[Obsolete]
        //public void ReplaceProperty(JProperty property)
        //{
        //    // Replace
        //    if (IsVertexMetaProperty(property.Name)) {
        //        ValuePropertyField valueProp;
        //        bool found = this.VertexMetaProperties.TryGetValue(property.Name, out valueProp);
        //        Debug.Assert(found && valueProp != null);

        //        valueProp.Replace(property);
        //    }
        //    else {
        //        VertexPropertyField propertyField;
        //        if (this.VertexProperties.TryGetValue(property.Name, out propertyField)) {
        //            propertyField.Replace(property);
        //        }
        //        else {
        //            this.VertexProperties.Add(
        //                property.Name, 
        //                new VertexPropertyField(property, this));
        //        }
        //    }
        //}


        public VertexField(GraphViewConnection connection)
        {
            VertexProperties = new Dictionary<string, VertexPropertyField>();
            AdjacencyList = new AdjacencyListField();
            RevAdjacencyList = new AdjacencyListField();

            this.connection = connection;
        }

        public override string ToString()
        {
            ValuePropertyField idProperty;
            string id;
            if (VertexMetaProperties.TryGetValue("id", out idProperty))
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
                ValuePropertyField idProperty;
                if (VertexMetaProperties.TryGetValue("id", out idProperty))
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
            sb.AppendFormat("\"id\": {0}", FieldObject.ToLiteral(VertexMetaProperties[KW_DOC_ID].PropertyValue));


            Debug.Assert(VertexMetaProperties.ContainsKey(KW_VERTEX_LABEL));
            if (VertexMetaProperties[KW_VERTEX_LABEL] != null) {
                sb.Append(", ");
                sb.AppendFormat("\"label\": {0}", FieldObject.ToLiteral(VertexMetaProperties[KW_VERTEX_LABEL].PropertyValue));
            }

            sb.Append(", ");
            sb.Append("\"type\": \"vertex\"");

            if (!connection.UseReverseEdges)
            {
                RevAdjacencyList = EdgeDocumentHelper.GetReverseAdjacencyListOfVertex(connection,
                    VertexMetaProperties["id"].PropertyValue);
            }

            if (RevAdjacencyList != null && RevAdjacencyList.AllEdges.Any())
            {
                sb.Append(", \"inE\": {");
                // Groups incoming edges by their labels
                var groupByLabel = RevAdjacencyList.AllEdges.GroupBy(e => e.Label);
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
                        sb.AppendFormat("\"id\": \"{0}\", ", 
                            edgeField.EdgeProperties[KW_EDGE_ID].ToValue);
                        sb.AppendFormat("\"outV\": \"{0}\"", edgeField.OutV);

                        bool firstInEProperty = true;
                        foreach (string propertyName in edgeField.EdgeProperties.Keys)
                        {
                            switch(propertyName)
                            {
                            case KW_EDGE_ID:
                            case KW_EDGE_LABEL:
                            case KW_EDGE_OFFSET:
                            case KW_EDGE_SRCV:
                            case KW_EDGE_SINKV:
                            case KW_EDGE_SRCV_LABEL:
                            case KW_EDGE_SINKV_LABEL:
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

                            if (edgeField.EdgeProperties[propertyName].JsonDataType == JsonDataType.String)
                            {
                                sb.AppendFormat("{0}: {1}",
                                    FieldObject.ToLiteral(propertyName),
                                    FieldObject.ToLiteral(edgeField.EdgeProperties[propertyName].PropertyValue));
                            }
                            else if (edgeField.EdgeProperties[propertyName].JsonDataType == JsonDataType.Boolean)
                            {
                                sb.AppendFormat("{0}: {1}",
                                    FieldObject.ToLiteral(propertyName),
                                    edgeField.EdgeProperties[propertyName].PropertyValue.ToLowerInvariant());
                            }
                            else
                            {
                                sb.AppendFormat("{0}: {1}",
                                    FieldObject.ToLiteral(propertyName),
                                    edgeField.EdgeProperties[propertyName].PropertyValue);
                            }
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

            if (AdjacencyList != null && AdjacencyList.AllEdges.Any())
            {
                sb.Append(", \"outE\": {");
                // Groups outgoing edges by their labels
                var groupByLabel = AdjacencyList.AllEdges.GroupBy(e => e.Label);
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
                        sb.AppendFormat("\"id\": \"{0}\", ", 
                        edgeField.EdgeProperties[KW_EDGE_ID].ToValue);
                        sb.AppendFormat("\"inV\": \"{0}\"", edgeField.InV);

                        bool firstOutEProperty = true;
                        foreach (string propertyName in edgeField.EdgeProperties.Keys)
                        {
                            switch (propertyName)
                            {
                            case KW_EDGE_ID:
                            case KW_EDGE_LABEL:
                            case KW_EDGE_OFFSET:
                            case KW_EDGE_SRCV:
                            case KW_EDGE_SINKV:
                            case KW_EDGE_SRCV_LABEL:
                            case KW_EDGE_SINKV_LABEL:
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

                            if (edgeField.EdgeProperties[propertyName].JsonDataType == JsonDataType.String)
                            {
                                sb.AppendFormat("{0}: {1}",
                                FieldObject.ToLiteral(propertyName),
                                FieldObject.ToLiteral(edgeField.EdgeProperties[propertyName].PropertyValue));
                            }
                            else if (edgeField.EdgeProperties[propertyName].JsonDataType == JsonDataType.Boolean)
                            {
                                sb.AppendFormat("{0}: {1}",
                                FieldObject.ToLiteral(propertyName),
                                edgeField.EdgeProperties[propertyName].PropertyValue.ToLowerInvariant());
                            }
                            else
                            {
                                sb.AppendFormat("{0}: {1}",
                                FieldObject.ToLiteral(propertyName),
                                edgeField.EdgeProperties[propertyName].PropertyValue);
                            }
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
            foreach (KeyValuePair<string, VertexPropertyField> kvp in VertexProperties)
            {
                string propertyName = kvp.Key;
                switch (propertyName)
                {
                    case KW_DOC_ID:
                    case KW_DOC_PARTITION:
                    case KW_VERTEX_LABEL:
                    case KW_VERTEX_EDGE:
                    case KW_VERTEX_REV_EDGE:
                    case KW_VERTEX_NEXTOFFSET:
                        continue;
                    default:
                        break;
                }

                if (firstVertexProperty)
                {
                    sb.Append(", \"properties\": {");
                    firstVertexProperty = false;
                }
                else {
                    sb.Append(", ");
                }

                VertexPropertyField vp = kvp.Value;
                sb.Append(vp.ToGraphSON());
            }

            if (!firstVertexProperty) {
                sb.Append("}");
            }

            sb.Append("}");

            return sb.ToString();
        }

        public override bool Equals(object obj)
        {
            if (Object.ReferenceEquals(this, obj)) return true;

            VertexField vf = obj as VertexField;
            if (vf == null)
            {
                return false;
            }

            return this["id"].ToValue.Equals(vf["id"].ToValue, StringComparison.OrdinalIgnoreCase);
        }

        public override int GetHashCode()
        {
            return this["id"].ToValue.GetHashCode();
        }
    }

    internal class TreeField : FieldObject
    {
        private FieldObject nodeObject;
        internal Dictionary<FieldObject, TreeField> Children;

        internal TreeField(FieldObject pNodeObject)
        {
            nodeObject = pNodeObject;
            Children = new Dictionary<FieldObject, TreeField>();
        }

        public override string ToGraphSON()
        {
            // Don't print the dummy root
            StringBuilder strBuilder = new StringBuilder();
            int cnt = 0;
            strBuilder.Append("{");
            foreach (KeyValuePair<FieldObject, TreeField> child in Children)
            {
                if (cnt++ != 0)
                    strBuilder.Append(", ");

                child.Value.ToGraphSON(strBuilder);
            }
            strBuilder.Append("}");
            return strBuilder.ToString();
        }

        public void ToGraphSON(StringBuilder strBuilder)
        {
            int cnt = 0;
            strBuilder.Append(FieldObject.ToLiteral(nodeObject.ToValue) + ":{\"key\":");
            strBuilder.Append(nodeObject.ToGraphSON());
            strBuilder.Append(", \"value\": ");
            strBuilder.Append("{");
            foreach (KeyValuePair<FieldObject, TreeField> child in Children)
            {
                if (cnt++ != 0)
                    strBuilder.Append(", ");

                child.Value.ToGraphSON(strBuilder);
            }
            strBuilder.Append("}}");
        }

        public override string ToString()
        {
            // Don't print the dummy root
            StringBuilder strBuilder = new StringBuilder();
            strBuilder.Append("[");
            int cnt = 0;
            foreach (KeyValuePair<FieldObject, TreeField> child in Children)
            {
                if (cnt++ != 0)
                    strBuilder.Append(", ");
                child.Value.ToString(strBuilder);
            }
            strBuilder.Append("]");
            return strBuilder.ToString();
        }

        private void ToString(StringBuilder strBuilder)
        {
            strBuilder.Append(nodeObject.ToString()).Append(":[");
            var cnt = 0;
            foreach (var child in Children)
            {
                if (cnt++ != 0)
                    strBuilder.Append(", ");
                child.Value.ToString(strBuilder);
            }
            strBuilder.Append("]");
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
        internal static ReadOnlyCollection<string> ReservedNodeProperties { get; } = 
            new ReadOnlyCollection<string>(new List<string> {
                GremlinKeyword.NodeID,
                GremlinKeyword.Label,
                GremlinKeyword.EdgeAdj,
                GremlinKeyword.ReverseEdgeAdj,
                GremlinKeyword.Star,
            });

        internal static ReadOnlyCollection<string> ReservedEdgeProperties { get; } =
            new ReadOnlyCollection<string>(new List<string> {
                GremlinKeyword.EdgeSourceV,
                GremlinKeyword.EdgeSinkV,
                GremlinKeyword.EdgeOtherV,
                GremlinKeyword.EdgeOffset,
                GremlinKeyword.Star,
            });
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


        //
        // [Obsolete]: Use GraphViewConnection.ExecuteQuery()
        //
        //internal static IQueryable<dynamic> SendQuery(string script, GraphViewConnection connection)
        //{
        //    FeedOptions QueryOptions = new FeedOptions { MaxItemCount = -1 };
        //    IQueryable<dynamic> Result = connection.DocDBClient.CreateDocumentQuery(
        //        UriFactory.CreateDocumentCollectionUri(connection.DocDBDatabaseId, connection.DocDBCollectionId), 
        //        script, QueryOptions);
        //    return Result;
        //}
    }
}
