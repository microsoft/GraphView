using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json.Linq;
using static GraphView.DocumentDBKeywords;

namespace GraphView
{
    [Serializable]
    internal abstract class ModificationBaseOperator : GraphViewExecutionOperator, ISerializable
    {
        protected GraphViewCommand Command;
        protected GraphViewExecutionOperator InputOperator;

        protected ModificationBaseOperator(GraphViewExecutionOperator inputOp, GraphViewCommand command)
        {
            this.InputOperator = inputOp;
            this.Command = command;
            this.Open();
        }

        internal abstract RawRecord DataModify(RawRecord record);

        public override RawRecord Next()
        {
            RawRecord srcRecord = null;

            while (InputOperator.State() && (srcRecord = InputOperator.Next()) != null)
            {
                RawRecord result = DataModify(srcRecord);
                if (result == null) continue;

                RawRecord resultRecord = new RawRecord(srcRecord);
                resultRecord.Append(result);

                return resultRecord;
            }

            Close();
            return null;
        }

        public override void ResetState()
        {
            InputOperator.ResetState();
            Open();
        }

        public override GraphViewExecutionOperator GetFirstOperator()
        {
            return this.InputOperator.GetFirstOperator();
        }

        public virtual void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("inputOp", this.InputOperator, typeof(GraphViewExecutionOperator));
        }

        protected ModificationBaseOperator(SerializationInfo info, StreamingContext context)
        {
            this.InputOperator = (GraphViewExecutionOperator)info.GetValue("inputOp", typeof(GraphViewExecutionOperator));
            AdditionalSerializationInfo additionalInfo = (AdditionalSerializationInfo)context.Context;
            this.Command = additionalInfo.Command;
            this.Open();
        }
    }

    [Serializable]
    internal class AddVOperator : ModificationBaseOperator
    {
        private JObject vertexDocument;
        private readonly List<string> projectedFieldList;
        private readonly List<PropertyTuple> properties;

        public AddVOperator(GraphViewExecutionOperator inputOp, GraphViewCommand command, JObject vertexDocument, 
            List<string> projectedFieldList, List<PropertyTuple> properties)
            : base(inputOp, command)
        {
            this.vertexDocument = vertexDocument;
            this.projectedFieldList = projectedFieldList;
            this.properties = properties;
        }

        private void AddPropertiesToVertexObject(JObject vertexJObject, RawRecord record)
        {
            foreach (PropertyTuple property in this.properties)
            {
                JValue propertyValue = property.GetPropertyJValue(record);
                Debug.Assert(propertyValue != null);

                // Special treat the partition key
                if (this.Command.Connection.CollectionType == CollectionType.PARTITIONED)
                {
                    Debug.Assert(this.Command.Connection.RealPartitionKey != null);
                    if (property.Name == this.Command.Connection.RealPartitionKey)
                    {
                        if (property.MetaProperties.Count > 0)
                        {
                            throw new GraphViewException("Partition value must not have meta properties");
                        }

                        if (vertexJObject[this.Command.Connection.RealPartitionKey] == null)
                        {
                            vertexJObject[this.Command.Connection.RealPartitionKey] = propertyValue;
                        }
                        else
                        {
                            throw new GraphViewException("Partition value must not be a list");
                        }
                        continue;
                    }
                }

                // Special treat the "id" property
                if (property.Name == KW_DOC_ID)
                {
                    if (vertexJObject[KW_DOC_ID] == null)
                    {
                        if (propertyValue.Type != JTokenType.String)
                        {
                            throw new GraphViewException("Vertex's ID must be a string");
                        }
                        if (string.IsNullOrEmpty((string)propertyValue))
                        {
                            throw new GraphViewException("Vertex's ID must not be null or empty");
                        }
                        vertexJObject[KW_DOC_ID] = (string)propertyValue;
                    }
                    else
                    {
                        throw new GraphViewException("Vertex's ID must not be specified more than once");
                    }
                    continue;
                }

                JObject meta = new JObject();
                foreach (KeyValuePair<string, Tuple<StringField, ScalarSubqueryFunction>> pair in property.MetaProperties)
                {
                    meta[pair.Key] = property.GetMetaPropertyJValue(pair.Key, record);
                }

                string name = property.Name;
                JArray propArray = (JArray)vertexJObject[name];
                if (propArray == null)
                {
                    propArray = new JArray();
                    vertexJObject[name] = propArray;
                }

                JObject prop = new JObject
                {
                    [KW_PROPERTY_VALUE] = propertyValue,
                    [KW_PROPERTY_ID] = GraphViewConnection.GenerateDocumentId(),
                };
                if (meta.Count > 0)
                {
                    prop[KW_PROPERTY_META] = meta;
                }
                propArray.Add(prop);
            }
        }

        internal override RawRecord DataModify(RawRecord record)
        {
            JObject vertexObject = (JObject)this.vertexDocument.DeepClone();
            this.AddPropertiesToVertexObject(vertexObject, record);

            string vertexId;
            if (vertexObject[KW_DOC_ID] == null)
            {
                vertexId = GraphViewConnection.GenerateDocumentId();
                vertexObject[KW_DOC_ID] = vertexId;
            }
            else
            {
                // Only string id is supported!
                // Assume user will not specify duplicated ids
                Debug.Assert(vertexObject[KW_DOC_ID] is JValue);
                Debug.Assert(((JValue)vertexObject[KW_DOC_ID]).Type == JTokenType.String);

                vertexId = (string)vertexObject[KW_DOC_ID];
            }

            Debug.Assert(vertexObject[KW_DOC_PARTITION] == null);
            if (this.Command.Connection.PartitionPathTopLevel == KW_DOC_PARTITION)
            {
                // Now the collection is created via GraphAPI

                if (vertexObject[this.Command.Connection.RealPartitionKey] == null)
                {
                    throw new GraphViewException($"AddV: Parition key '{this.Command.Connection.RealPartitionKey}' must be provided.");
                }

                // Special treat "id" or "label" specified as partition key
                JValue partition;
                if (this.Command.Connection.RealPartitionKey == KW_DOC_ID ||
                    this.Command.Connection.RealPartitionKey == KW_VERTEX_LABEL)
                {
                    partition = (JValue)(string)vertexObject[this.Command.Connection.RealPartitionKey];
                }
                else {
                    JValue value = (JValue)vertexObject[this.Command.Connection.RealPartitionKey];
                    partition = value;
                }

                vertexObject[KW_DOC_PARTITION] = partition;
            }

            VertexField vertexField;

            if (this.Command.InLazyMode)
            {
                vertexObject[DocumentDBKeywords.KW_DOC_ETAG] = DateTimeOffset.Now.ToUniversalTime().ToString();
                vertexField = this.Command.VertexCache.AddOrUpdateVertexField(vertexId, vertexObject);
                DeltaLogAddVertex log = new DeltaLogAddVertex();
                this.Command.VertexCache.AddOrUpdateVertexDelta(vertexField, log);
            }
            else
            {
                //
                // NOTE: We don't check whether the partition key exists. Let DocDB do it.
                // If the vertex doesn't have the specified partition key, a DocumentClientException will be thrown.
                //
                try
                {
                    this.Command.Connection.CreateDocumentAsync(vertexObject, this.Command).Wait();
                }
                catch (AggregateException ex)
                {
                    throw new GraphViewException("Error when uploading the vertex", ex.InnerException);
                }
                vertexField = this.Command.VertexCache.AddOrUpdateVertexField(vertexId, vertexObject);
            }

            RawRecord result = new RawRecord();

            foreach (string fieldName in projectedFieldList)
            {
                FieldObject fieldValue = vertexField[fieldName];

                result.Append(fieldValue);
            }

            return result;
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("vertex", this.vertexDocument.ToString());
            GraphViewSerializer.SerializeList(info, "projectedFieldList", this.projectedFieldList);
            GraphViewSerializer.SerializeList(info, "properties", this.properties);
        }

        protected AddVOperator(SerializationInfo info, StreamingContext context) : base(info, context)
        {
            this.vertexDocument = JObject.Parse(info.GetString("vertex"));
            this.projectedFieldList = GraphViewSerializer.DeserializeList<string>(info, "projectedFieldList");
            this.properties = GraphViewSerializer.DeserializeList<PropertyTuple>(info, "properties");
        }

    }

    [Serializable]
    internal class DropOperator : ModificationBaseOperator
    {
        private readonly int dropTargetIndex;

        public DropOperator(GraphViewExecutionOperator inputOp, GraphViewCommand command, int dropTargetIndex)
            : base(inputOp, command)
        {
            this.dropTargetIndex = dropTargetIndex;
        }

        private void DropVertex(VertexField vertexField)
        {
            string vertexId = vertexField.VertexId;
            VertexField vertex = this.Command.VertexCache.GetVertexField(vertexId);

            foreach (EdgeField outEdge in vertex.AdjacencyList.AllEdges.ToList())
            {
                this.DropEdge(outEdge);
            }

            foreach (EdgeField inEdge in vertex.RevAdjacencyList.AllEdges.ToList())
            {
                this.DropEdge(inEdge);
            }

            // Delete the vertex-document!
            JObject vertexObject = vertex.VertexJObject;
#if DEBUG
            if (vertex.ViaGraphAPI)
            {
                Debug.Assert(vertexObject[KW_VERTEX_EDGE] is JArray);
                if (!EdgeDocumentHelper.IsSpilledVertex(vertexObject, false))
                {
                    Debug.Assert(((JArray)vertexObject[KW_VERTEX_EDGE]).Count == 0);
                }

                if (this.Command.Connection.UseReverseEdges)
                {
                    Debug.Assert(vertexObject[KW_VERTEX_REV_EDGE] is JArray);
                    if (!EdgeDocumentHelper.IsSpilledVertex(vertexObject, true))
                    {
                        Debug.Assert(((JArray)vertexObject[KW_VERTEX_REV_EDGE]).Count == 0);
                    }
                }
            }
#endif
            if (this.Command.InLazyMode)
            {
                DeltaLogDropVertex log = new DeltaLogDropVertex();
                this.Command.VertexCache.AddOrUpdateVertexDelta(vertex, log);
            }
            else
            {
                this.Command.Connection.ReplaceOrDeleteDocumentAsync(vertexId, null,
                    this.Command.Connection.GetDocumentPartition(vertexObject), this.Command).Wait();
            }

            // Update VertexCache
            this.Command.VertexCache.TryRemoveVertexField(vertexId);
        }

        private void DropEdge(EdgeField edgeField)
        {
            string edgeId = edgeField.EdgeId;
            string srcId = edgeField.OutV;
            string sinkId = edgeField.InV;
            string srcVertexPartition = edgeField.OutVPartition;
            string sinkVertexPartition = edgeField.InVPartition;

            VertexField srcVertexField = this.Command.VertexCache.GetVertexField(srcId, srcVertexPartition);
            VertexField sinkVertexField = this.Command.VertexCache.GetVertexField(sinkId, sinkVertexPartition);

            if (this.Command.InLazyMode)
            {
                DeltaLogDropEdge log = new DeltaLogDropEdge();
                this.Command.VertexCache.AddOrUpdateEdgeDelta(edgeField, srcVertexField,
                    null, sinkVertexField, log, this.Command.Connection.UseReverseEdges);
            }
            else
            {
                JObject srcVertexObject = srcVertexField.VertexJObject;
                JObject srcEdgeObject;
                string srcEdgeDocId;
                EdgeDocumentHelper.FindEdgeBySourceAndEdgeId(
                    this.Command, srcVertexObject, srcId, edgeId, false,
                    out srcEdgeObject, out srcEdgeDocId);

                if (srcEdgeObject == null)
                {
                    return;
                }

                JObject sinkVertexObject = sinkVertexField.VertexJObject;
                string sinkEdgeDocId = null;

                if (this.Command.Connection.UseReverseEdges)
                {
                    if (!string.Equals(sinkId, srcId))
                    {
                        JObject dummySinkEdgeObject;
                        EdgeDocumentHelper.FindEdgeBySourceAndEdgeId(
                            this.Command, sinkVertexObject, srcId, edgeId, true,
                            out dummySinkEdgeObject, out sinkEdgeDocId);
                    }
                    else
                    {
                        Debug.Assert(object.ReferenceEquals(sinkVertexField, srcVertexField));
                        Debug.Assert(sinkVertexObject == srcVertexObject);
                        sinkEdgeDocId = srcEdgeDocId;
                    }
                }

                // <docId, <docJson, partition>>
                Dictionary<string, Tuple<JObject, string>> uploadDocuments = new Dictionary<string, Tuple<JObject, string>>();
                EdgeDocumentHelper.RemoveEdge(uploadDocuments, this.Command, srcEdgeDocId,
                    srcVertexField, false, srcId, edgeId);
                if (this.Command.Connection.UseReverseEdges)
                {
                    EdgeDocumentHelper.RemoveEdge(uploadDocuments, this.Command, sinkEdgeDocId,
                        sinkVertexField, true, srcId, edgeId);
                }
                this.Command.Connection.ReplaceOrDeleteDocumentsAsync(uploadDocuments, this.Command).Wait();

#if DEBUG
                // NOTE: srcVertexObject is excatly the reference of srcVertexField.VertexJObject
                // NOTE: sinkVertexObject is excatly the reference of sinkVertexField.VertexJObject

                // If source vertex is not spilled, the outgoing edge JArray of srcVertexField.VertexJObject should have been updated
                if (!EdgeDocumentHelper.IsSpilledVertex(srcVertexField.VertexJObject, false))
                {
                    Debug.Assert(
                        srcVertexField.VertexJObject[KW_VERTEX_EDGE].Cast<JObject>().All(
                            edgeObj => (string)edgeObj[KW_EDGE_ID] != edgeId));
                }

                if (this.Command.Connection.UseReverseEdges)
                {
                    // If sink vertex is not spilled, the incoming edge JArray of sinkVertexField.VertexJObject should have been updated
                    if (!EdgeDocumentHelper.IsSpilledVertex(srcVertexField.VertexJObject, true))
                    {
                        Debug.Assert(
                            sinkVertexField.VertexJObject[KW_VERTEX_REV_EDGE].Cast<JObject>().All(
                                edgeObj => (string)edgeObj[KW_EDGE_ID] != edgeId));
                    }
                }
#endif
            }

            srcVertexField.AdjacencyList.RemoveEdgeField(edgeId);
            sinkVertexField.RevAdjacencyList.RemoveEdgeField(edgeId);
        }

        private void DropVertexSingleProperty(VertexSinglePropertyField vp)
        {
            if (vp.PropertyName == this.Command.Connection.RealPartitionKey) {
                throw new GraphViewException("Drop the partition-by property is not supported");
            }

            // Update DocDB
            VertexField vertexField = vp.VertexProperty.Vertex;
            JObject vertexObject = vertexField.VertexJObject;

            Debug.Assert(vertexObject[vp.PropertyName] != null);

            JArray vertexProperty = (JArray)vertexObject[vp.PropertyName];
            vertexProperty
                .First(singleProperty => (string)singleProperty[DocumentDBKeywords.KW_PROPERTY_ID] == vp.PropertyId)
                .Remove();
            if (vertexProperty.Count == 0) {
               vertexObject.Property(vp.PropertyName).Remove();
            }

            if (this.Command.InLazyMode)
            {
                DeltaLogDropVertexSingleProperty log = new DeltaLogDropVertexSingleProperty(vp.PropertyName, vp.PropertyId);
                this.Command.VertexCache.AddOrUpdateVertexDelta(vertexField, log);
            }
            else
            {
                this.Command.Connection.ReplaceOrDeleteDocumentAsync(vertexField.VertexId, vertexObject, 
                    this.Command.Connection.GetDocumentPartition(vertexObject), this.Command).Wait();
            }

            // Update vertex field
            VertexPropertyField vertexPropertyField = vertexField.VertexProperties[vp.PropertyName];
            bool found = vertexPropertyField.Multiples.Remove(vp.PropertyId);
            Debug.Assert(found);
            if (!vertexPropertyField.Multiples.Any()) {
                vertexField.VertexProperties.Remove(vp.PropertyName);
            }

        }

        private void DropVertexPropertyMetaProperty(ValuePropertyField metaProperty)
        {
            Debug.Assert(metaProperty.Parent is VertexSinglePropertyField);
#if DEBUG
            VertexSinglePropertyField vsp = (VertexSinglePropertyField)metaProperty.Parent;
            VertexField vertex = vsp.VertexProperty.Vertex;
            if (!vertex.ViaGraphAPI)
            {
                Debug.Assert(vertex.VertexJObject[vsp.PropertyName] is JArray);
            }
#endif

            VertexSinglePropertyField vertexSingleProperty = (VertexSinglePropertyField)metaProperty.Parent;

            VertexField vertexField = vertexSingleProperty.VertexProperty.Vertex;
            JObject vertexObject = vertexField.VertexJObject;

            Debug.Assert(vertexObject[vertexSingleProperty.PropertyName] != null);

            JToken propertyJToken = ((JArray) vertexObject[vertexSingleProperty.PropertyName])
                .First(singleProperty => (string) singleProperty[KW_PROPERTY_ID] == vertexSingleProperty.PropertyId);

            JObject metaPropertyJObject = (JObject) propertyJToken?[KW_PROPERTY_META];

            if (metaPropertyJObject != null)
            {
                metaPropertyJObject.Property(metaProperty.PropertyName)?.Remove();
                if (metaPropertyJObject.Count == 0)
                {
                    ((JObject)propertyJToken).Remove(KW_PROPERTY_META);
                }
            }

            if (this.Command.InLazyMode)
            {
                DeltaLogDropVertexMetaProperty log = new DeltaLogDropVertexMetaProperty(metaProperty.PropertyName,
                    vertexSingleProperty.PropertyName, vertexSingleProperty.PropertyId);
                this.Command.VertexCache.AddOrUpdateVertexDelta(vertexField, log);
            }
            else
            {
                // Update DocDB
                this.Command.Connection.ReplaceOrDeleteDocumentAsync(vertexField.VertexId, vertexObject,
                    this.Command.Connection.GetDocumentPartition(vertexObject), this.Command).Wait();
            }

            // Update vertex field
            vertexSingleProperty.MetaProperties.Remove(metaProperty.PropertyName);
        }

        private void DropEdgeProperty(EdgePropertyField ep)
        {
            string propertyName = ep.PropertyName;
            EdgeField edgeField = ep.Edge;
            string edgeId = edgeField.EdgeId;

            string srcVertexId = edgeField.OutV;
            string srcVertexPartition = edgeField.OutVPartition;
            VertexField srcVertexField = this.Command.VertexCache.GetVertexField(srcVertexId, srcVertexPartition);
            JObject srcVertexObject = srcVertexField.VertexJObject;

            VertexField sinkVertexField;
            JObject sinkVertexObject;
            string sinkVertexId = edgeField.InV;
            string sinkVertexPartition = edgeField.InVPartition;

            bool foundSink;
            if (this.Command.Connection.UseReverseEdges)
            {
                sinkVertexField = this.Command.VertexCache.GetVertexField(sinkVertexId, sinkVertexPartition);
                sinkVertexObject = sinkVertexField.VertexJObject;
                foundSink = true;
            }
            else
            {
                foundSink = this.Command.VertexCache.TryGetVertexField(sinkVertexId, out sinkVertexField);
                sinkVertexObject = sinkVertexField?.VertexJObject;
            }

            EdgeField outEdgeField = srcVertexField.AdjacencyList.GetEdgeField(edgeId, true);
            EdgeField inEdgeField = null;
            if (this.Command.Connection.UseReverseEdges)
            {
                inEdgeField = sinkVertexField?.RevAdjacencyList.GetEdgeField(edgeId, true);
            }

            JObject outEdgeObject = outEdgeField.EdgeJObject;
            string outEdgeDocId = null;

            JObject inEdgeObject = inEdgeField?.EdgeJObject;
            string inEdgeDocId = null;

            if (!this.Command.InLazyMode)
            {
                EdgeDocumentHelper.FindEdgeBySourceAndEdgeId(
                    this.Command, srcVertexObject, srcVertexId, edgeId, false,
                    out outEdgeObject, out outEdgeDocId);

                if (outEdgeObject == null)
                {
                    Debug.WriteLine(
                        $"[DropEdgeProperty] The edge does not exist: vertexId = {srcVertexId}, edgeId = {edgeId}");
                    return;
                }

                if (this.Command.Connection.UseReverseEdges)
                {
                    Debug.Assert(foundSink);

                    if (sinkVertexId.Equals(srcVertexId))
                    {
                        Debug.Assert(object.ReferenceEquals(sinkVertexField, srcVertexField));
                        Debug.Assert(object.ReferenceEquals(sinkVertexObject, srcVertexObject));
                        inEdgeDocId = outEdgeDocId;
                    }
                    else
                    {
                        EdgeDocumentHelper.FindEdgeBySourceAndEdgeId(
                            this.Command, sinkVertexObject, srcVertexId, edgeId, true,
                            out inEdgeObject, out inEdgeDocId);
                    }
                }
            }

            // Modify edgeObject (drop the edge property)
            GraphViewJsonCommand.DropProperty(outEdgeObject, propertyName);
            // Update VertexCache
            outEdgeField.EdgeProperties.Remove(propertyName);

            if (this.Command.Connection.UseReverseEdges && inEdgeField != null)
            {
                // Modify edgeObject (drop the edge property)
                GraphViewJsonCommand.DropProperty(inEdgeObject, propertyName);
                // Update VertexCache
                inEdgeField.EdgeProperties.Remove(propertyName);
            }

            if (this.Command.InLazyMode)
            {
                DeltaLogDropEdgeProperty log = new DeltaLogDropEdgeProperty(propertyName);
                this.Command.VertexCache.AddOrUpdateEdgeDelta(outEdgeField, srcVertexField,
                    inEdgeField, sinkVertexField, log, this.Command.Connection.UseReverseEdges);
            }
            else
            {
                // Interact with DocDB to update the property 
                EdgeDocumentHelper.UpdateEdgeProperty(this.Command, srcVertexObject, outEdgeDocId, false,
                    outEdgeObject);
                if (this.Command.Connection.UseReverseEdges)
                {
                    EdgeDocumentHelper.UpdateEdgeProperty(this.Command, sinkVertexObject, inEdgeDocId, true,
                        inEdgeObject);
                }
            }
        }

        internal override RawRecord DataModify(RawRecord record)
        {
            FieldObject dropTarget = record[this.dropTargetIndex];

            VertexField vertexField = dropTarget as VertexField;;
            if (vertexField != null) {
                this.DropVertex(vertexField);
                return null;
            }

            EdgeField edgeField = dropTarget as EdgeField;
            if (edgeField != null)
            {
                this.DropEdge(edgeField);
                return null;
            }

            PropertyField property = dropTarget as PropertyField;
            if (property != null)
            {
                if (property is VertexSinglePropertyField)
                {
                    this.DropVertexSingleProperty((VertexSinglePropertyField)property);
                }
                else if (property is EdgePropertyField)
                {
                    this.DropEdgeProperty((EdgePropertyField)property);
                }
                else
                {
                    this.DropVertexPropertyMetaProperty((ValuePropertyField)property);
                }

                return null;
            }

            // Should not reach here
            throw new GraphViewException("The incoming object is not removable");
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("dropTargetIndex", this.dropTargetIndex);
        }

        protected DropOperator(SerializationInfo info, StreamingContext context) : base(info, context)
        {
            this.dropTargetIndex = info.GetInt32("dropTargetIndex");
        }
    }

    [Serializable]
    internal class UpdatePropertiesOperator : ModificationBaseOperator
    {
        private readonly int updateTargetIndex;
        private readonly List<PropertyTuple> updateProperties;

        public UpdatePropertiesOperator(
            GraphViewExecutionOperator dummyInputOp,
            GraphViewCommand command,
            int updateTargetIndex,
            List<PropertyTuple> updateProperties)
            : base(dummyInputOp, command)
        {
            this.updateTargetIndex = updateTargetIndex;
            this.updateProperties = updateProperties;
        }

        private void UpdatePropertiesOfVertex(VertexField vertex, RawRecord record)
        {
            JObject vertexDocument = vertex.VertexJObject;

            foreach (PropertyTuple property in this.updateProperties)
            {
                string name = property.Name;
                if (name == this.Command.Connection.RealPartitionKey)
                {
                    throw new GraphViewException("Updating the partition-by property is not supported.");
                }

                if (!vertex.ViaGraphAPI && vertexDocument[name] is JValue)
                {
                    // Add/Update an existing flat vertex property
                    throw new GraphViewException($"The adding/updating property '{name}' already exists as flat.");
                }

                // Construct single property
                JObject meta = new JObject();
                List<Tuple<string, JValue>> metaList = new List<Tuple<string, JValue>>();
                foreach (KeyValuePair<string, Tuple<StringField, ScalarSubqueryFunction>> pair in property.MetaProperties)
                {
                    JValue metaValue = property.GetMetaPropertyJValue(pair.Key, record);
                    meta[pair.Key] = metaValue;
                    metaList.Add(new Tuple<string, JValue>(pair.Key, metaValue));
                }

                string propertyId = GraphViewConnection.GenerateDocumentId();
                JValue value = property.GetPropertyJValue(record);
                Debug.Assert(value != null);
                JObject singleProperty = new JObject
                {
                    [KW_PROPERTY_VALUE] = value,
                    [KW_PROPERTY_ID] = propertyId,
                };
                if (meta.Count > 0)
                {
                    singleProperty[KW_PROPERTY_META] = meta;
                }

                // Set / Append to multiProperty
                JArray multiProperty;
                if (vertexDocument[name] == null)
                {
                    multiProperty = new JArray();
                    vertexDocument[name] = multiProperty;
                }
                else
                {
                    multiProperty = (JArray)vertexDocument[name];
                }
                bool isMultiProperty = property.Cardinality != GremlinKeyword.PropertyCardinality.Single;
                if (!isMultiProperty)
                {
                    multiProperty.Clear();
                }
                multiProperty.Add(singleProperty);

                if (this.Command.InLazyMode)
                {
                    DeltaLogUpdateVertexSingleProperty log = new DeltaLogUpdateVertexSingleProperty(name,
                        value, propertyId, isMultiProperty, metaList);
                    this.Command.VertexCache.AddOrUpdateVertexDelta(vertex, log);
                }

                // Update vertex field
                VertexPropertyField vertexProperty;
                bool existed = vertex.VertexProperties.TryGetValue(name, out vertexProperty);
                if (!existed)
                {
                    vertexProperty = new VertexPropertyField(vertexDocument.Property(name), vertex);
                    vertex.VertexProperties.Add(name, vertexProperty);
                }
                else
                {
                    vertexProperty.Replace(vertexDocument.Property(name));
                }
            }

            if (!this.Command.InLazyMode)
            {
                // Upload to DB
                this.Command.Connection.ReplaceOrDeleteDocumentAsync(
                    vertex.VertexId, vertexDocument,
                    this.Command.Connection.GetDocumentPartition(vertexDocument), this.Command).Wait();
            }

        }

        private void UpdatePropertiesOfEdge(EdgeField edgeField, RawRecord record)
        {
            List<Tuple<string, JValue>> propertyList = new List<Tuple<string, JValue>>();
            foreach (PropertyTuple property in this.updateProperties)
            {
                if (property.Cardinality == GremlinKeyword.PropertyCardinality.List || property.MetaProperties.Count > 0)
                {
                    throw new Exception("Can't create meta property or duplicated property on edges");
                }

                propertyList.Add(new Tuple<string, JValue>(property.Name, property.GetPropertyJValue(record)));
            }

            string edgeId = edgeField.EdgeId;

            string srcVertexId = edgeField.OutV;
            string srcVertexPartition = edgeField.OutVPartition;
            VertexField srcVertexField = this.Command.VertexCache.GetVertexField(srcVertexId, srcVertexPartition);
            JObject srcVertexObject = srcVertexField.VertexJObject;

            VertexField sinkVertexField;
            JObject sinkVertexObject;
            string sinkVertexId = edgeField.InV;
            string sinkVertexPartition = edgeField.InVPartition;

            bool foundSink;
            if (this.Command.Connection.UseReverseEdges)
            {
                sinkVertexField = this.Command.VertexCache.GetVertexField(sinkVertexId, sinkVertexPartition);
                sinkVertexObject = sinkVertexField.VertexJObject;
                foundSink = true;
            }
            else
            {
                foundSink = this.Command.VertexCache.TryGetVertexField(sinkVertexId, out sinkVertexField);
                sinkVertexObject = sinkVertexField?.VertexJObject;
            }

            EdgeField outEdgeField = srcVertexField.AdjacencyList.GetEdgeField(edgeId, true);
            EdgeField inEdgeField = null;
            if (this.Command.Connection.UseReverseEdges)
            {
                inEdgeField = sinkVertexField?.RevAdjacencyList.GetEdgeField(edgeId, true);
            }

            JObject outEdgeObject = outEdgeField.EdgeJObject;
            string outEdgeDocId = null;

            JObject inEdgeObject = inEdgeField?.EdgeJObject;
            string inEdgeDocId = null;

            if (!this.Command.InLazyMode)
            {
                EdgeDocumentHelper.FindEdgeBySourceAndEdgeId(
                    this.Command, srcVertexObject, srcVertexId, edgeId, false,
                    out outEdgeObject, out outEdgeDocId);

                if (outEdgeObject == null)
                {
                    Debug.WriteLine($"[UpdateEdgeProperties] The edge does not exist: vertexId = {srcVertexId}, edgeId = {edgeId}");
                    return;
                }

                if (this.Command.Connection.UseReverseEdges)
                {
                    Debug.Assert(foundSink);

                    if (sinkVertexId.Equals(srcVertexId))
                    {
                        Debug.Assert(object.ReferenceEquals(sinkVertexField, srcVertexField));
                        Debug.Assert(object.ReferenceEquals(sinkVertexObject, srcVertexObject));
                        inEdgeDocId = outEdgeDocId;
                    }
                    else
                    {
                        EdgeDocumentHelper.FindEdgeBySourceAndEdgeId(
                            this.Command, sinkVertexObject, srcVertexId, edgeId, true,
                            out inEdgeObject, out inEdgeDocId);
                    }
                }
            }

            List<Tuple<string, JValue>> deltaProperties = new List<Tuple<string, JValue>>();

            foreach (Tuple<string, JValue> tuple in propertyList)
            {
                string name = tuple.Item1;
                JValue value = tuple.Item2;

                // Modify edgeObject (update the edge property)
                JProperty updatedProperty = GraphViewJsonCommand.UpdateProperty(outEdgeObject, name, value);
                // Update VertexCache
                outEdgeField.UpdateEdgeProperty(updatedProperty);
                if (this.Command.InLazyMode)
                {
                    deltaProperties.Add(new Tuple<string, JValue>(name, value));
                }

                if (this.Command.Connection.UseReverseEdges && inEdgeField != null)
                {
                    // Modify edgeObject (update the edge property)
                    updatedProperty = GraphViewJsonCommand.UpdateProperty(inEdgeObject, name, value);
                    // Update VertexCache
                    inEdgeField.UpdateEdgeProperty(updatedProperty);
                }
            }

            if (this.Command.InLazyMode)
            {
                DeltaLogUpdateEdgeProperties log = new DeltaLogUpdateEdgeProperties(deltaProperties);
                this.Command.VertexCache.AddOrUpdateEdgeDelta(outEdgeField, srcVertexField,
                    inEdgeField, sinkVertexField, log, this.Command.Connection.UseReverseEdges);
            }
            else
            {
                // Interact with DocDB to update the property 
                EdgeDocumentHelper.UpdateEdgeProperty(this.Command, srcVertexObject, outEdgeDocId, false,
                    outEdgeObject);
                if (this.Command.Connection.UseReverseEdges)
                {
                    EdgeDocumentHelper.UpdateEdgeProperty(this.Command, sinkVertexObject, inEdgeDocId, true,
                        inEdgeObject);
                }
            }
        }

        private void UpdateMetaPropertiesOfSingleVertexProperty(VertexSinglePropertyField vp, RawRecord record)
        {
            if (!vp.VertexProperty.Vertex.ViaGraphAPI)
            {
                // We know this property must be added via GraphAPI (if exist)
                JToken prop = vp.VertexProperty.Vertex.VertexJObject[vp.PropertyName];
                Debug.Assert(prop == null || prop is JObject);
            }

            string vertexId = vp.VertexProperty.Vertex.VertexId;
            JObject vertexDocument = vp.VertexProperty.Vertex.VertexJObject;
            JObject singleProperty = (JObject)((JArray)vertexDocument[vp.PropertyName])
                .First(single => (string) single[KW_PROPERTY_ID] == vp.PropertyId);
            JObject meta = (JObject)singleProperty[KW_PROPERTY_META];

            if (meta == null && this.updateProperties.Count > 0)
            {
                meta = new JObject();
                singleProperty[KW_PROPERTY_META] = meta;
            }
            List<Tuple<string, JValue>> metaList = new List<Tuple<string, JValue>>();
            foreach (PropertyTuple property in this.updateProperties)
            {
                if (property.Cardinality == GremlinKeyword.PropertyCardinality.List || property.MetaProperties.Count > 0)
                {
                    throw new Exception("Can't create meta property or duplicated property on vertex-property's meta property");
                }
                JValue value = property.GetPropertyJValue(record);
                meta[property.Name] = value;
                metaList.Add(new Tuple<string, JValue>(property.Name, value));
            }

            // Update vertex single property
            vp.Replace(singleProperty);

            if (this.Command.InLazyMode)
            {
                DeltaLogUpdateVertexMetaPropertyOfSingleProperty log = new DeltaLogUpdateVertexMetaPropertyOfSingleProperty(
                    vp.PropertyName, vp.PropertyId, metaList);
                this.Command.VertexCache.AddOrUpdateVertexDelta(vp.VertexProperty.Vertex, log);
            }
            else
            {
                // Upload to DB
                this.Command.Connection.ReplaceOrDeleteDocumentAsync(vertexId, vertexDocument,
                    this.Command.Connection.GetDocumentPartition(vertexDocument), this.Command).Wait();
            }
            
        }

        internal override RawRecord DataModify(RawRecord record)
        {
            FieldObject updateTarget = record[this.updateTargetIndex];

            VertexField vertex = updateTarget as VertexField; ;
            if (vertex != null)
            {
                this.UpdatePropertiesOfVertex(vertex, record);

                return record;
            }

            EdgeField edge = updateTarget as EdgeField;
            if (edge != null)
            {
                this.UpdatePropertiesOfEdge(edge, record);

                return record;
            }

            PropertyField property = updateTarget as PropertyField;
            if (property != null)
            {
                if (property is VertexSinglePropertyField)
                {
                    this.UpdateMetaPropertiesOfSingleVertexProperty((VertexSinglePropertyField) property, record);
                }
                else
                {
                    throw new GraphViewException($"BUG: updateTarget is {nameof(PropertyField)}: {property.GetType()}");
                }

                return record;
            }

            // Should not reach here
            throw new Exception("BUG: Should not get here!");
        }

        public override RawRecord Next()
        {
            RawRecord srcRecord = null;

            while (InputOperator.State() && (srcRecord = InputOperator.Next()) != null)
            {
                RawRecord result = DataModify(srcRecord);
                if (result == null) continue;

                // Return the srcRecord
                return srcRecord;
            }

            Close();
            return null;
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("updateTargetIndex", this.updateTargetIndex);
            GraphViewSerializer.SerializeList(info, "updateProperties", this.updateProperties);
        }

        protected UpdatePropertiesOperator(SerializationInfo info, StreamingContext context) : base(info, context)
        {
            this.updateTargetIndex = info.GetInt32("updateTargetIndex");
            this.updateProperties = GraphViewSerializer.DeserializeList<PropertyTuple>(info, "updateProperties");
        }
    }

    [Serializable]
    internal class AddEOperator : ModificationBaseOperator
    {
        //
        // if otherVTag == 0, this newly added edge's otherV() is the src vertex.
        // Otherwise, it's the sink vertex
        //
        private int otherVTag;
        //
        // The subquery operator select the vertex ID of source and sink of the edge to be added or deleted
        //
        private Container container;
        private GraphViewExecutionOperator srcSubQueryOp;
        private GraphViewExecutionOperator sinkSubQueryOp;
        //
        // The initial json object string of to-be-inserted edge, waiting to update the edgeOffset field
        //
        private JObject edgeJsonObject;
        private List<string> edgeProperties;
        private List<PropertyTuple> subtraversalProperties;

        public AddEOperator(GraphViewExecutionOperator inputOp, GraphViewCommand command, Container container,
            GraphViewExecutionOperator srcSubQueryOp, GraphViewExecutionOperator sinkSubQueryOp,
            int otherVTag, JObject edgeJsonObject, List<string> projectedFieldList, List<PropertyTuple> subtraversalProperties)
            : base(inputOp, command)
        {
            this.container = container;
            this.srcSubQueryOp = srcSubQueryOp;
            this.sinkSubQueryOp = sinkSubQueryOp;
            this.otherVTag = otherVTag;
            this.edgeJsonObject = edgeJsonObject;
            this.edgeProperties = projectedFieldList;
            this.subtraversalProperties = subtraversalProperties;
        }

        internal override RawRecord DataModify(RawRecord record)
        {
            foreach (PropertyTuple property in this.subtraversalProperties)
            {
                GraphViewJsonCommand.UpdateProperty(this.edgeJsonObject, property.Name, property.GetPropertyJValue(record));
            }

            this.container.ResetTableCache(record);
            srcSubQueryOp.ResetState();
            sinkSubQueryOp.ResetState();
            //
            // Gremlin will only add edge from the first vertex generated by the src subquery 
            // to the first vertex generated by the sink subquery
            //
            RawRecord srcRecord = srcSubQueryOp.Next();
            RawRecord sinkRecord = sinkSubQueryOp.Next();

            VertexField srcVertexField = srcRecord[0] as VertexField;
            VertexField sinkVertexField = sinkRecord[0] as VertexField;

            if (srcVertexField == null || sinkVertexField == null)
            {
                return null;
            }

            string srcId = srcVertexField[KW_DOC_ID].ToValue;
            string sinkId = sinkVertexField[KW_DOC_ID].ToValue;

            JObject srcVertexObject = srcVertexField.VertexJObject;
            JObject sinkVertexObject = sinkVertexField.VertexJObject;
            if (srcId.Equals(sinkId))
            {
                Debug.Assert(ReferenceEquals(sinkVertexObject, srcVertexObject));
                Debug.Assert(ReferenceEquals(sinkVertexField, srcVertexField));
            }

            JObject outEdgeObject, inEdgeObject;
            string outEdgeDocID = null, inEdgeDocID = null;

            outEdgeObject = (JObject)this.edgeJsonObject.DeepClone();
            inEdgeObject = (JObject)this.edgeJsonObject.DeepClone();

            // Add "id" property to edgeObject
            string edgeId = GraphViewConnection.GenerateDocumentId();

            string srcLabel = srcVertexObject[KW_VERTEX_LABEL]?.ToString();
            string sinkLabel = sinkVertexObject[KW_VERTEX_LABEL]?.ToString();
            GraphViewJsonCommand.UpdateEdgeMetaProperty(outEdgeObject, edgeId, false, sinkId, sinkLabel, sinkVertexField.Partition);
            GraphViewJsonCommand.UpdateEdgeMetaProperty(inEdgeObject, edgeId, true, srcId, srcLabel, srcVertexField.Partition);

            if (!this.Command.InLazyMode)
            {
                // Interact with DocDB and add the edge
                // - For a small-degree vertex (now filled into one document), insert the edge in-place
                //     - If the upload succeeds, done!
                //     - If the upload fails with size-limit-exceeded(SLE), put either incoming or outgoing edges into a seperate document
                // - For a large-degree vertex (already spilled)
                //     - Update either incoming or outgoing edges in the seperate edge-document
                //     - If the upload fails with SLE, create a new document to store the edge, and update the vertex document
                EdgeDocumentHelper.InsertEdgeObjectInternal(this.Command, srcVertexObject, srcVertexField, outEdgeObject, false, out outEdgeDocID); // srcVertex uploaded

                if (this.Command.Connection.UseReverseEdges)
                {
                    EdgeDocumentHelper.InsertEdgeObjectInternal(this.Command, sinkVertexObject, sinkVertexField, inEdgeObject, true, out inEdgeDocID); // sinkVertex uploaded
                }
                else
                {
                    inEdgeDocID = EdgeDocumentHelper.VirtualReverseEdgeDocId;
                }
            }

            // Update vertex's adjacency list and reverse adjacency list (in vertex field)
            EdgeField outEdgeField = srcVertexField.AdjacencyList.TryAddEdgeField(
                (string)outEdgeObject[KW_EDGE_ID],
                () => EdgeField.ConstructForwardEdgeField(srcId, srcVertexField.VertexLabel, srcVertexField.Partition, outEdgeDocID, outEdgeObject));

            EdgeField inEdgeField = sinkVertexField.RevAdjacencyList.TryAddEdgeField(
                (string)inEdgeObject[KW_EDGE_ID], 
                () => EdgeField.ConstructBackwardEdgeField(sinkId, sinkVertexField.VertexLabel, sinkVertexField.Partition, inEdgeDocID, inEdgeObject));

            if (this.Command.InLazyMode)
            {
                DeltaLogAddEdge log = new DeltaLogAddEdge();
                this.Command.VertexCache.AddOrUpdateEdgeDelta(outEdgeField, srcVertexField, inEdgeField, sinkVertexField, log, this.Command.Connection.UseReverseEdges);
            }

            // Construct the newly added edge's RawRecord
            RawRecord result = new RawRecord();

            // source, sink, other, edgeId, *
            result.Append(new StringField(srcId));
            result.Append(new StringField(sinkId));
            result.Append(new StringField(otherVTag == 0 ? srcId : sinkId));
            result.Append(new StringField(outEdgeField.EdgeId));
            result.Append(outEdgeField);

            for (int i = GraphViewReservedProperties.ReservedEdgeProperties.Count; i < edgeProperties.Count; i++)
            {
                FieldObject fieldValue = outEdgeField[edgeProperties[i]];
                result.Append(fieldValue);
            }

            return result;
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.container = new Container();
            EnumeratorOperator enumeratorOp = this.srcSubQueryOp.GetFirstOperator() as EnumeratorOperator;
            enumeratorOp.SetContainer(this.container);
            enumeratorOp = this.sinkSubQueryOp.GetFirstOperator() as EnumeratorOperator;
            enumeratorOp.SetContainer(this.container);
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("otherVTag", this.otherVTag);
            info.AddValue("srcSubQueryOp", this.srcSubQueryOp, typeof(GraphViewExecutionOperator));
            info.AddValue("sinkSubQueryOp", this.sinkSubQueryOp, typeof(GraphViewExecutionOperator));
            info.AddValue("edge", this.edgeJsonObject.ToString());
            GraphViewSerializer.SerializeList(info, "edgeProperties", this.edgeProperties);
            GraphViewSerializer.SerializeList(info, "subtraversalProperties", this.subtraversalProperties);
        }

        protected AddEOperator(SerializationInfo info, StreamingContext context) : base(info, context)
        {
            this.otherVTag = info.GetInt32("otherVTag");
            this.srcSubQueryOp = (GraphViewExecutionOperator)info.GetValue("srcSubQueryOp", typeof(GraphViewExecutionOperator));
            this.sinkSubQueryOp = (GraphViewExecutionOperator)info.GetValue("sinkSubQueryOp", typeof(GraphViewExecutionOperator));
            this.edgeJsonObject = JObject.Parse(info.GetString("edge"));
            this.edgeProperties = GraphViewSerializer.DeserializeList<string>(info, "edgeProperties");
            this.subtraversalProperties = GraphViewSerializer.DeserializeList<PropertyTuple>(info, "subtraversalProperties");
        }

    }

    [Serializable]
    internal class CommitOperator : GraphViewExecutionOperator
    {
        [NonSerialized]
        private GraphViewCommand command;
        private GraphViewExecutionOperator inputOp;

        [NonSerialized]
        private Queue<RawRecord> outputBuffer;

        public CommitOperator(GraphViewCommand command, GraphViewExecutionOperator inputOp)
        {
            this.command = command;
            this.command.InLazyMode = true;
            this.inputOp = inputOp;
            this.outputBuffer = new Queue<RawRecord>();

            this.Open();
        }

        public override RawRecord Next()
        {
            RawRecord r = null;
            while (this.inputOp.State() && (r = this.inputOp.Next()) != null)
            {
                this.outputBuffer.Enqueue(new RawRecord(r));
            }

            this.command.VertexCache.UploadDelta();

            if (this.outputBuffer.Count <= 1) this.Close();
            if (this.outputBuffer.Count != 0) return this.outputBuffer.Dequeue();
            return null;
        }

        public override GraphViewExecutionOperator GetFirstOperator()
        {
            return this.inputOp.GetFirstOperator();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.outputBuffer = new Queue<RawRecord>();
            AdditionalSerializationInfo additionalInfo = (AdditionalSerializationInfo)context.Context;
            this.command = additionalInfo.Command;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.Open();
        }
    }
}
