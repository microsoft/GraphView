using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json.Linq;
using static GraphView.GraphViewKeywords;

namespace GraphView
{
    internal abstract class ModificationBaseOpertaor2 : GraphViewExecutionOperator
    {
        protected GraphViewConnection Connection;
        protected GraphViewExecutionOperator InputOperator;

        protected ModificationBaseOpertaor2(GraphViewExecutionOperator inputOp, GraphViewConnection connection)
        {
            InputOperator = inputOp;
            Connection = connection;
            Open();
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


        ///// <summary>
        ///// Given a edge's source node document and its offset, return the edge's sink id and reverse edge's offset
        ///// </summary>
        ///// <param name="edgeJson"></param>
        ///// <param name="edgeOffset"></param>
        ///// <returns></returns>
        //protected List<string> GetSinkIdAndReverseEdgeOffset(string edgeJson, string edgeOffset)
        //{
        //    JObject document = JObject.Parse(edgeJson);
        //    JArray adjList = (JArray)document["_edge"];
        //
        //    return (from edge in adjList.Children<JObject>()
        //            where edge["_ID"].ToString().Equals(edgeOffset)
        //            select new List<string> {
        //                edge["_sink"].ToString(),
        //                edge["_reverse_ID"].ToString()
        //            }).FirstOrDefault();
        //}
    }

    internal class AddVOperator : ModificationBaseOpertaor2
    {
        private readonly JObject _vertexDocument;
        private readonly List<string> _projectedFieldList; 

        public AddVOperator(GraphViewExecutionOperator inputOp, GraphViewConnection connection, JObject vertexDocument, List<string> projectedFieldList)
            : base(inputOp, connection)
        {
            this._vertexDocument = vertexDocument;
            this._projectedFieldList = projectedFieldList;
        }

        internal override RawRecord DataModify(RawRecord record)
        {
            JObject vertexObject = (JObject)this._vertexDocument.DeepClone();

            string vertexId;
            if (vertexObject[KW_DOC_ID] == null) {
                vertexId = GraphViewConnection.GenerateDocumentId();
                vertexObject[KW_DOC_ID] = vertexId;
            }
            else {
                // Only string id is supported!
                // Assume user will not specify duplicated ids
                Debug.Assert(vertexObject[KW_DOC_ID] is JValue);
                Debug.Assert(((JValue)vertexObject[KW_DOC_ID]).Type == JTokenType.String);

                vertexId = (string)vertexObject[KW_DOC_ID];
            }

            Debug.Assert(vertexObject[KW_DOC_PARTITION] == null);
            if (this.Connection.PartitionPathTopLevel == KW_DOC_PARTITION) {

                // Now the collection is created via GraphAPI

                if (vertexObject[this.Connection.RealPartitionKey] == null) {
                    throw new GraphViewException($"AddV: Parition key '{this.Connection.RealPartitionKey}' must be provided.");
                }

                // Special treat "id" or "label" specified as partition key
                JValue partition;
                if (this.Connection.RealPartitionKey == KW_DOC_ID ||
                    this.Connection.RealPartitionKey == KW_VERTEX_LABEL)
                {
                    partition = (JValue)(string)vertexObject[this.Connection.RealPartitionKey];
                }
                else {
                    JValue value = (JValue)vertexObject[this.Connection.RealPartitionKey];
                    partition = value;
                }

                vertexObject[KW_DOC_PARTITION] = partition;
            }

            //
            // NOTE: We don't check whether the partition key exists. Let DocDB do it.
            // If the vertex doesn't have the specified partition key, a DocumentClientException will be thrown.
            //
            try {
                this.Connection.CreateDocumentAsync(vertexObject).Wait();
            }
            catch (AggregateException ex) {
                throw new GraphViewException("Error when uploading the vertex" ,ex.InnerException);
            }


            VertexField vertexField = Connection.VertexCache.AddOrUpdateVertexField(vertexId, vertexObject);

            RawRecord result = new RawRecord();

            foreach (string fieldName in _projectedFieldList)
            {
                FieldObject fieldValue = vertexField[fieldName];

                result.Append(fieldValue);
            }

            return result;
        }

    }

    internal class DropOperator : ModificationBaseOpertaor2
    {
        private readonly int dropTargetIndex;
        private readonly GraphViewExecutionOperator dummyInputOp;

        public DropOperator(GraphViewExecutionOperator dummyInputOp, GraphViewConnection connection, int dropTargetIndex)
            : base(dummyInputOp, connection)
        {
            this.dropTargetIndex = dropTargetIndex;
            this.dummyInputOp = dummyInputOp;
        }

        private void DropVertex(VertexField vertexField)
        {
            RawRecord record = new RawRecord();
            record.Append(new StringField(vertexField.VertexId));  // nodeIdIndex
            DropNodeOperator op = new DropNodeOperator(this.dummyInputOp, this.Connection, 0);
            op.DataModify(record);

            // Now VertexCacheObject has been updated (in DataModify)
        }

        private void DropEdge(EdgeField edgeField)
        {
            RawRecord record = new RawRecord();
            record.Append(edgeField);
            DropEdgeOperator op = new DropEdgeOperator(this.dummyInputOp, this.Connection, 0);
            op.DataModify(record);

            // Now VertexCacheObject has been updated (in DataModify)
        }

        private void DropVertexProperty(VertexPropertyField vp)
        {
            // Update DocDB
            VertexField vertexField = vp.Vertex;
            JObject vertexObject = vertexField.VertexJObject;

            Debug.Assert(vertexObject[vp.PropertyName] != null);
            vertexObject.Property(vp.PropertyName).Remove();

            this.Connection.ReplaceOrDeleteDocumentAsync(vertexField.VertexId, vertexObject, 
                this.Connection.GetDocumentPartition(vertexObject)).Wait();

            // Update vertex field
            vertexField.VertexProperties.Remove(vp.PropertyName);
        }

        private void DropVertexSingleProperty(VertexSinglePropertyField vp)
        {
            //if (!vp.VertexProperty.Vertex.ViaGraphAPI) {
            //    // Just drop the whole property!
            //    this.DropVertexProperty(vp.VertexProperty);
            //    return;
            //}
            if (vp.PropertyName == this.Connection.RealPartitionKey) {
                throw new GraphViewException("Drop the partition-by property is not supported");
            }

            // Update DocDB
            VertexField vertexField = vp.VertexProperty.Vertex;
            JObject vertexObject = vertexField.VertexJObject;

            Debug.Assert(vertexObject[vp.PropertyName] != null);

            JArray vertexProperty = (JArray)vertexObject[vp.PropertyName];
            vertexProperty
                .First(singleProperty => (string)singleProperty[GraphViewKeywords.KW_PROPERTY_ID] == vp.PropertyId)
                .Remove();
            if (vertexProperty.Count == 0) {
               vertexObject.Property(vp.PropertyName).Remove();
            }

            this.Connection.ReplaceOrDeleteDocumentAsync(vertexField.VertexId, vertexObject, this.Connection.GetDocumentPartition(vertexObject)).Wait();

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
            if (!vertex.ViaGraphAPI) {
                Debug.Assert(vertex.VertexJObject[vsp.PropertyName] is JArray);
                ////throw new GraphViewException("BUG: Compatible vertices should not have meta properties.");
            }
#endif

            Debug.Assert(metaProperty.Parent is VertexSinglePropertyField);
            VertexSinglePropertyField vertexSingleProperty = (VertexSinglePropertyField)metaProperty.Parent;

            VertexField vertexField = vertexSingleProperty.VertexProperty.Vertex;
            JObject vertexObject = vertexField.VertexJObject;

            Debug.Assert(vertexObject[vertexSingleProperty.PropertyName] != null);

            JToken propertyJToken = ((JArray) vertexObject[vertexSingleProperty.PropertyName])
                .First(singleProperty => (string) singleProperty[KW_PROPERTY_ID] == vertexSingleProperty.PropertyId);

            JObject metaPropertyJObject = (JObject) propertyJToken?[KW_PROPERTY_META];

            if (metaPropertyJObject != null) {
                metaPropertyJObject.Property(metaProperty.PropertyName)?.Remove();
                if (metaPropertyJObject.Count == 0) {
                    ((JObject)propertyJToken).Remove(KW_PROPERTY_META);
                }
            }

            // Update DocDB
            this.Connection.ReplaceOrDeleteDocumentAsync(vertexField.VertexId, vertexObject, 
                this.Connection.GetDocumentPartition(vertexObject)).Wait();

            // Update vertex field
            vertexSingleProperty.MetaProperties.Remove(metaProperty.PropertyName);
        }

        private void DropEdgeProperty(EdgePropertyField ep)
        {
            //EdgeField edgeField = ep.Edge;
            //if (edgeField.EdgeDocID != null) {  // This is a spilled edge
            //    JObject edgeDocument = this.connection.RetrieveDocumentById(edgeField.EdgeDocID);
            //    ((JArray)edgeDocument["_edge"])
            //        .First(edge => (string)edge[KW_EDGE_SRCV] == edgeField.OutV && (long)edge[KW_EDGE_OFFSET] == edgeField.Offset)
            //        [ep.PropertyName]
            //        .Remove();
            //    this.connection.ReplaceOrDeleteDocumentAsync(edgeField.EdgeDocID, edgeDocument).Wait();
            //}
            //else {  // This is not a spilled edge
            //    JObject edgeDocument = this.connection.RetrieveDocumentById(edgeField.EdgeDocID);
            //    ((JArray)edgeDocument["_edge"])
            //        .First(edge => (string)edge[KW_EDGE_SRCV] == edgeField.OutV && (long)edge[KW_EDGE_OFFSET] == edgeField.Offset)
            //        [ep.PropertyName]
            //        .Remove();
            //    this.connection.ReplaceOrDeleteDocumentAsync(edgeField.EdgeDocID, edgeDocument).Wait();
            //}

            //// Update edge field
            //bool found = edgeField.EdgeProperties.Remove(ep.PropertyName);
            //Debug.Assert(found);

            List<Tuple<WValueExpression, WValueExpression, int>> propertyList = new List<Tuple<WValueExpression, WValueExpression, int>>();
            propertyList.Add(
                new Tuple<WValueExpression, WValueExpression, int>(
                    new WValueExpression(ep.PropertyName, true), 
                    new WValueExpression("null", false), 
                    0));
            UpdateEdgePropertiesOperator op = new UpdateEdgePropertiesOperator(
                this.dummyInputOp, 
                this.Connection,
                0, 
                propertyList
                );
            RawRecord record = new RawRecord();
            record.Append(ep.Edge);
            op.DataModify(record);

            // Now VertexCacheObject has been updated (in DataModify)
            Debug.Assert(!ep.Edge.EdgeProperties.ContainsKey(ep.PropertyName));
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
                if (property is VertexPropertyField)
                {
                    this.DropVertexProperty((VertexPropertyField)property);
                }
                else if (property is VertexSinglePropertyField)
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

            //
            // Should not reach here
            //
            throw new GraphViewException("The incoming object is not removable");
            return null;
        }
    }

    internal class UpdatePropertiesOperator : ModificationBaseOpertaor2
    {
        private readonly int updateTargetIndex;
        private readonly List<WPropertyExpression> updateProperties;

        public UpdatePropertiesOperator(
            GraphViewExecutionOperator dummyInputOp,
            GraphViewConnection connection,
            int updateTargetIndex,
            List<WPropertyExpression> updateProperties)
            : base(dummyInputOp, connection)
        {
            this.updateTargetIndex = updateTargetIndex;
            this.updateProperties = updateProperties;
        }

        private void UpdatePropertiesOfVertex(VertexField vertex)
        {
            JObject vertexDocument = vertex.VertexJObject;

            foreach (WPropertyExpression property in this.updateProperties) {
                Debug.Assert(property.Value != null);

                VertexPropertyField vertexProperty;
                string name = property.Key.Value;
                if (name == this.Connection.RealPartitionKey) {
                    throw new GraphViewException("Updating the partition-by property is not supported.");
                }

                if (!vertex.ViaGraphAPI && vertexDocument[name] is JValue) {
                    // Add/Update an existing flat vertex property
                    throw new GraphViewException($"The adding/updating property '{name}' already exists as flat.");
                }

                // Construct single property
                JObject meta = new JObject();
                foreach (KeyValuePair<WValueExpression, WValueExpression> pair in property.MetaProperties) {
                    meta[pair.Key.Value] = pair.Value.ToJValue();
                }
                JObject singleProperty = new JObject {
                    [KW_PROPERTY_VALUE] = property.Value.ToJValue(),
                    [KW_PROPERTY_ID] = GraphViewConnection.GenerateDocumentId(),
                };
                if (meta.Count > 0) {
                    singleProperty[KW_PROPERTY_META] = meta;
                }

                // Set / Append to multiProperty
                JArray multiProperty;
                if (vertexDocument[name] == null) {
                    multiProperty = new JArray();
                    vertexDocument[name] = multiProperty;
                }
                else {
                    multiProperty = (JArray)vertexDocument[name];
                }

                if (property.Cardinality == GremlinKeyword.PropertyCardinality.Single) {
                    multiProperty.Clear();
                }
                multiProperty.Add(singleProperty);

                // Update vertex field
                bool existed = vertex.VertexProperties.TryGetValue(name, out vertexProperty);
                if (!existed) {
                    vertexProperty = new VertexPropertyField(vertexDocument.Property(name), vertex);
                    vertex.VertexProperties.Add(name, vertexProperty);
                }
                else {
                    vertexProperty.Replace(vertexDocument.Property(name));
                }
            }

            // Upload to DocDB
            this.Connection.ReplaceOrDeleteDocumentAsync(
                vertex.VertexId, vertexDocument,
                this.Connection.GetDocumentPartition(vertexDocument)).Wait();
        }

        private void UpdatePropertiesOfEdge(EdgeField edge)
        {
            List<Tuple<WValueExpression, WValueExpression, int>> propertyList =
                new List<Tuple<WValueExpression, WValueExpression, int>>();
            foreach (WPropertyExpression property in this.updateProperties) {
                if (property.Cardinality == GremlinKeyword.PropertyCardinality.List ||
                    property.MetaProperties.Count > 0) {
                    throw new Exception("Can't create meta property or duplicated property on edges");
                }

                propertyList.Add(new Tuple<WValueExpression, WValueExpression, int>(property.Key, property.Value, 0));
            }

            RawRecord record = new RawRecord();
            record.Append(edge);
            UpdateEdgePropertiesOperator op = new UpdateEdgePropertiesOperator(this.InputOperator, this.Connection, 0, propertyList);
            op.DataModify(record);
        }

        private void UpdateMetaPropertiesOfSingleVertexProperty(VertexSinglePropertyField vp)
        {
            if (!vp.VertexProperty.Vertex.ViaGraphAPI) {
                //throw new GraphViewException("Update vertex meta property is supported only in pure GraphAPI vertices.");

                // We know this property must be added via GraphAPI (if exist)
                JToken prop = vp.VertexProperty.Vertex.VertexJObject[vp.PropertyName];
                Debug.Assert(prop == null || prop is JObject);
            }

            string vertexId = vp.VertexProperty.Vertex.VertexId;
            JObject vertexDocument = vp.VertexProperty.Vertex.VertexJObject;
            JObject singleProperty = (JObject)((JArray)vertexDocument[vp.PropertyName])
                .First(single => (string) single[KW_PROPERTY_ID] == vp.PropertyId);
            JObject meta = (JObject)singleProperty[KW_PROPERTY_META];

            if (meta == null && this.updateProperties.Count > 0) {
                meta = new JObject();
                singleProperty[KW_PROPERTY_META] = meta;
            }

            foreach (WPropertyExpression property in this.updateProperties) {
                if (property.Cardinality == GremlinKeyword.PropertyCardinality.List ||
                    property.MetaProperties.Count > 0) {
                    throw new Exception("Can't create meta property or duplicated property on vertex-property's meta property");
                }

                meta[property.Key.Value] = property.Value.ToJValue();
            }

            // Update vertex single property
            vp.Replace(singleProperty);

            // Upload to DocDB
            this.Connection.ReplaceOrDeleteDocumentAsync(vertexId, vertexDocument, 
                this.Connection.GetDocumentPartition(vertexDocument)).Wait();
        }

        internal override RawRecord DataModify(RawRecord record)
        {
            FieldObject updateTarget = record[this.updateTargetIndex];

            VertexField vertex = updateTarget as VertexField; ;
            if (vertex != null)
            {
                this.UpdatePropertiesOfVertex(vertex);

                return record;
            }

            EdgeField edge = updateTarget as EdgeField;
            if (edge != null)
            {
                this.UpdatePropertiesOfEdge(edge);

                return record;
            }

            PropertyField property = updateTarget as PropertyField;
            if (property != null)
            {
                if (property is VertexSinglePropertyField)
                {
                    this.UpdateMetaPropertiesOfSingleVertexProperty((VertexSinglePropertyField) property);
                }
                else
                {
                    throw new GraphViewException($"BUG: updateTarget is {nameof(PropertyField)}: {property.GetType()}");
                }

                return record;
            }

            //
            // Should not reach here
            //
            throw new Exception("BUG: Should not get here!");
        }

        public override RawRecord Next()
        {
            RawRecord srcRecord = null;

            while (InputOperator.State() && (srcRecord = InputOperator.Next()) != null)
            {
                RawRecord result = DataModify(srcRecord);
                if (result == null) continue;

                //
                // Return the srcRecord
                //
                return srcRecord;
            }

            Close();
            return null;
        }
    }

    internal class DropNodeOperator : ModificationBaseOpertaor2
    {
        private int _nodeIdIndex;

        public DropNodeOperator(GraphViewExecutionOperator dummyInputOp, GraphViewConnection connection, int pNodeIdIndex)
            : base(dummyInputOp, connection)
        {
            _nodeIdIndex = pNodeIdIndex;
        }

        // TODO: Batch upload for the DropEdge part
        internal override RawRecord DataModify(RawRecord record)
        {
            string vertexId = record[this._nodeIdIndex].ToValue;

            // Temporarily change
            DropEdgeOperator dropEdgeOp = new DropEdgeOperator(null, this.Connection, 0);
            RawRecord temp = new RawRecord(2);

            VertexField vertex = this.Connection.VertexCache.GetVertexField(vertexId);

            // Save a copy of Edges _IDs & drop outgoing edges
            //List<string> outEdgeIds = vertex.AdjacencyList.AllEdges.Select(e => e.EdgeId).ToList();
            //foreach (string outEdgeId in outEdgeIds) {
            //    temp.fieldValues[0] = new StringField(vertexId);
            //    temp.fieldValues[1] = new StringField(outEdgeId);
            //    dropEdgeOp.DataModify(temp);
            //}

            foreach (EdgeField outEdge in vertex.AdjacencyList.AllEdges.ToList()) {
                temp.fieldValues[0] = outEdge;
                dropEdgeOp.DataModify(temp);
            }

            // Save a copy of incoming Edges <srcVertexId, edgeOffsetInSrcVertex> & drop them
            //List<Tuple<string, string>> inEdges = vertex.RevAdjacencyList.AllEdges.Select(
            //    e => new Tuple<string, string>(e.OutV, e.EdgeId)).ToList();
            //foreach (var inEdge in inEdges)
            //{
            //    temp.fieldValues[0] = new StringField(inEdge.Item1); // srcVertexId
            //    temp.fieldValues[1] = new StringField(inEdge.Item2); // edgeIdInSrcVertex
            //    dropEdgeOp.DataModify(temp);
            //}

            foreach (EdgeField inEdge in vertex.RevAdjacencyList.AllEdges.ToList()) {
                temp.fieldValues[0] = inEdge;
                dropEdgeOp.DataModify(temp);
            }

            // Delete the vertex-document!
            JObject vertexObject = vertex.VertexJObject;
#if DEBUG
            if (vertex.ViaGraphAPI) {
                Debug.Assert(vertexObject[KW_VERTEX_EDGE] is JArray);
                if (!EdgeDocumentHelper.IsSpilledVertex(vertexObject, false)) {
                    Debug.Assert(((JArray)vertexObject[KW_VERTEX_EDGE]).Count == 0);
                }

                if (this.Connection.UseReverseEdges) {
                    Debug.Assert(vertexObject[KW_VERTEX_REV_EDGE] is JArray);
                    if (!EdgeDocumentHelper.IsSpilledVertex(vertexObject, true)) {
                        Debug.Assert(((JArray)vertexObject[KW_VERTEX_REV_EDGE]).Count == 0);
                    }
                }
            }
#endif
            this.Connection.ReplaceOrDeleteDocumentAsync(vertexId, null, 
                this.Connection.GetDocumentPartition(vertexObject)).Wait();

            // Update VertexCache
            this.Connection.VertexCache.TryRemoveVertexField(vertexId);

            return null;
        }
    }

    internal class AddEOperator : ModificationBaseOpertaor2
    {
        //
        // if otherVTag == 0, this newly added edge's otherV() is the src vertex.
        // Otherwise, it's the sink vertex
        //
        private int otherVTag;
        //
        // The subquery operator select the vertex ID of source and sink of the edge to be added or deleted
        //
        private ConstantSourceOperator srcSubQuerySourceOp;
        private ConstantSourceOperator sinkSubQuerySouceOp;
        private GraphViewExecutionOperator srcSubQueryOp;
        private GraphViewExecutionOperator sinkSubQueryOp;
        //
        // The initial json object string of to-be-inserted edge, waiting to update the edgeOffset field
        //
        private JObject edgeJsonObject;
        private List<string> edgeProperties;

        public AddEOperator(GraphViewExecutionOperator inputOp, GraphViewConnection connection,
            ConstantSourceOperator srcSubQuerySourceOp, GraphViewExecutionOperator srcSubQueryOp,
            ConstantSourceOperator sinkSubQuerySouceOp, GraphViewExecutionOperator sinkSubQueryOp,
            int otherVTag, JObject edgeJsonObject, List<string> projectedFieldList)
            : base(inputOp, connection)
        {
            this.srcSubQuerySourceOp = srcSubQuerySourceOp;
            this.sinkSubQuerySouceOp = sinkSubQuerySouceOp;
            this.srcSubQueryOp = srcSubQueryOp;
            this.sinkSubQueryOp = sinkSubQueryOp;
            this.otherVTag = otherVTag;
            this.edgeJsonObject = edgeJsonObject;
            this.edgeProperties = projectedFieldList;
        }

        internal override RawRecord DataModify(RawRecord record)
        {
            srcSubQuerySourceOp.ConstantSource = record;
            srcSubQueryOp.ResetState();
            sinkSubQuerySouceOp.ConstantSource = record;
            sinkSubQuerySouceOp.ResetState();
            //
            // Gremlin will only add edge from the first vertex generated by the src subquery 
            // to the first vertex generated by the sink subquery
            //
            RawRecord srcRecord = srcSubQueryOp.Next();
            RawRecord sinkRecord = sinkSubQueryOp.Next();

            VertexField srcVertexField = srcRecord[0] as VertexField;
            VertexField sinkVertexField = sinkRecord[0] as VertexField;

            if (srcVertexField == null || sinkVertexField == null) return null;

            string srcId = srcVertexField[KW_DOC_ID].ToValue;
            string sinkId = sinkVertexField[KW_DOC_ID].ToValue;

            JObject srcVertexObject = srcVertexField.VertexJObject;
            JObject sinkVertexObject = sinkVertexField.VertexJObject;
            if (srcId.Equals(sinkId)) {
                Debug.Assert(ReferenceEquals(sinkVertexObject, srcVertexObject));
                Debug.Assert(ReferenceEquals(sinkVertexField, srcVertexField));
            }


            //
            // Interact with DocDB and add the edge
            // - For a small-degree vertex (now filled into one document), insert the edge in-place
            //     - If the upload succeeds, done!
            //     - If the upload fails with size-limit-exceeded(SLE), put either incoming or outgoing edges into a seperate document
            // - For a large-degree vertex (already spilled)
            //     - Update either incoming or outgoing edges in the seperate edge-document
            //     - If the upload fails with SLE, create a new document to store the edge, and update the vertex document
            //
            JObject outEdgeObject, inEdgeObject;
            string outEdgeDocID, inEdgeDocID;
            EdgeDocumentHelper.InsertEdgeAndUpload(this.Connection,
                                                   srcId, sinkId,
                                                   srcVertexField, sinkVertexField, this.edgeJsonObject,
                                                   srcVertexObject, sinkVertexObject,
                                                   out outEdgeObject, out outEdgeDocID,
                                                   out inEdgeObject, out inEdgeDocID);

            //
            // Update vertex's adjacency list and reverse adjacency list (in vertex field)
            //
            EdgeField outEdgeField = srcVertexField.AdjacencyList.TryAddEdgeField(
                (string)outEdgeObject[KW_EDGE_ID],
                () => EdgeField.ConstructForwardEdgeField(srcId, srcVertexField.VertexLabel, srcVertexField.Partition, outEdgeDocID, outEdgeObject));

            EdgeField inEdgeField = sinkVertexField.RevAdjacencyList.TryAddEdgeField(
                (string)inEdgeObject[KW_EDGE_ID], 
                () => EdgeField.ConstructBackwardEdgeField(sinkId, sinkVertexField.VertexLabel, sinkVertexField.Partition, inEdgeDocID, inEdgeObject));


            // Construct the newly added edge's RawRecord
            RawRecord result = new RawRecord();

            // source, sink, other, edgeId, *
            result.Append(new StringField(srcId));
            result.Append(new StringField(sinkId));
            result.Append(new StringField(otherVTag == 0 ? srcId : sinkId));
            result.Append(new StringField(outEdgeField.EdgeId));
            result.Append(outEdgeField);

            for (int i = GraphViewReservedProperties.ReservedEdgeProperties.Count; i < edgeProperties.Count; i++) {
                FieldObject fieldValue = outEdgeField[edgeProperties[i]];
                result.Append(fieldValue);
            }

            return result;
        }
    }

    internal class DropEdgeOperator : ModificationBaseOpertaor2
    {
        private readonly int edgeFieldIndex;

        public DropEdgeOperator(GraphViewExecutionOperator inputOp, GraphViewConnection connection, int edgeFieldIndex)
            : base(inputOp, connection)
        {
            this.edgeFieldIndex = edgeFieldIndex;
        }

        internal override RawRecord DataModify(RawRecord record)
        {
            EdgeField edgeField = (EdgeField)record[this.edgeFieldIndex];
            string srcId = edgeField.OutV;
            string edgeId = edgeField.EdgeId;
            string srcVertexPartition = edgeField.OutVPartition;

            VertexField srcVertexField = this.Connection.VertexCache.GetVertexField(srcId, srcVertexPartition);
            JObject srcVertexObject = srcVertexField.VertexJObject;
            JObject srcEdgeObject;
            string srcEdgeDocId;
            EdgeDocumentHelper.FindEdgeBySourceAndEdgeId(
                this.Connection, srcVertexObject, srcId, edgeId, false,
                out srcEdgeObject, out srcEdgeDocId);
            if (srcEdgeObject == null)
            {
                //TODO: Check is this condition alright?
                return null;
            }

            string sinkId = (string)srcEdgeObject[KW_EDGE_SINKV];
            string sinkVertexPartition = srcEdgeObject[KW_EDGE_SINKV_PARTITION].ToObject<string>();
            VertexField sinkVertexField = this.Connection.VertexCache.GetVertexField(sinkId, sinkVertexPartition);
            JObject sinkVertexObject = sinkVertexField.VertexJObject;
            string sinkEdgeDocId = null;

            if (this.Connection.UseReverseEdges) {
                if (!string.Equals(sinkId, srcId)) {
                    JObject dummySinkEdgeObject;
                    EdgeDocumentHelper.FindEdgeBySourceAndEdgeId(
                        this.Connection, sinkVertexObject, srcId, edgeId, true,
                        out dummySinkEdgeObject, out sinkEdgeDocId);
                } else {
                    Debug.Assert(object.ReferenceEquals(sinkVertexField, srcVertexField));
                    Debug.Assert(sinkVertexObject == srcVertexObject);
                    sinkEdgeDocId = srcEdgeDocId;
                }
            }

            // <docId, <docJson, partition>>
            Dictionary<string, Tuple<JObject, string>> uploadDocuments = new Dictionary<string, Tuple<JObject, string>>();
            EdgeDocumentHelper.RemoveEdge(uploadDocuments, this.Connection, srcEdgeDocId, srcVertexField, false, srcId, edgeId);
            if (this.Connection.UseReverseEdges) {
                EdgeDocumentHelper.RemoveEdge(uploadDocuments, this.Connection, sinkEdgeDocId, sinkVertexField, true, srcId, edgeId);
            }
            this.Connection.ReplaceOrDeleteDocumentsAsync(uploadDocuments).Wait();

#if DEBUG
            // NOTE: srcVertexObject is excatly the reference of srcVertexField.VertexJObject
            // NOTE: sinkVertexObject is excatly the reference of sinkVertexField.VertexJObject

            // If source vertex is not spilled, the outgoing edge JArray of srcVertexField.VertexJObject should have been updated
            if (!EdgeDocumentHelper.IsSpilledVertex(srcVertexField.VertexJObject, false)) {
                Debug.Assert(
                    srcVertexField.VertexJObject[KW_VERTEX_EDGE].Cast<JObject>().All(
                        edgeObj => (string)edgeObj[KW_EDGE_ID] != edgeId));
            }

            if (this.Connection.UseReverseEdges) {
                // If sink vertex is not spilled, the incoming edge JArray of sinkVertexField.VertexJObject should have been updated
                if (!EdgeDocumentHelper.IsSpilledVertex(srcVertexField.VertexJObject, true)) {
                    Debug.Assert(
                        sinkVertexField.VertexJObject[KW_VERTEX_REV_EDGE].Cast<JObject>().All(
                            edgeObj => (string)edgeObj[KW_EDGE_ID] != edgeId));
                }
            }
#endif

            srcVertexField.AdjacencyList.RemoveEdgeField(edgeId);
            sinkVertexField.RevAdjacencyList.RemoveEdgeField(edgeId);

            return null;
        }
    }

    internal abstract class UpdatePropertiesBaseOperator : ModificationBaseOpertaor2
    {
        internal enum UpdatePropertyMode
        {
            Set,
            Append
        };

        protected UpdatePropertyMode Mode;
        /// <summary>
        /// Item1 is property key.
        /// Item2 is property value. If it is null, then delete the property
        /// Item3 is property's index in the input record. If it is -1, then the input record doesn't contain this property.
        /// </summary>
        // TODO: Now the item3 is useless
        protected List<Tuple<WValueExpression, WValueExpression, int>> PropertiesToBeUpdated;

        protected UpdatePropertiesBaseOperator(GraphViewExecutionOperator inputOp, GraphViewConnection connection,
            List<Tuple<WValueExpression, WValueExpression, int>> pPropertiesToBeUpdated, UpdatePropertyMode pMode = UpdatePropertyMode.Set)
            : base(inputOp, connection)
        {
            PropertiesToBeUpdated = pPropertiesToBeUpdated;
            Mode = pMode;
        }

        public override RawRecord Next()
        {
            RawRecord srcRecord = null;

            while (InputOperator.State() && (srcRecord = InputOperator.Next()) != null)
            {
                RawRecord result = DataModify(srcRecord);
                if (result == null) continue;

                //foreach (var tuple in PropertiesToBeUpdated)
                //{
                //    var propertyIndex = tuple.Item3;
                //    var propertyNewValue = tuple.Item2;
                //    if (propertyIndex == -1) continue;

                //    srcRecord.fieldValues[propertyIndex] = new StringField(propertyNewValue.Value);
                //}

                return srcRecord;
            }

            Close();
            return null;
        }
    }

    //internal class UpdateNodePropertiesOperator : UpdatePropertiesBaseOperator
    //{
    //    private int _nodeIdIndex;

    //    public UpdateNodePropertiesOperator(GraphViewExecutionOperator inputOp, GraphViewConnection connection,
    //                                        int pNodeIndex, List<Tuple<WValueExpression, WValueExpression, int>> pPropertiesList, UpdatePropertyMode pMode = UpdatePropertyMode.Set)
    //        : base(inputOp, connection, pPropertiesList, pMode)
    //    {
    //        _nodeIdIndex = pNodeIndex;
    //    }

    //    internal override RawRecord DataModify(RawRecord record)
    //    {
    //        string vertexId = record[this._nodeIdIndex].ToValue;

    //        JObject vertexDocObject = this.Connection.RetrieveDocumentById(vertexId);

    //        UpdateNodeProperties(vertexId, vertexDocObject, PropertiesToBeUpdated, Mode);
    //        this.Connection.ReplaceOrDeleteDocumentAsync(vertexId, vertexDocObject, (string)vertexDocObject[KW_DOC_PARTITION]).Wait();

    //        // Drop step, return null
    //        if (PropertiesToBeUpdated.Any(t => t.Item2 == null)) return null;
    //        return record;
    //    }

    //    private void UpdateNodeProperties(
    //        string vertexId,
    //        JObject vertexDocObject,
    //        List<Tuple<WValueExpression, WValueExpression, int>> propList,
    //        UpdatePropertyMode mode)
    //    {
    //        VertexField vertexField = this.Connection.VertexCache.GetVertexField(vertexId);

    //        // Drop all non-reserved properties
    //        if (propList.Count == 1 &&
    //            !propList[0].Item1.SingleQuoted &&
    //            propList[0].Item1.Value.Equals("*", StringComparison.OrdinalIgnoreCase) &&
    //            !propList[0].Item2.SingleQuoted &&
    //            propList[0].Item2.Value.Equals("null", StringComparison.OrdinalIgnoreCase))
    //        {
    //            List<string> toBeDroppedPropertiesNames = GraphViewJsonCommand.DropAllNodeProperties(vertexDocObject);
    //            foreach (var propertyName in toBeDroppedPropertiesNames)
    //            {
    //                vertexField.VertexProperties.Remove(propertyName);
    //            }
    //        }
    //        else
    //        {
    //            foreach (var t in propList)
    //            {
    //                WValueExpression keyExpression = t.Item1;
    //                WValueExpression valueExpression = t.Item2;

    //                if (mode == UpdatePropertyMode.Set) {
    //                    string name = (string)keyExpression.ToJValue();
    //                    JValue value = valueExpression.ToJValue();

    //                    if (value == null) {
    //                        vertexField.VertexProperties.Remove(keyExpression.Value);
    //                    }
    //                    else {
    //                        string propertyId;
    //                        if (vertexField.VertexProperties.ContainsKey(name)) {
    //                            // TODO: HACK
    //                            propertyId = vertexField.VertexProperties[name].Multiples.Values.First().PropertyId;
    //                        }
    //                        else {
    //                            propertyId = GraphViewConnection.GenerateDocumentId();
    //                        }

    //                        JProperty multiProperty = new JProperty(name) {
    //                            Value = new JArray {
    //                                new JObject {
    //                                    [KW_PROPERTY_VALUE] = value,
    //                                    [KW_PROPERTY_ID] = propertyId,
    //                                    [KW_PROPERTY_META] = new JObject(),
    //                                }
    //                            }
    //                        };
    //                        vertexDocObject[multiProperty.Name] = multiProperty.Value;
    //                        vertexField.ReplaceProperty(multiProperty);
    //                    }
    //                }
    //                else {
    //                    throw new NotImplementedException();
    //                }
    //            }
    //        }
    //    }
    //}

    internal class UpdateEdgePropertiesOperator : UpdatePropertiesBaseOperator
    {
        private readonly int edgeFieldIndex;

        public UpdateEdgePropertiesOperator(
            GraphViewExecutionOperator inputOp, GraphViewConnection connection,
            int edgeFieldIndex,
            List<Tuple<WValueExpression, WValueExpression, int>> propertiesList,
            UpdatePropertyMode pMode = UpdatePropertyMode.Set)
            : base(inputOp, connection, propertiesList, pMode)
        {
            this.edgeFieldIndex = edgeFieldIndex;
        }

        internal override RawRecord DataModify(RawRecord record)
        {
            EdgeField edgeField = (EdgeField)record[this.edgeFieldIndex];
            string edgeId = edgeField.EdgeId;
            string srcVertexId = edgeField.OutV;
            string srcVertexPartition = edgeField.OutVPartition;

            VertexField srcVertexField = this.Connection.VertexCache.GetVertexField(srcVertexId, srcVertexPartition);
            JObject srcVertexObject = srcVertexField.VertexJObject;
            string outEdgeDocId;
            JObject outEdgeObject;
            EdgeDocumentHelper.FindEdgeBySourceAndEdgeId(
                this.Connection, srcVertexObject, srcVertexId, edgeId, false,
                out outEdgeObject, out outEdgeDocId);
            if (outEdgeObject == null) {
                // TODO: Is there something wrong?
                Debug.WriteLine($"[UpdateEdgePropertiesOperator] The edge does not exist: vertexId = {srcVertexId}, edgeId = {edgeId}");
                return null;
            }

            string sinkVertexId = (string)outEdgeObject[KW_EDGE_SINKV];
            string sinkVertexPartition = outEdgeObject[KW_EDGE_SINKV_PARTITION].ToObject<string>();
            VertexField sinkVertexField;
            JObject sinkVertexObject;
            bool foundSink;
            if (this.Connection.UseReverseEdges) {
                sinkVertexField = this.Connection.VertexCache.GetVertexField(sinkVertexId, sinkVertexPartition);
                sinkVertexObject = sinkVertexField.VertexJObject;
                foundSink = true;
            }
            else {
                foundSink = this.Connection.VertexCache.TryGetVertexField(sinkVertexId, out sinkVertexField);
                sinkVertexObject = sinkVertexField?.VertexJObject;
            }

            string inEdgeDocId = null;
            JObject inEdgeObject = null;

            if (this.Connection.UseReverseEdges) {
                Debug.Assert(foundSink);

                if (sinkVertexId.Equals(srcVertexId)) {
                    Debug.Assert(object.ReferenceEquals(sinkVertexField, srcVertexField));
                    Debug.Assert(object.ReferenceEquals(sinkVertexObject, srcVertexObject));
                }

                EdgeDocumentHelper.FindEdgeBySourceAndEdgeId(
                    this.Connection, sinkVertexObject, srcVertexId, edgeId, true,
                    out inEdgeObject, out inEdgeDocId);
            }

            EdgeField outEdgeField = srcVertexField.AdjacencyList.GetEdgeField(edgeId, true);

            // `inEdgeField` can be null in two cases:
            //   - `sinkVertexField` is null
            //   - `sinkVertexField` is in VertexCache, but its rev-edges are stilled lazy.
            // NOTE: if UseReverseEdge is true, we have to fetch rev-edge to update its property.
            EdgeField inEdgeField = sinkVertexField?.RevAdjacencyList.GetEdgeField(edgeId, false);

            // Drop all non-reserved properties
            if (this.PropertiesToBeUpdated.Count == 1 &&
                !this.PropertiesToBeUpdated[0].Item1.SingleQuoted &&
                this.PropertiesToBeUpdated[0].Item1.Value.Equals("*", StringComparison.OrdinalIgnoreCase) &&
                !this.PropertiesToBeUpdated[0].Item2.SingleQuoted &&
                this.PropertiesToBeUpdated[0].Item2.Value.Equals("null", StringComparison.OrdinalIgnoreCase))
            {
                throw new Exception("BUG: This condition is obsolete. Code should not reach here now!");
            }
            else
            {
                foreach (Tuple<WValueExpression, WValueExpression, int> tuple in this.PropertiesToBeUpdated)
                {
                    WValueExpression keyExpression = tuple.Item1;
                    WValueExpression valueExpression = tuple.Item2;

                    if (this.Mode == UpdatePropertyMode.Set)
                    {
                        // Modify edgeObject (update the edge property)
                        JProperty updatedProperty = GraphViewJsonCommand.UpdateProperty(outEdgeObject, keyExpression, valueExpression);
                        // Update VertexCache
                        if (updatedProperty == null)
                            outEdgeField.EdgeProperties.Remove(keyExpression.Value);
                        else
                            outEdgeField.UpdateEdgeProperty(updatedProperty);

                        if (this.Connection.UseReverseEdges) {
                            // Modify edgeObject (update the edge property)
                            updatedProperty = GraphViewJsonCommand.UpdateProperty(inEdgeObject, keyExpression, valueExpression);
                        }

                        // Update VertexCache (if found)
                        if (inEdgeField != null) {
                            Debug.Assert(foundSink);
                            if (updatedProperty == null)
                                inEdgeField.EdgeProperties.Remove(keyExpression.Value);
                            else
                                inEdgeField.UpdateEdgeProperty(updatedProperty);
                        }
                    }
                    else {
                        throw new GraphViewException("Edges can't have duplicated-name properties.");
                    }
                }
            }

            // Interact with DocDB to update the property 
            EdgeDocumentHelper.UpdateEdgeProperty(this.Connection, srcVertexObject, outEdgeDocId, false, outEdgeObject);
            if (this.Connection.UseReverseEdges) {
                EdgeDocumentHelper.UpdateEdgeProperty(this.Connection, sinkVertexObject, inEdgeDocId, true, inEdgeObject);
            }

            //
            // Drop edge property
            //
            if (this.PropertiesToBeUpdated.Any(t => t.Item2 == null)) return null;
            return record;
        }

        //private Dictionary<string, string> UpdateEdgeAndReverseEdgeProperties(string srcId, string edgeOffset,
        //    string sinkId, string revEdgeOffset, string srcJsonDocument, string sinkJsonDocument,
        //    EdgeField edgeField, EdgeField revEdgeField, UpdatePropertyMode mode)
        //{
        //    var documentsMap = new Dictionary<string, string>();
        //    documentsMap.Add(srcId, srcJsonDocument);
        //    if (!documentsMap.ContainsKey(sinkId)) documentsMap.Add(sinkId, sinkJsonDocument);

        //    UpdateEdgeProperties(documentsMap, PropertiesToBeUpdated, srcId, edgeOffset, false, edgeField, mode);
        //    UpdateEdgeProperties(documentsMap, PropertiesToBeUpdated, sinkId, revEdgeOffset, true, revEdgeField, mode);

        //    return documentsMap;
        //}

        //private void UpdateEdgeProperties(Dictionary<string, string> documentsMap, List<Tuple<WValueExpression, WValueExpression, int>> propList, 
        //    string id, string edgeOffset, bool isReverseEdge, EdgeField edgeField, UpdatePropertyMode mode)
        //{
        //    var document = JObject.Parse(documentsMap[id]);
        //    var adj = isReverseEdge ? (JArray)document["_reverse_edge"] : (JArray)document["_edge"];
        //    var edgeId = int.Parse(edgeOffset);

        //    foreach (var edge in adj.Children<JObject>())
        //    {
        //        if (int.Parse(edge["_ID"].ToString()) != edgeId) continue;

        //        foreach (var t in propList)
        //        {
        //            var keyExpression = t.Item1;
        //            var valueExpression = t.Item2;

        //            if (mode == UpdatePropertyMode.Set)
        //            {
        //                var updatedProperty = GraphViewJsonCommand.UpdateProperty(edge, keyExpression, valueExpression);
        //                if (updatedProperty == null)
        //                    edgeField.EdgeProperties.Remove(keyExpression.Value);
        //                else
        //                    edgeField.UpdateEdgeProperty(updatedProperty.Name, updatedProperty.Value.ToString());
        //            }
        //            else
        //            {
        //                throw new NotImplementedException();
        //            }
        //        }
        //        break;
        //    }

        //    documentsMap[id] = document.ToString();
        //}
    }
}
