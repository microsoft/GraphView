using System;
using System.Collections.Generic;
using System.Diagnostics;
using Newtonsoft.Json.Linq;

namespace GraphView
{
    internal enum VertexDeltaType
    {
        DropSingleProperty,
        DropPropertyMetaProperty,
        UpdateSingleProperty,
        UpdateMultiProperty, // updateSingleProperty and isMutiple = true
        UpdateMetaPropertiesOfSingleProperty
    };

    internal enum EdgeDeltaType
    {
        UpdateProperty,
        DropProperty,
    };

    internal class DeltaVertexField
    {
        private VertexField vertexField;

        private bool isAddV = false;
        private bool isDropV = false;

        //                  name               id                            value            meta-name                      meta-value
        private Dictionary<string, Dictionary<string, Tuple<VertexDeltaType, JValue, Dictionary<string, Tuple<VertexDeltaType, JValue>>>>> deltaProperties;

        public DeltaVertexField(VertexField vertexField)
        {
            this.vertexField = vertexField;
            this.deltaProperties = new Dictionary<string, Dictionary<string, Tuple<VertexDeltaType, JValue, Dictionary<string, Tuple<VertexDeltaType, JValue>>>>>();
        }

        public void AddDeltaLog(DeltaLog log)
        {
            if (log is DeltaLogAddVertex)
            {
                this.isAddV = true;
            }
            else if (log is DeltaLogDropVertex)
            {
                this.isDropV = true;
                this.deltaProperties.Clear();
            }
            else if (log is DeltaLogDropVertexSingleProperty)
            {
                DeltaLogDropVertexSingleProperty deltaLog = log as DeltaLogDropVertexSingleProperty;
                string name = deltaLog.propertyName;
                string id = deltaLog.propertyId;

                if (!this.deltaProperties.ContainsKey(name))
                {
                    this.deltaProperties[name] = new Dictionary<string, Tuple<VertexDeltaType, JValue, Dictionary<string, Tuple<VertexDeltaType, JValue>>>>();
                }

                this.deltaProperties[name][id] = new Tuple<VertexDeltaType, JValue, Dictionary<string, Tuple<VertexDeltaType, JValue>>>(
                    VertexDeltaType.DropSingleProperty, null, null);
            }
            else if (log is DeltaLogDropVertexMetaProperty)
            {
                DeltaLogDropVertexMetaProperty deltaLog = log as DeltaLogDropVertexMetaProperty;
                string parentName = deltaLog.parentPropertyName;
                string parentId = deltaLog.parentPropertyId;

                if (!this.deltaProperties.ContainsKey(parentName))
                {
                    this.deltaProperties[parentName] = new Dictionary<string, Tuple<VertexDeltaType, JValue, Dictionary<string, Tuple<VertexDeltaType, JValue>>>>();
                }

                if (this.deltaProperties[parentName].ContainsKey(parentId))
                {
                    Tuple<VertexDeltaType, JValue, Dictionary<string, Tuple<VertexDeltaType, JValue>>> singleProperty =
                        this.deltaProperties[parentName][parentId];

                    if (singleProperty.Item1 == VertexDeltaType.DropSingleProperty)
                    {
                        // maybe something wrong, drop a meta-property from a droped single-property
                        return;
                    }

                    Dictionary<string, Tuple<VertexDeltaType, JValue>> metaProperties = singleProperty.Item3;

                    if (metaProperties.ContainsKey(deltaLog.propertyName))
                    {
                        if (metaProperties[deltaLog.propertyName].Item1 == VertexDeltaType.UpdateMetaPropertiesOfSingleProperty)
                        {
                            metaProperties[deltaLog.propertyName] = new Tuple<VertexDeltaType, JValue>(VertexDeltaType.DropPropertyMetaProperty, null);
                        }
                        else if (metaProperties[deltaLog.propertyName].Item1 == VertexDeltaType.UpdateSingleProperty)
                        {
                            metaProperties.Remove(deltaLog.propertyName);
                        }
                        else if (metaProperties[deltaLog.propertyName].Item1 == VertexDeltaType.DropPropertyMetaProperty)
                        {
                            // duplicate drop happens, maybe something wrong?
                        }
                    }
                    else
                    {
                        metaProperties[deltaLog.propertyName] = new Tuple<VertexDeltaType, JValue>(VertexDeltaType.DropPropertyMetaProperty, null);
                    }
                }
                else
                {
                    this.deltaProperties[parentName][parentId] = new Tuple<VertexDeltaType, JValue, Dictionary<string, Tuple<VertexDeltaType, JValue>>>(
                        VertexDeltaType.DropPropertyMetaProperty,
                        null,
                        new Dictionary<string, Tuple<VertexDeltaType, JValue>>());
                    this.deltaProperties[parentName][parentId].Item3[deltaLog.propertyName] = new Tuple<VertexDeltaType, JValue>(
                        VertexDeltaType.DropPropertyMetaProperty, null);
                }
            }
            else if (log is DeltaLogUpdateVertexSingleProperty)
            {
                DeltaLogUpdateVertexSingleProperty deltaLog = log as DeltaLogUpdateVertexSingleProperty;
                string name = deltaLog.propertyName;
                string id = deltaLog.propertyId;

                if (!this.deltaProperties.ContainsKey(name))
                {
                    this.deltaProperties[name] = new Dictionary<string, Tuple<VertexDeltaType, JValue, Dictionary<string, Tuple<VertexDeltaType, JValue>>>>();
                }

                if (!deltaLog.isMultiProperty)
                {
                    this.deltaProperties[name].Clear();
                }

                VertexDeltaType deltaType = VertexDeltaType.UpdateSingleProperty;
                if (deltaLog.isMultiProperty)
                {
                    deltaType = VertexDeltaType.UpdateMultiProperty;
                }

                if (deltaLog.metaProperties.Count > 0)
                {
                    this.deltaProperties[name][id] = new Tuple<VertexDeltaType, JValue, Dictionary<string, Tuple<VertexDeltaType, JValue>>>(
                        deltaType, deltaLog.propertyValue, new Dictionary<string, Tuple<VertexDeltaType, JValue>>());

                    foreach (Tuple<string, JValue> meta in deltaLog.metaProperties)
                    {
                        this.deltaProperties[name][id].Item3[meta.Item1] = new Tuple<VertexDeltaType, JValue>(
                            VertexDeltaType.UpdateSingleProperty, meta.Item2);
                    }
                }
                else
                {
                    this.deltaProperties[name][id] = new Tuple<VertexDeltaType, JValue, Dictionary<string, Tuple<VertexDeltaType, JValue>>>(
                        deltaType, deltaLog.propertyValue, null);
                }
            }
            else if (log is DeltaLogUpdateVertexMetaPropertyOfSingleProperty)
            {
                DeltaLogUpdateVertexMetaPropertyOfSingleProperty deltaLog = log as DeltaLogUpdateVertexMetaPropertyOfSingleProperty;
                string name = deltaLog.propertyName;
                string id = deltaLog.propertyId;

                if (!this.deltaProperties.ContainsKey(name))
                {
                    this.deltaProperties[name] = new Dictionary<string, Tuple<VertexDeltaType, JValue, Dictionary<string, Tuple<VertexDeltaType, JValue>>>>();
                }

                if (!this.deltaProperties[name].ContainsKey(id))
                {
                    this.deltaProperties[name][id] = new Tuple<VertexDeltaType, JValue, Dictionary<string, Tuple<VertexDeltaType, JValue>>>(
                        VertexDeltaType.UpdateMetaPropertiesOfSingleProperty, null, new Dictionary<string, Tuple<VertexDeltaType, JValue>>());
                }

                Debug.Assert(this.deltaProperties[name][id].Item1 != VertexDeltaType.DropSingleProperty);
                Debug.Assert(this.deltaProperties[name][id].Item3 != null);

                foreach (Tuple<string, JValue> meta in deltaLog.metaProperties)
                {
                    this.deltaProperties[name][id].Item3[meta.Item1] = new Tuple<VertexDeltaType, JValue>(
                        VertexDeltaType.UpdateMetaPropertiesOfSingleProperty, meta.Item2);
                }
            }
        }

        public void Upload(GraphViewCommand command)
        {
            if (this.isAddV && this.isDropV)
            {
                return;
            }
            if (this.isDropV)
            {
                command.Connection.ReplaceOrDeleteDocumentAsync(this.vertexField.VertexId, null,
                    command.Connection.GetDocumentPartition(this.vertexField.VertexJObject), command).Wait();
            }
            else if (this.isAddV)
            {
                command.Connection.CreateDocumentAsync(this.vertexField.VertexJObject, command).Wait();
            }
            else if (this.deltaProperties.Count > 0)
            {
                command.Connection.ReplaceOrDeleteDocumentAsync(this.vertexField.VertexId, this.vertexField.VertexJObject,
                    command.Connection.GetDocumentPartition(this.vertexField.VertexJObject), command).Wait();
            }
        }
    }

    internal abstract class DeltaLog { }

    internal class DeltaLogAddVertex : DeltaLog { }

    internal class DeltaLogDropVertex : DeltaLog { }

    internal class DeltaLogDropVertexSingleProperty : DeltaLog
    {
        public string propertyName;
        public string propertyId;

        public DeltaLogDropVertexSingleProperty(string propertyName, string propertyId)
        {
            this.propertyName = propertyName;
            this.propertyId = propertyId;
        }
    }

    internal class DeltaLogDropVertexMetaProperty : DeltaLog
    {
        public string propertyName;
        public string parentPropertyName;
        public string parentPropertyId;

        public DeltaLogDropVertexMetaProperty(string propertyName, string parentPropertyName, string parentPropertyId)
        {
            this.propertyName = propertyName;
            this.parentPropertyName = parentPropertyName;
            this.parentPropertyId = parentPropertyId;
        }
    }

    internal class DeltaLogUpdateVertexSingleProperty : DeltaLog
    {
        public string propertyName;
        public JValue propertyValue;
        public string propertyId;
        public bool isMultiProperty;
        public List<Tuple<string, JValue>> metaProperties;

        public DeltaLogUpdateVertexSingleProperty(string propertyName, JValue propertyValue, string propertyId,
            bool isMultiProperty, List<Tuple<string, JValue>> metaProperties)
        {
            this.propertyName = propertyName;
            this.propertyValue = propertyValue;
            this.propertyId = propertyId;
            this.isMultiProperty = isMultiProperty;
            this.metaProperties = metaProperties;
        }
    }

    internal class DeltaLogUpdateVertexMetaPropertyOfSingleProperty : DeltaLog
    {
        public string propertyName;
        public string propertyId;
        public List<Tuple<string, JValue>> metaProperties;

        public DeltaLogUpdateVertexMetaPropertyOfSingleProperty(string propertyName, string propertyId,
            List<Tuple<string, JValue>> metaProperties)
        {
            this.propertyName = propertyName;
            this.propertyId = propertyId;
            this.metaProperties = metaProperties;
        }
    }

    internal class DeltaEdgeField
    {
        private EdgeField outEdgeField;
        private EdgeField inEdgeField;
        private VertexField srcVertexField;
        private VertexField sinkVertexField;

        private bool UseReverseEdges;
        private bool isAddE = false;
        private bool isDropE = false;

        private Dictionary<string, Tuple<EdgeDeltaType, JValue>> deltaProperties;

        public DeltaEdgeField(EdgeField outEdgeField, VertexField srcVertexField, EdgeField inEdgeField, VertexField sinkVertexField, bool useReverseEdges)
        {
            this.outEdgeField = outEdgeField;
            this.inEdgeField = inEdgeField;
            this.srcVertexField = srcVertexField;
            this.sinkVertexField = sinkVertexField;
            this.UseReverseEdges = useReverseEdges;
            this.deltaProperties = new Dictionary<string, Tuple<EdgeDeltaType, JValue>>();
        }

        public void AddDeltaLog(DeltaLog log)
        {
            if (log is DeltaLogAddEdge)
            {
                this.isAddE = true;
            }
            else if (log is DeltaLogDropEdge)
            {
                this.isDropE = true;
                this.deltaProperties.Clear();
            }
            else if (log is DeltaLogUpdateEdgeProperties)
            {
                DeltaLogUpdateEdgeProperties deltaLog = log as DeltaLogUpdateEdgeProperties;
                foreach (Tuple<string, JValue> property in deltaLog.edgeProperties)
                {
                    string name = property.Item1;
                    this.deltaProperties[name] = new Tuple<EdgeDeltaType, JValue>(EdgeDeltaType.UpdateProperty, property.Item2);
                }
            }
            else if (log is DeltaLogDropEdgeProperty)
            {
                DeltaLogDropEdgeProperty deltaLog = log as DeltaLogDropEdgeProperty;
                string name = deltaLog.propertyName;
                this.deltaProperties[name] = new Tuple<EdgeDeltaType, JValue>(EdgeDeltaType.DropProperty, null);
            }
        }

        public void Upload(GraphViewCommand command)
        {
            if (this.isAddE && this.isDropE)
            {
                return;
            }
            else if (this.isAddE)
            {
                string outEdgeDocId;
                EdgeDocumentHelper.InsertEdgeObjectInternal(command, this.srcVertexField.VertexJObject, this.srcVertexField,
                    this.outEdgeField.EdgeJObject, false, out outEdgeDocId);
                this.outEdgeField.EdgeDocID = outEdgeDocId;

                if (this.UseReverseEdges)
                {
                    string inEdgeDocId;
                    EdgeDocumentHelper.InsertEdgeObjectInternal(command, this.sinkVertexField.VertexJObject, this.sinkVertexField,
                        this.inEdgeField.EdgeJObject, true, out inEdgeDocId);
                    this.inEdgeField.EdgeDocID = inEdgeDocId;
                }
            }
            else if (this.isDropE)
            {
                string edgeId = outEdgeField.EdgeId;
                string srcId = outEdgeField.OutV;
                string sinkId = outEdgeField.InV;

                JObject outEdgeObject;
                string outEdgeDocId;
                EdgeDocumentHelper.FindEdgeBySourceAndEdgeId(
                    command, this.srcVertexField.VertexJObject, srcId, edgeId, false,
                    out outEdgeObject, out outEdgeDocId);

                if (outEdgeObject == null)
                {
                    // something wrong. the edge that we want to drop does not exist in db
                    return;
                }

                string inEdgeDocId = null;
                if (this.UseReverseEdges)
                {
                    if (!string.Equals(sinkId, srcId))
                    {
                        JObject dummySinkEdgeObject;
                        EdgeDocumentHelper.FindEdgeBySourceAndEdgeId(
                            command, this.sinkVertexField.VertexJObject, srcId, edgeId, true,
                            out dummySinkEdgeObject, out inEdgeDocId);
                    }
                    else
                    {
                        Debug.Assert(object.ReferenceEquals(this.sinkVertexField, this.srcVertexField));
                        Debug.Assert(this.sinkVertexField.VertexJObject == this.srcVertexField.VertexJObject);
                        inEdgeDocId = outEdgeDocId;
                    }
                }

                // <docId, <docJson, partition>>
                Dictionary<string, Tuple<JObject, string>> uploadDocuments = new Dictionary<string, Tuple<JObject, string>>();
                EdgeDocumentHelper.RemoveEdge(uploadDocuments, command, outEdgeDocId,
                    this.srcVertexField, false, srcId, edgeId);
                if (this.UseReverseEdges)
                {
                    EdgeDocumentHelper.RemoveEdge(uploadDocuments, command, inEdgeDocId,
                        this.sinkVertexField, true, srcId, edgeId);
                }
                command.Connection.ReplaceOrDeleteDocumentsAsync(uploadDocuments, command).Wait();
            }
            else if (this.deltaProperties.Count > 0)
            {
                string edgeId = outEdgeField.EdgeId;
                string srcId = outEdgeField.OutV;
                string sinkId = outEdgeField.InV;

                JObject outEdgeObject;
                string outEdgeDocId;
                EdgeDocumentHelper.FindEdgeBySourceAndEdgeId(
                    command, this.srcVertexField.VertexJObject, srcId, edgeId, false,
                    out outEdgeObject, out outEdgeDocId);

                if (outEdgeObject == null)
                {
                    // something wrong. the edge that we want to update does not exist in db
                    return;
                }

                string inEdgeDocId = null;
                if (this.UseReverseEdges)
                {
                    if (!string.Equals(sinkId, srcId))
                    {
                        JObject inEdgeObject;
                        EdgeDocumentHelper.FindEdgeBySourceAndEdgeId(
                            command, this.sinkVertexField.VertexJObject, srcId, edgeId, true,
                            out inEdgeObject, out inEdgeDocId);
                    }
                    else
                    {
                        Debug.Assert(object.ReferenceEquals(this.sinkVertexField, this.srcVertexField));
                        Debug.Assert(this.sinkVertexField.VertexJObject == this.srcVertexField.VertexJObject);
                        inEdgeDocId = outEdgeDocId;
                    }
                }

                // Interact with DocDB to update the property 
                EdgeDocumentHelper.UpdateEdgeProperty(command, this.srcVertexField.VertexJObject, outEdgeDocId, false,
                    this.outEdgeField.EdgeJObject);
                if (this.UseReverseEdges)
                {
                    EdgeDocumentHelper.UpdateEdgeProperty(command, this.sinkVertexField.VertexJObject, inEdgeDocId, true,
                        this.inEdgeField.EdgeJObject);
                }
            }
        }
    }

    internal class DeltaLogAddEdge : DeltaLog { }

    internal class DeltaLogDropEdge : DeltaLog { }

    internal class DeltaLogUpdateEdgeProperties : DeltaLog
    {
        public List<Tuple<string, JValue>> edgeProperties;

        public DeltaLogUpdateEdgeProperties(List<Tuple<string, JValue>> edgeProperties)
        {
            this.edgeProperties = edgeProperties;
        }
    }

    internal class DeltaLogDropEdgeProperty : DeltaLog
    {
        public string propertyName;

        public DeltaLogDropEdgeProperty(string propertyName)
        {
            this.propertyName = propertyName;
        }
    }
}
