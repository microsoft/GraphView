using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.ServiceModel;
using Newtonsoft.Json.Linq;
using static GraphView.DocumentDBKeywords;

namespace GraphView
{
    [ServiceContract]
    internal interface IRawRecordService
    {
        [OperationContract]
        void SendRawRecord(RawRecordMessage record);
    }

    internal class RawRecordService : IRawRecordService
    {
        public void SendRawRecord(RawRecordMessage record)
        {
        }
    }

    [DataContract]
    internal class RawRecordMessage
    {
        [DataMember]
        private Dictionary<string, string> vertices;
        [DataMember]
        private Dictionary<string, string> forwardEdges;
        [DataMember]
        private Dictionary<string, string> backwardEdges;

        [DataMember]
        private RawRecord record;

        private Dictionary<string, VertexField> vertexFields;
        private Dictionary<string, EdgeField> forwardEdgeFields;
        private Dictionary<string, EdgeField> backwardEdgeFields;

        public RawRecordMessage(RawRecord record)
        {
            this.record = record;

            this.vertices = new Dictionary<string, string>();
            this.forwardEdges = new Dictionary<string, string>();
            this.backwardEdges = new Dictionary<string, string>();
            // analysis record
            for (int i = 0; i < record.Length; i++)
            {
                AnalysisFieldObject(record[i]);
            }
        }

        private void AnalysisFieldObject(FieldObject fieldObject)
        {
            StringField stringField = fieldObject as StringField;
            if (stringField != null)
            {
                return;
            }

            PathStepField pathStepField = fieldObject as PathStepField;
            if (pathStepField != null)
            {
                AnalysisFieldObject(pathStepField.StepFieldObject);
            }

            PathField pathField = fieldObject as PathField;
            if (pathField != null)
            {
                foreach (FieldObject pathStep in pathField.Path)
                {
                    AnalysisFieldObject(pathStep);
                }
            }

            CollectionField collectionField = fieldObject as CollectionField;
            if (collectionField != null)
            {
                foreach (FieldObject field in collectionField.Collection)
                {
                    AnalysisFieldObject(field);
                }
            }

            MapField mapField = fieldObject as MapField;
            if (mapField != null)
            {
                foreach (EntryField entry in mapField.ToList())
                {
                    AnalysisFieldObject(entry);
                }
            }

            EntryField entryField = fieldObject as EntryField;
            if (entryField != null)
            {
                AnalysisFieldObject(entryField.Key);
                AnalysisFieldObject(entryField.Value);
            }

            CompositeField compositeField = fieldObject as CompositeField;
            if (compositeField != null)
            {
                foreach (FieldObject field in compositeField.CompositeFieldObject.Values)
                {
                    AnalysisFieldObject(field);
                }
            }

            TreeField treeField = fieldObject as TreeField;
            if (treeField != null)
            {
                AnalysisFieldObject(treeField.NodeObject);
                foreach (TreeField field in treeField.Children.Values)
                {
                    AnalysisFieldObject(field);
                }
            }

            VertexSinglePropertyField vertexSinglePropertyField = fieldObject as VertexSinglePropertyField;
            if (vertexSinglePropertyField != null)
            {
                AddVertex(vertexSinglePropertyField.VertexProperty.Vertex);
            }

            EdgePropertyField edgePropertyField = fieldObject as EdgePropertyField;
            if (edgePropertyField != null)
            {
                AddEdge(edgePropertyField.Edge);
            }

            ValuePropertyField valuePropertyField = fieldObject as ValuePropertyField;
            if (valuePropertyField != null)
            {
                VertexField parentVertex = valuePropertyField.Parent as VertexField;
                if (parentVertex != null)
                {
                    AddVertex(parentVertex);
                }
                else
                {
                    VertexSinglePropertyField singleProperty = (VertexSinglePropertyField) fieldObject;
                    AddVertex(singleProperty.VertexProperty.Vertex);
                }
            }

            VertexPropertyField vertexPropertyField = fieldObject as VertexPropertyField;
            if (vertexPropertyField != null)
            {
                AddVertex(vertexPropertyField.Vertex);
            }

            EdgeField edgeField = fieldObject as EdgeField;
            if (edgeField != null)
            {
                AddEdge(edgeField);
            }

            VertexField vertexField = fieldObject as VertexField;
            if (vertexField != null)
            {
                AddVertex(vertexField);
            }

            throw new GraphViewException($"The type of the fieldObject is wrong. Now the type is: {fieldObject.GetType()}");
        }

        private void AddVertex(VertexField vertexField)
        {
            string vertexId = vertexField.VertexId;
            if (!this.vertices.ContainsKey(vertexId))
            {
                this.vertices[vertexId] = vertexField.VertexJObject.ToString();
            }
        }

        private void AddEdge(EdgeField edgeField)
        {
            string edgeId = edgeField.EdgeId;
            bool isForward = edgeField.GetEdgeDirection();

            if (isForward)
            {
                if (!this.forwardEdges.ContainsKey(edgeId))
                {
                    this.forwardEdges[edgeId] = edgeField.EdgeJObject.ToString();
                }
            }
            else
            {
                if (!this.backwardEdges.ContainsKey(edgeId))
                {
                    this.backwardEdges[edgeId] = edgeField.EdgeJObject.ToString();
                }
            }
        }

        public RawRecord DecodingMessage(GraphViewCommand command)
        {
            this.vertexFields = new Dictionary<string, VertexField>();
            this.forwardEdges = new Dictionary<string, string>();
            this.backwardEdges = new Dictionary<string, string>();

            foreach (KeyValuePair<string, string> pair in this.vertices)
            {
                this.vertexFields[pair.Key] = new VertexField(command, JObject.Parse(pair.Value));
            }

            foreach (KeyValuePair<string, string> pair in this.forwardEdges)
            {
                JObject edgeJObject = JObject.Parse(pair.Value);
                string outVId = (string)edgeJObject[KW_EDGE_SRCV];
                string outVLable = (string)edgeJObject[KW_EDGE_SRCV_LABEL];
                string outVPartition = (string)edgeJObject[KW_EDGE_SRCV_PARTITION];
                string edgeDocId = (string)edgeJObject[KW_DOC_ID];
                this.forwardEdgeFields[pair.Key] = EdgeField.ConstructForwardEdgeField(outVId,
                    outVLable, outVPartition, edgeDocId, edgeJObject);

                if (this.vertexFields.ContainsKey(outVId))
                {
                    this.vertexFields[outVId].AdjacencyList.TryAddEdgeField(pair.Key, 
                        () => this.forwardEdgeFields[pair.Key]);
                }
            }

            foreach (KeyValuePair<string, string> pair in this.backwardEdges)
            {
                JObject edgeJObject = JObject.Parse(pair.Value);
                string inVId = (string)edgeJObject[KW_EDGE_SINKV];
                string inVLable = (string)edgeJObject[KW_EDGE_SINKV_LABEL];
                string inVPartition = (string)edgeJObject[KW_EDGE_SINKV_PARTITION];
                string edgeDocId = (string)edgeJObject[KW_DOC_ID];
                this.backwardEdgeFields[pair.Key] = EdgeField.ConstructBackwardEdgeField(inVId,
                    inVLable, inVPartition, edgeDocId, edgeJObject);

                if (this.vertexFields.ContainsKey(inVId))
                {
                    this.vertexFields[inVId].RevAdjacencyList.TryAddEdgeField(pair.Key,
                        () => this.backwardEdgeFields[pair.Key]);
                }
            }

            RawRecord correctRecord = new RawRecord();
            for (int i = 0; i < this.record.Length; i++)
            {
                correctRecord.Append(RecoverFieldObject(this.record[i]));
            }
            return new RawRecord();
        }

        private FieldObject RecoverFieldObject(FieldObject fieldObject)
        {
            StringField stringField = fieldObject as StringField;
            if (stringField != null)
            {
                return fieldObject;
            }

            PathStepField pathStepField = fieldObject as PathStepField;
            if (pathStepField != null)
            {
                pathStepField.StepFieldObject = RecoverFieldObject(pathStepField.StepFieldObject);
                return pathStepField;
            }

            PathField pathField = fieldObject as PathField;
            if (pathField != null)
            {
                for (int i = 0; i < pathField.Path.Count; i++)
                {
                    pathField.Path[i] = RecoverFieldObject(pathField.Path[i]);
                }
                return pathField;
            }

            CollectionField collectionField = fieldObject as CollectionField;
            if (collectionField != null)
            {
                for (int i = 0; i < collectionField.Collection.Count; i++)
                {
                    collectionField.Collection[i] = RecoverFieldObject(collectionField.Collection[i]);
                }
                return collectionField;
            }

            MapField mapField = fieldObject as MapField;
            if (mapField != null)
            {
                MapField newMapField = new MapField(mapField.Count);;
                foreach (FieldObject key in mapField.Order)
                {
                    newMapField.Add(RecoverFieldObject(key), RecoverFieldObject(mapField[key]));
                }
                return newMapField;
            }

            EntryField entryField = fieldObject as EntryField;
            if (entryField != null)
            {
                throw new GraphViewException("Type of fieldObject should not be EntryField");
            }

            CompositeField compositeField = fieldObject as CompositeField;
            if (compositeField != null)
            {
                foreach (KeyValuePair<string, FieldObject> pair in compositeField.CompositeFieldObject)
                {
                    compositeField[pair.Key] = RecoverFieldObject(pair.Value);
                }
            }

            TreeField treeField = fieldObject as TreeField;
            if (treeField != null)
            {
                TreeField newTreeField = new TreeField(RecoverFieldObject(treeField.NodeObject));
                foreach (TreeField child in treeField.Children.Values)
                {
                    TreeField newChild = (TreeField)RecoverFieldObject(child);
                    newTreeField.Children.Add(newChild.NodeObject, newChild);
                }
                return newTreeField;
            }

            VertexSinglePropertyField vertexSinglePropertyField = fieldObject as VertexSinglePropertyField;
            if (vertexSinglePropertyField != null)
            {
                string vertexId = vertexSinglePropertyField.SearchInfo.Item1;
                string propertyName = vertexSinglePropertyField.SearchInfo.Item2;
                string propertyId = vertexSinglePropertyField.SearchInfo.Item3;
                return this.vertexFields[vertexId].VertexProperties[propertyName].Multiples[propertyId];
            }

            EdgePropertyField edgePropertyField = fieldObject as EdgePropertyField;
            if (edgePropertyField != null)
            {
                string edgeId = edgePropertyField.SearchInfo.Item1;
                bool isReverseEdge = edgePropertyField.SearchInfo.Item2;
                string propertyName = edgePropertyField.SearchInfo.Item3;

                if (isReverseEdge)
                {
                    return this.backwardEdgeFields[edgeId].EdgeProperties[propertyName];
                }
                else
                {
                    return this.forwardEdgeFields[edgeId].EdgeProperties[propertyName];
                }
            }

            ValuePropertyField valuePropertyField = fieldObject as ValuePropertyField;
            if (valuePropertyField != null)
            {
                string vertexId = valuePropertyField.SearchInfo.Item1;
                if (valuePropertyField.SearchInfo.Item2 != null)
                {
                    string singlePropertyName = valuePropertyField.SearchInfo.Item2;
                    string singlePropertyId = valuePropertyField.SearchInfo.Item3;
                    string propertyName = valuePropertyField.SearchInfo.Item4;
                    return this.vertexFields[vertexId].VertexProperties[singlePropertyName].Multiples[singlePropertyId]
                        .MetaProperties[propertyName];
                }
                else
                {
                    string propertyName = valuePropertyField.SearchInfo.Item4;
                    return this.vertexFields[vertexId].VertexMetaProperties[propertyName];
                }
            }

            VertexPropertyField vertexPropertyField = fieldObject as VertexPropertyField;
            if (vertexPropertyField != null)
            {
                string vertexId = vertexPropertyField.SearchInfo.Item1;
                string propertyName = vertexPropertyField.SearchInfo.Item2;
                return this.vertexFields[vertexId].VertexProperties[propertyName];
            }

            EdgeField edgeField = fieldObject as EdgeField;
            if (edgeField != null)
            {
                string edgeId = edgeField.SearchInfo.Item1;
                bool isReverseEdge = edgeField.SearchInfo.Item2;
                if (isReverseEdge)
                {
                    return this.backwardEdgeFields[edgeId];
                }
                else
                {
                    return this.forwardEdgeFields[edgeId];
                }
            }

            VertexField vertexField = fieldObject as VertexField;
            if (vertexField != null)
            {
                string vertexId = vertexField.SearchInfo;
                return this.vertexFields[vertexId];
            }

            throw new GraphViewException($"The type of the fieldObject is wrong. Now the type is: {fieldObject.GetType()}");
        }
    }

}
