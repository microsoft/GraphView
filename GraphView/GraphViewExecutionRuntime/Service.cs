using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.ServiceModel.Description;
using System.Threading;
using Newtonsoft.Json.Linq;
using static GraphView.DocumentDBKeywords;

namespace GraphView
{
    [ServiceContract]
    internal interface IRawRecordService
    {
        [OperationContract]
        void SendRawRecord(RawRecordMessage record);

        [OperationContract]
        void SendSignal(int index);
    }

    internal class RawRecordService : IRawRecordService
    {
        // use concurrentQueue temporarily
        public ConcurrentQueue<RawRecordMessage> Messages { get; } = new ConcurrentQueue<RawRecordMessage>();
        public ConcurrentQueue<int> Signals { get; } = new ConcurrentQueue<int>();

        public void SendRawRecord(RawRecordMessage record)
        {
            Messages.Enqueue(record);
        }

        public void SendSignal(int index)
        {
            Signals.Enqueue(index);
        }
    }

    internal class SendOperatorOfTraversalOp : GraphViewExecutionOperator
    {
        private GraphViewExecutionOperator inputOp;

        private int edgeFieldIndex;
        TraversalOperator.TraversalTypeEnum traversalType;

        private string receiveOpId;

        // Set following fields in deserialization
        private List<PartitionPlan> partitionPlans;
        private int partitionPlanIndex;
        private GraphViewCommand command;

        public SendOperatorOfTraversalOp(GraphViewExecutionOperator inputOp, int edgeFieldIndex, 
            TraversalOperator.TraversalTypeEnum traversalType)
        {
            this.inputOp = inputOp;
            this.edgeFieldIndex = edgeFieldIndex;
            this.traversalType = traversalType;
            this.Open();
        }

        public override RawRecord Next()
        {
            RawRecord record;
            while (this.inputOp.State() && (record = this.inputOp.Next()) != null)
            {
                EdgeField edge = record[this.edgeFieldIndex] as EdgeField;
                switch (this.traversalType)
                {
                    case TraversalOperator.TraversalTypeEnum.Source:
                        if (ReturnOrSend(record, edge.OutVPartition))
                        {
                            return record;
                        }
                        break;
                    case TraversalOperator.TraversalTypeEnum.Sink:
                        if (ReturnOrSend(record, edge.InVPartition))
                        {
                            return record;
                        }
                        break;
                    case TraversalOperator.TraversalTypeEnum.Other:
                        if (ReturnOrSend(record, edge.OtherVPartition))
                        {
                            return record;
                        }
                        break;
                    case TraversalOperator.TraversalTypeEnum.Both:
                        if (ReturnOrSend(record, edge.OutVPartition))
                        {
                            return record;
                        }
                        break;
                    default:
                        throw new GraphViewException("Type of TraversalTypeEnum wrong.");
                }
            }

            for (int i = 0; i < this.partitionPlans.Count; i++)
            {
                if (i == this.partitionPlanIndex)
                {
                    continue;
                }
                // todo: construct client and send signal
            }

            this.Close();
            return null;
        }

        private bool ReturnOrSend(RawRecord record, string partition)
        {
            if (this.partitionPlans[this.partitionPlanIndex].BelongToPartitionPlan(partition))
            {
                return true;
            }
            for (int i = 0; i < this.partitionPlans.Count; i++)
            {
                if (i == this.partitionPlanIndex)
                {
                    continue;
                }
                if (this.partitionPlans[this.partitionPlanIndex].BelongToPartitionPlan(partition))
                {
                    SendRawRecord(record, i);
                    break;
                }
                Debug.Assert(i != this.partitionPlans.Count - 1);
            }
            return false;
        }

        private void SendRawRecord(RawRecord record, int NodeIndex)
        {
            RawRecordMessage message = new RawRecordMessage(record, this.command);

            string ip = this.partitionPlans[this.partitionPlanIndex].IP;
            int port = this.partitionPlans[this.partitionPlanIndex].Port;
            UriBuilder uri = new UriBuilder("http", ip, port, this.receiveOpId);
            Uri baseAddress = uri.Uri;

            // todo: construct client and send message
        }

        public void SetReceiveOpId(string id)
        {
            this.receiveOpId = id;
        }

        public override GraphViewExecutionOperator GetFirstOperator()
        {
            return this.inputOp.GetFirstOperator();
        }

        public override void ResetState()
        {
            this.Open();
            this.inputOp.ResetState();
        }
    }

    internal class ReceiveOperatorOfTraversalOp : GraphViewExecutionOperator
    {
        private SendOperatorOfTraversalOp inputOp;

        private ServiceHost selfHost;
        private RawRecordService service;
        private List<bool> hasBeenSignaled;

        // Set following fields in deserialization
        private List<PartitionPlan> partitionPlans;
        private int partitionPlanIndex;
        private GraphViewCommand command;

        public string Id { get;}

        public ReceiveOperatorOfTraversalOp(SendOperatorOfTraversalOp inputOp)
        {
            this.inputOp = inputOp;

            this.Id = Guid.NewGuid().ToString("N");
            this.inputOp.SetReceiveOpId(this.Id);

            this.selfHost = null;
            this.hasBeenSignaled = Enumerable.Repeat(false, this.partitionPlans.Count).ToList();
            this.hasBeenSignaled[this.partitionPlanIndex] = true;

            this.Open();
        }

        public override RawRecord Next()
        {
            if (this.selfHost == null)
            {
                PartitionPlan ownPartitionPlan = this.partitionPlans[this.partitionPlanIndex];
                UriBuilder uri = new UriBuilder("http", ownPartitionPlan.IP, ownPartitionPlan.Port, this.Id);
                Uri baseAddress = uri.Uri;

                this.selfHost = new ServiceHost(new RawRecordService(), baseAddress);

                selfHost.AddServiceEndpoint(typeof(IRawRecordService), new WSHttpBinding(), "RawRecordService");

                ServiceMetadataBehavior smb = new ServiceMetadataBehavior();
                smb.HttpGetEnabled = true;
                selfHost.Description.Behaviors.Add(smb);
                selfHost.Description.Behaviors.Find<ServiceBehaviorAttribute>().InstanceContextMode =
                    InstanceContextMode.Single;

                this.service = selfHost.SingletonInstance as RawRecordService;

                selfHost.Open();
            }

            RawRecord record;
            if (this.inputOp.State() && (record = this.inputOp.Next()) != null)
            {
                return record;
            }

            // todo : use loop temporarily. need change later.
            while (true)
            {
                RawRecordMessage message;
                if (this.service.Messages.TryDequeue(out message))
                {
                    return message.DecodingMessage(this.command);
                }

                int signalIndex;
                while (this.service.Signals.TryDequeue(out signalIndex))
                {
                    this.hasBeenSignaled[signalIndex] = true;
                }

                bool canClose = true;
                foreach (bool flag in this.hasBeenSignaled)
                {
                    if (flag == false)
                    {
                        canClose = false;
                        break;
                    }
                }
                if (canClose)
                {
                    break;
                }
            }

            this.selfHost.Close();
            this.Close();
            return null;
        }

        public override GraphViewExecutionOperator GetFirstOperator()
        {
            return this.inputOp.GetFirstOperator();
        }

        public override void ResetState()
        {
            this.Open();
            Debug.Assert(this.selfHost == null || this.selfHost.State == CommunicationState.Closed);
            this.selfHost = null;
            this.inputOp.ResetState();
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

        public RawRecordMessage(RawRecord record, GraphViewCommand command)
        {
            this.record = record;

            this.vertices = new Dictionary<string, string>();
            this.forwardEdges = new Dictionary<string, string>();
            this.backwardEdges = new Dictionary<string, string>();
            // analysis record
            for (int i = 0; i < record.Length; i++)
            {
                AnalysisFieldObject(record[i], command);
            }
        }

        private void AnalysisFieldObject(FieldObject fieldObject, GraphViewCommand command)
        {
            StringField stringField = fieldObject as StringField;
            if (stringField != null)
            {
                return;
            }

            PathStepField pathStepField = fieldObject as PathStepField;
            if (pathStepField != null)
            {
                AnalysisFieldObject(pathStepField.StepFieldObject, command);
            }

            PathField pathField = fieldObject as PathField;
            if (pathField != null)
            {
                foreach (FieldObject pathStep in pathField.Path)
                {
                    AnalysisFieldObject(pathStep, command);
                }
            }

            CollectionField collectionField = fieldObject as CollectionField;
            if (collectionField != null)
            {
                foreach (FieldObject field in collectionField.Collection)
                {
                    AnalysisFieldObject(field, command);
                }
            }

            MapField mapField = fieldObject as MapField;
            if (mapField != null)
            {
                foreach (EntryField entry in mapField.ToList())
                {
                    AnalysisFieldObject(entry, command);
                }
            }

            EntryField entryField = fieldObject as EntryField;
            if (entryField != null)
            {
                AnalysisFieldObject(entryField.Key, command);
                AnalysisFieldObject(entryField.Value, command);
            }

            CompositeField compositeField = fieldObject as CompositeField;
            if (compositeField != null)
            {
                foreach (FieldObject field in compositeField.CompositeFieldObject.Values)
                {
                    AnalysisFieldObject(field, command);
                }
            }

            TreeField treeField = fieldObject as TreeField;
            if (treeField != null)
            {
                AnalysisFieldObject(treeField.NodeObject, command);
                foreach (TreeField field in treeField.Children.Values)
                {
                    AnalysisFieldObject(field, command);
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
                AddEdge(edgePropertyField.Edge, command);
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
                AddEdge(edgeField, command);
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

        private void AddEdge(EdgeField edgeField, GraphViewCommand command)
        {
            string edgeId = edgeField.EdgeId;
            bool isReverse = edgeField.IsReverse;

            if (isReverse)
            {
                string vertexId = edgeField.InV;
                VertexField vertex;
                if (command.VertexCache.TryGetVertexField(vertexId, out vertex))
                {
                    bool isSpilled = EdgeDocumentHelper.IsSpilledVertex(vertex.VertexJObject, isReverse);
                    if (isSpilled)
                    {
                        if (!this.forwardEdges.ContainsKey(edgeId))
                        {
                            this.forwardEdges[edgeId] = edgeField.EdgeJObject.ToString();
                        }
                    }
                    else
                    {
                        if (!this.vertices.ContainsKey(vertexId))
                        {
                            this.vertices[vertexId] = vertex.VertexJObject.ToString();
                        }
                    }
                }
                else
                {
                    throw new GraphViewException($"VertexCache should have this vertex. VertexId:{vertexId}");
                }
            }
            else
            {
                string vertexId = edgeField.OutV;
                VertexField vertex;
                if (command.VertexCache.TryGetVertexField(vertexId, out vertex))
                {
                    bool isSpilled = EdgeDocumentHelper.IsSpilledVertex(vertex.VertexJObject, isReverse);
                    if (isSpilled)
                    {
                        if (!this.backwardEdges.ContainsKey(edgeId))
                        {
                            this.backwardEdges[edgeId] = edgeField.EdgeJObject.ToString();
                        }
                    }
                    else
                    {
                        if (!this.vertices.ContainsKey(vertexId))
                        {
                            this.vertices[vertexId] = vertex.VertexJObject.ToString();
                        }
                    }
                }
                else
                {
                    throw new GraphViewException($"VertexCache should have this vertex. VertexId:{vertexId}");
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

                this.vertexFields[outVId].AdjacencyList.TryAddEdgeField(pair.Key,
                    () => EdgeField.ConstructForwardEdgeField(outVId, outVLable, outVPartition, edgeDocId, edgeJObject));
            }

            foreach (KeyValuePair<string, string> pair in this.backwardEdges)
            {
                JObject edgeJObject = JObject.Parse(pair.Value);
                string inVId = (string)edgeJObject[KW_EDGE_SINKV];
                string inVLable = (string)edgeJObject[KW_EDGE_SINKV_LABEL];
                string inVPartition = (string)edgeJObject[KW_EDGE_SINKV_PARTITION];
                string edgeDocId = (string)edgeJObject[KW_DOC_ID];

                this.vertexFields[inVId].RevAdjacencyList.TryAddEdgeField(pair.Key,
                    () => EdgeField.ConstructBackwardEdgeField(inVId, inVLable, inVPartition, edgeDocId, edgeJObject));
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
                string vertexId = edgePropertyField.SearchInfo.Item1;
                string edgeId = edgePropertyField.SearchInfo.Item2;
                bool isReverseEdge = edgePropertyField.SearchInfo.Item3;
                string propertyName = edgePropertyField.SearchInfo.Item4;

                if (isReverseEdge)
                {
                    return this.vertexFields[vertexId].RevAdjacencyList.GetEdgeField(edgeId, false).EdgeProperties[propertyName];
                }
                else
                {
                    return this.vertexFields[vertexId].AdjacencyList.GetEdgeField(edgeId, false).EdgeProperties[propertyName];
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
                string vertexId = edgePropertyField.SearchInfo.Item1;
                string edgeId = edgeField.SearchInfo.Item2;
                bool isReverseEdge = edgeField.SearchInfo.Item3;
                if (isReverseEdge)
                {
                    return this.vertexFields[vertexId].RevAdjacencyList.GetEdgeField(edgeId, false);
                }
                else
                {
                    return this.vertexFields[vertexId].AdjacencyList.GetEdgeField(edgeId, false);
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

    internal class BoundedBuffer<T>
    {
        private int bufferSize;
        private Queue<T> boundedBuffer;

        // Whether the queue expects more elements to come
        public bool More { get; private set; }

        private readonly Object monitor;

        public BoundedBuffer(int bufferSize)
        {
            this.boundedBuffer = new Queue<T>(bufferSize);
            this.bufferSize = bufferSize;
            this.More = true;
            this.monitor = new object();
        }

        public void Add(T element)
        {
            lock (this.monitor)
            {
                while (boundedBuffer.Count == this.bufferSize)
                {
                    Monitor.Wait(this.monitor);
                }

                this.boundedBuffer.Enqueue(element);
                Monitor.Pulse(this.monitor);
            }
        }

        public T Retrieve()
        {
            T element = default(T);

            lock (this.monitor)
            {
                while (this.boundedBuffer.Count == 0 && this.More)
                {
                    Monitor.Wait(this.monitor);
                }

                if (this.boundedBuffer.Count > 0)
                {
                    element = this.boundedBuffer.Dequeue();
                    Monitor.Pulse(this.monitor);
                }
            }

            return element;
        }

        public void Close()
        {
            lock (this.monitor)
            {
                this.More = false;
                Monitor.PulseAll(this.monitor);
            }
        }
    }

}
