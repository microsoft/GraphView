using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
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
        void SendRawRecord(string message);

        [OperationContract]
        void SendSignal(int index);
    }

    internal class RawRecordService : IRawRecordService
    {
        // use concurrentQueue temporarily
        public ConcurrentQueue<string> Messages { get; } = new ConcurrentQueue<string>();
        public ConcurrentQueue<int> Signals { get; } = new ConcurrentQueue<int>();

        public void SendRawRecord(string message)
        {
            Messages.Enqueue(message);
        }

        public void SendSignal(int index)
        {
            Signals.Enqueue(index);
        }
    }

    [Serializable]
    internal abstract class GetPartitionMethod
    {
        public abstract string GetPartition(RawRecord record);
    }

    [Serializable]
    internal class GetPartitionMethodForTraversalOp : GetPartitionMethod
    {
        private int edgeFieldIndex;
        TraversalOperator.TraversalTypeEnum traversalType;

        public GetPartitionMethodForTraversalOp(int edgeFieldIndex, TraversalOperator.TraversalTypeEnum traversalType)
        {
            this.edgeFieldIndex = edgeFieldIndex;
            this.traversalType = traversalType;
        }

        public override string GetPartition(RawRecord record)
        {
            EdgeField edge = record[this.edgeFieldIndex] as EdgeField;
            switch (this.traversalType)
            {
                case TraversalOperator.TraversalTypeEnum.Source:
                    return edge.OutVPartition;
                case TraversalOperator.TraversalTypeEnum.Sink:
                    return edge.InVPartition;
                case TraversalOperator.TraversalTypeEnum.Other:
                    return edge.OtherVPartition;
                case TraversalOperator.TraversalTypeEnum.Both:
                    return edge.OutVPartition;
                default:
                    throw new GraphViewException("Type of TraversalTypeEnum wrong.");
            }
        }
    }

    [Serializable]
    internal class SendOperator : GraphViewExecutionOperator
    {
        private GraphViewExecutionOperator inputOp;

        private GetPartitionMethod getPartitionMethod;

        private string receiveOpId;

        private readonly int retryLimit;
        // if send data failed, sendOp will wait retryInterval milliseconds.
        private readonly int retryInterval;

        // Set following fields in deserialization
        [NonSerialized]
        private List<PartitionPlan> partitionPlans;
        [NonSerialized]
        private int partitionPlanIndex;
        [NonSerialized]
        private GraphViewCommand command;

        public SendOperator(GraphViewExecutionOperator inputOp, GetPartitionMethod getPartitionMethod)
        {
            this.inputOp = inputOp;
            this.getPartitionMethod = getPartitionMethod;
            this.retryLimit = 20;
            this.retryInterval = 5; // 5 ms
            this.Open();
        }

        public override RawRecord Next()
        {   
            RawRecord record;
            while (this.inputOp.State() && (record = this.inputOp.Next()) != null)
            {
                if (ReturnOrSend(record, this.getPartitionMethod.GetPartition(record)))
                {
                    return record;
                }
            }

            for (int i = 0; i < this.partitionPlans.Count; i++)
            {
                if (i == this.partitionPlanIndex)
                {
                    continue;
                }
                SendSignal(i);
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
                if (this.partitionPlans[i].BelongToPartitionPlan(partition))
                {
                    SendRawRecord(record, i);
                    return false;
                }
            }
            throw new GraphViewException($"This partition does not belong to any partition plan! partition:{ partition }");
        }

        private void SendRawRecord(RawRecord record, int nodeIndex)
        {
            string message = RawRecordMessage.CodeMessage(record);
            RawRecordServiceClient client = ConstructClient(nodeIndex);

            for (int i = 0; i < this.retryLimit; i++)
            {
                try
                {
                    client.SendRawRecord(message);
                    return;
                }
                catch (Exception e)
                {
                    if (i == this.retryLimit - 1)
                    {
                        throw e;
                    }
                    System.Threading.Thread.Sleep(this.retryInterval);
                }
            }
        }

        private void SendSignal(int nodeIndex)
        {
            RawRecordServiceClient client = ConstructClient(nodeIndex);

            for (int i = 0; i < this.retryLimit; i++)
            {
                try
                {
                    client.SendSignal(this.partitionPlanIndex);
                    return;
                }
                catch (Exception e)
                {
                    if (i == this.retryLimit - 1)
                    {
                        throw e;
                    }
                    System.Threading.Thread.Sleep(this.retryInterval);
                }
            }
        }

        private RawRecordServiceClient ConstructClient(int nodeIndex)
        {
            string ip = this.partitionPlans[nodeIndex].IP;
            int port = this.partitionPlans[nodeIndex].Port;
            UriBuilder uri = new UriBuilder("http", ip, port, this.receiveOpId + "/RawRecordService");
            EndpointAddress endpointAddress = new EndpointAddress(uri.ToString());
            WSHttpBinding binding = new WSHttpBinding();
            binding.Security.Mode = SecurityMode.None;
            return new RawRecordServiceClient(binding, endpointAddress);
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

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            AdditionalSerializationInfo additionalInfo = (AdditionalSerializationInfo)context.Context;
            this.command = additionalInfo.Command;
            this.partitionPlans = additionalInfo.PartitionPlans;
            this.partitionPlanIndex = additionalInfo.PartitionPlanIndex;
        }

    }

    [Serializable]
    internal class ReceiveOperator : GraphViewExecutionOperator
    {
        private SendOperator inputOp;

        [NonSerialized]
        private ServiceHost selfHost;
        [NonSerialized]
        private RawRecordService service;
        [NonSerialized]
        private List<bool> hasBeenSignaled;

        // Set following fields in deserialization
        [NonSerialized]
        private List<PartitionPlan> partitionPlans;
        [NonSerialized]
        private int partitionPlanIndex;
        [NonSerialized]
        private GraphViewCommand command;

        private readonly string id;

        public ReceiveOperator(SendOperator inputOp)
        {
            this.inputOp = inputOp;

            this.id = Guid.NewGuid().ToString("N");
            this.inputOp.SetReceiveOpId(this.id);

            this.selfHost = null;

            this.Open();
        }

        public override RawRecord Next()
        {
            if (this.selfHost == null)
            {
                PartitionPlan ownPartitionPlan = this.partitionPlans[this.partitionPlanIndex];
                UriBuilder uri = new UriBuilder("http", ownPartitionPlan.IP, ownPartitionPlan.Port, this.id);
                Uri baseAddress = uri.Uri;

                WSHttpBinding binding = new WSHttpBinding();
                binding.Security.Mode = SecurityMode.None;

                this.selfHost = new ServiceHost(new RawRecordService(), baseAddress);
                this.selfHost.AddServiceEndpoint(typeof(IRawRecordService), binding, "RawRecordService");

                ServiceMetadataBehavior smb = new ServiceMetadataBehavior();
                smb.HttpGetEnabled = true;
                this.selfHost.Description.Behaviors.Add(smb);
                this.selfHost.Description.Behaviors.Find<ServiceBehaviorAttribute>().InstanceContextMode =
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
                string message;
                if (this.service.Messages.TryDequeue(out message))
                {
                    return RawRecordMessage.DecodeMessage(message, this.command);
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
            this.selfHost = null;
            this.inputOp.ResetState();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            AdditionalSerializationInfo additionalInfo = (AdditionalSerializationInfo)context.Context;
            this.command = additionalInfo.Command;
            this.partitionPlans = additionalInfo.PartitionPlans;
            this.partitionPlanIndex = additionalInfo.PartitionPlanIndex;

            this.selfHost = null;
            this.hasBeenSignaled = Enumerable.Repeat(false, this.partitionPlans.Count).ToList();
            this.hasBeenSignaled[this.partitionPlanIndex] = true;
        }
    }

    // for Debug
    //public class StartHostForDevelopment
    //{
    //    public static void Main(string[] args)
    //    {
    //        Uri baseAddress = new Uri("http://localhost:8000/Host/"); ;

    //        ServiceHost selfHost = new ServiceHost(new RawRecordService(), baseAddress);

    //        selfHost.AddServiceEndpoint(typeof(IRawRecordService), new WSHttpBinding(), typeof(RawRecordService).ToString());

    //        ServiceMetadataBehavior smb = new ServiceMetadataBehavior();
    //        smb.HttpGetEnabled = true;
    //        selfHost.Description.Behaviors.Add(smb);
    //        selfHost.Description.Behaviors.Find<ServiceBehaviorAttribute>().InstanceContextMode =
    //            InstanceContextMode.Single;

    //        selfHost.Open();
    //        foreach (var endpoint in selfHost.Description.Endpoints)
    //        {
    //            Console.WriteLine(endpoint.Address.ToString());
    //        }
    //        Console.ReadLine();
    //        selfHost.Close();
    //    }
    //}

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

        public static string CodeMessage(RawRecord record)
        {
            using (MemoryStream memStream = new MemoryStream())
            {
                DataContractSerializer ser = new DataContractSerializer(typeof(RawRecordMessage));
                ser.WriteObject(memStream, new RawRecordMessage(record));

                memStream.Position = 0;
                StreamReader stringReader = new StreamReader(memStream);
                return stringReader.ReadToEnd();
            }
        }

        public static RawRecord DecodeMessage(string message, GraphViewCommand command)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                StreamWriter writer = new StreamWriter(stream);
                writer.Write(message);
                writer.Flush();
                stream.Position = 0;

                DataContractSerializer deser = new DataContractSerializer(typeof(RawRecordMessage));
                RawRecordMessage rawRecordMessage = (RawRecordMessage)deser.ReadObject(stream);
                return rawRecordMessage.ReconstructRawRecord(command);
            }
        }

        private RawRecordMessage(RawRecord record)
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
            if (fieldObject == null)
            {
                return;
            }

            StringField stringField = fieldObject as StringField;
            if (stringField != null)
            {
                return;
            }

            PathField pathField = fieldObject as PathField;
            if (pathField != null)
            {
                foreach (FieldObject pathStep in pathField.Path)
                {
                    if (pathStep == null)
                    {
                        continue;
                    }

                    AnalysisFieldObject(((PathStepField)pathStep).StepFieldObject);
                }
                return;
            }

            CollectionField collectionField = fieldObject as CollectionField;
            if (collectionField != null)
            {
                foreach (FieldObject field in collectionField.Collection)
                {
                    AnalysisFieldObject(field);
                }
                return;
            }

            MapField mapField = fieldObject as MapField;
            if (mapField != null)
            {
                foreach (EntryField entry in mapField.ToList())
                {
                    AnalysisFieldObject(entry.Key);
                    AnalysisFieldObject(entry.Value);
                }
                return;
            }

            CompositeField compositeField = fieldObject as CompositeField;
            if (compositeField != null)
            {
                foreach (FieldObject field in compositeField.CompositeFieldObject.Values)
                {
                    AnalysisFieldObject(field);
                }
                return;
            }

            TreeField treeField = fieldObject as TreeField;
            if (treeField != null)
            {
                //AnalysisFieldObject(treeField.NodeObject);
                //foreach (TreeField field in treeField.Children.Values)
                //{
                //    AnalysisFieldObject(field);
                //}

                Queue<TreeField> queue = new Queue<TreeField>();
                queue.Enqueue(treeField);

                while (queue.Count > 0)
                {
                    TreeField treeNode = queue.Dequeue();
                    AnalysisFieldObject(treeNode);
                    foreach (TreeField childNode in treeNode.Children.Values)
                    {
                        queue.Enqueue(childNode);
                    }
                }

                return;
            }

            VertexSinglePropertyField vertexSinglePropertyField = fieldObject as VertexSinglePropertyField;
            if (vertexSinglePropertyField != null)
            {
                AddVertex(vertexSinglePropertyField.VertexProperty.Vertex);
                return;
            }

            EdgePropertyField edgePropertyField = fieldObject as EdgePropertyField;
            if (edgePropertyField != null)
            {
                AddEdge(edgePropertyField.Edge);
                return;
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
                return;
            }

            VertexPropertyField vertexPropertyField = fieldObject as VertexPropertyField;
            if (vertexPropertyField != null)
            {
                AddVertex(vertexPropertyField.Vertex);
                return;
            }

            EdgeField edgeField = fieldObject as EdgeField;
            if (edgeField != null)
            {
                AddEdge(edgeField);
                return;
            }

            VertexField vertexField = fieldObject as VertexField;
            if (vertexField != null)
            {
                AddVertex(vertexField);
                return;
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
            bool isReverse = edgeField.IsReverse;

            if (!isReverse)
            {
                if (!this.forwardEdges.ContainsKey(edgeId))
                {
                    this.forwardEdges[edgeId] = edgeField.EdgeJObject.ToString();
                }
                //string vertexId = edgeField.InV;
                //VertexField vertex;
                //if (command.VertexCache.TryGetVertexField(vertexId, out vertex))
                //{
                //    bool isSpilled = EdgeDocumentHelper.IsSpilledVertex(vertex.VertexJObject, isReverse);
                //    if (isSpilled)
                //    {
                //        if (!this.forwardEdges.ContainsKey(edgeId))
                //        {
                //            this.forwardEdges[edgeId] = edgeField.EdgeJObject.ToString();
                //        }
                //    }
                //    else
                //    {
                //        if (!this.vertices.ContainsKey(vertexId))
                //        {
                //            this.vertices[vertexId] = vertex.VertexJObject.ToString();
                //        }
                //    }
                //}
                //else
                //{
                //    throw new GraphViewException($"VertexCache should have this vertex. VertexId:{vertexId}");
                //}
            }
            else
            {
                if (!this.backwardEdges.ContainsKey(edgeId))
                {
                    this.backwardEdges[edgeId] = edgeField.EdgeJObject.ToString();
                }
                //string vertexId = edgeField.OutV;
                //VertexField vertex;
                //if (command.VertexCache.TryGetVertexField(vertexId, out vertex))
                //{
                //    bool isSpilled = EdgeDocumentHelper.IsSpilledVertex(vertex.VertexJObject, isReverse);
                //    if (isSpilled)
                //    {
                //        if (!this.backwardEdges.ContainsKey(edgeId))
                //        {
                //            this.backwardEdges[edgeId] = edgeField.EdgeJObject.ToString();
                //        }
                //    }
                //    else
                //    {
                //        if (!this.vertices.ContainsKey(vertexId))
                //        {
                //            this.vertices[vertexId] = vertex.VertexJObject.ToString();
                //        }
                //    }
                //}
                //else
                //{
                //    throw new GraphViewException($"VertexCache should have this vertex. VertexId:{vertexId}");
                //}
            }
        }

        public RawRecord ReconstructRawRecord(GraphViewCommand command)
        {
            this.vertexFields = new Dictionary<string, VertexField>();
            this.forwardEdgeFields = new Dictionary<string, EdgeField>();
            this.backwardEdgeFields = new Dictionary<string, EdgeField>();

            foreach (KeyValuePair<string, string> pair in this.vertices)
            {
                VertexField vertex;
                if (command.VertexCache.TryGetVertexField(pair.Key, out vertex))
                {
                    this.vertexFields[pair.Key] = vertex;
                }
                else
                {
                    this.vertexFields[pair.Key] = new VertexField(command, JObject.Parse(pair.Value));
                }
            }

            foreach (KeyValuePair<string, string> pair in this.forwardEdges)
            {
                JObject edgeJObject = JObject.Parse(pair.Value);
                string outVId = (string)edgeJObject[KW_EDGE_SRCV];
                string outVLable = (string)edgeJObject[KW_EDGE_SRCV_LABEL];
                string outVPartition = (string)edgeJObject[KW_EDGE_SRCV_PARTITION];
                string edgeDocId = (string)edgeJObject[KW_DOC_ID];

                //this.vertexFields[outVId].AdjacencyList.TryAddEdgeField(pair.Key,
                //    () => EdgeField.ConstructForwardEdgeField(outVId, outVLable, outVPartition, edgeDocId, edgeJObject));
                this.forwardEdgeFields[pair.Key] =
                    EdgeField.ConstructForwardEdgeField(outVId, outVLable, outVPartition, edgeDocId, edgeJObject);
            }

            foreach (KeyValuePair<string, string> pair in this.backwardEdges)
            {
                JObject edgeJObject = JObject.Parse(pair.Value);
                string inVId = (string)edgeJObject[KW_EDGE_SINKV];
                string inVLable = (string)edgeJObject[KW_EDGE_SINKV_LABEL];
                string inVPartition = (string)edgeJObject[KW_EDGE_SINKV_PARTITION];
                string edgeDocId = (string)edgeJObject[KW_DOC_ID];

                //this.vertexFields[inVId].RevAdjacencyList.TryAddEdgeField(pair.Key,
                //    () => EdgeField.ConstructBackwardEdgeField(inVId, inVLable, inVPartition, edgeDocId, edgeJObject));
                this.backwardEdgeFields[pair.Key] = EdgeField.ConstructBackwardEdgeField(inVId, inVLable, inVPartition, edgeDocId, edgeJObject);
            }

            RawRecord correctRecord = new RawRecord();
            for (int i = 0; i < this.record.Length; i++)
            {
                correctRecord.Append(RecoverFieldObject(this.record[i]));
            }
            return correctRecord;
        }

        private FieldObject RecoverFieldObject(FieldObject fieldObject)
        {
            if (fieldObject == null)
            {
                return null;
            }

            StringField stringField = fieldObject as StringField;
            if (stringField != null)
            {
                return stringField;
            }

            PathField pathField = fieldObject as PathField;
            if (pathField != null)
            {
                foreach (FieldObject pathStep in pathField.Path)
                {
                    if (pathStep == null)
                    {
                        continue;
                    }
                    PathStepField pathStepField = pathStep as PathStepField;
                    pathStepField.StepFieldObject = RecoverFieldObject(pathStepField.StepFieldObject);
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

    // todo: check it later
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
