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
    internal interface IMessageService
    {
        [OperationContract]
        void SendMessage(string message);

        [OperationContract]
        void SendMessageWithSource(string message, int from);

        [OperationContract]
        void SendSignal(string message);

        [OperationContract]
        void SendSignalWithSource(string message, int from);
    }

    internal class MessageService : IMessageService
    {
        // use concurrentQueue temporarily
        public ConcurrentQueue<string> Messages { get; } = new ConcurrentQueue<string>();
        public ConcurrentQueue<string> Signals { get; } = new ConcurrentQueue<string>();
        public ConcurrentQueue<int> Sources { get; } = new ConcurrentQueue<int>();

        public void SendMessage(string message)
        {
            Messages.Enqueue(message);
        }

        public void SendMessageWithSource(string message, int from)
        {
            Messages.Enqueue(message);
            Sources.Enqueue(from);
        }

        public void SendSignal(string message)
        {
            Signals.Enqueue(message);
        }

        public void SendSignalWithSource(string message, int from)
        {
            Signals.Enqueue(message);
            Sources.Enqueue(from);
        }
    }

    [Serializable]
    internal class SendClient
    {
        private string receiveHostId;
        // if send data failed, sendOp will retry retryLimit times.
        private readonly int retryLimit;
        // if send data failed, sendOp will wait retryInterval milliseconds.
        private readonly int retryInterval;

        [NonSerialized]
        private List<PartitionPlan> partitionPlans;

        public SendClient(string receiveHostId)
        {
            this.receiveHostId = receiveHostId;
            this.retryLimit = 100;
            this.retryInterval = 10; // 10 ms
        }

        public SendClient(string receiveHostId, List<PartitionPlan> partitionPlans) : this(receiveHostId)
        {
            this.partitionPlans = partitionPlans;
        }

        private MessageServiceClient ConstructClient(int targetTask)
        {
            string ip = this.partitionPlans[targetTask].IP;
            int port = this.partitionPlans[targetTask].Port;
            UriBuilder uri = new UriBuilder("http", ip, port, this.receiveHostId + "/GraphView");
            EndpointAddress endpointAddress = new EndpointAddress(uri.ToString());
            WSHttpBinding binding = new WSHttpBinding();
            binding.Security.Mode = SecurityMode.None;
            return new MessageServiceClient(binding, endpointAddress);
        }

        public void SendMessage(string message, int targetTask)
        {
            MessageServiceClient client = ConstructClient(targetTask);

            for (int i = 0; i < this.retryLimit; i++)
            {
                try
                {
                    client.SendMessage(message);
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

        public void SendMessage(string message, int targetTask, int currentTask)
        {
            MessageServiceClient client = ConstructClient(targetTask);

            for (int i = 0; i < this.retryLimit; i++)
            {
                try
                {
                    client.SendMessageWithSource(message, currentTask);
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

        public void SendSignal(string signal, int targetTask)
        {
            MessageServiceClient client = ConstructClient(targetTask);

            for (int i = 0; i < this.retryLimit; i++)
            {
                try
                {
                    client.SendSignal(signal);
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

        public void SendSignal(string signal, int targetTask, int currentTask)
        {
            MessageServiceClient client = ConstructClient(targetTask);

            for (int i = 0; i < this.retryLimit; i++)
            {
                try
                {
                    client.SendSignalWithSource(signal, currentTask);
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

        public void SendRawRecord(RawRecord record, int targetTask)
        {
            string message = RawRecordMessage.CodeMessage(record);
            SendMessage(message, targetTask);
        }

        public void SetReceiveHostId(string id)
        {
            this.receiveHostId = id;
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            AdditionalSerializationInfo additionalInfo = (AdditionalSerializationInfo)context.Context;
            this.partitionPlans = additionalInfo.PartitionPlans;
        }
    }

    [Serializable]
    internal class ReceiveHost
    {
        private readonly string receiveHostId;
        public string ReceiveHostId => receiveHostId;

        [NonSerialized]
        private ServiceHost selfHost;
        [NonSerialized]
        private MessageService service;

        // Set following fields in deserialization
        [NonSerialized]
        private List<PartitionPlan> partitionPlans;
        [NonSerialized]
        private int currentTask;

        public ReceiveHost(string receiveHostId)
        {
            this.receiveHostId = receiveHostId;
            this.selfHost = null;
        }

        public ReceiveHost(string receiveHostId, int currentTask, List<PartitionPlan> partitionPlans)
        {
            this.receiveHostId = receiveHostId;
            this.currentTask = currentTask;
            this.partitionPlans = partitionPlans;
            this.selfHost = null;
        }

        public bool TryGetMessage(out string message)
        {
            return this.service.Messages.TryDequeue(out message);
        }

        public bool TryGetSignal(out string signal)
        {
            return this.service.Signals.TryDequeue(out signal);
        }

        public void OpenHost()
        {
            PartitionPlan ownPartitionPlan = this.partitionPlans[this.currentTask];
            UriBuilder uri = new UriBuilder("http", ownPartitionPlan.IP, ownPartitionPlan.Port, this.receiveHostId);
            Uri baseAddress = uri.Uri;

            WSHttpBinding binding = new WSHttpBinding();
            binding.Security.Mode = SecurityMode.None;
            // Set the max size of message.
            // If message is larger than this limit, WCF will return error "(413) Request Entity Too Large."
            binding.MaxReceivedMessageSize = 10485760; // 10 Mb

            this.selfHost = new ServiceHost(new MessageService(), baseAddress);
            this.selfHost.AddServiceEndpoint(typeof(IMessageService), binding, "GraphView");

            ServiceMetadataBehavior smb = new ServiceMetadataBehavior();
            smb.HttpGetEnabled = true;
            this.selfHost.Description.Behaviors.Add(smb);
            this.selfHost.Description.Behaviors.Find<ServiceBehaviorAttribute>().InstanceContextMode =
                InstanceContextMode.Single;

            this.service = selfHost.SingletonInstance as MessageService;

            this.selfHost.Open();
        }

        public List<string> WaitReturnAllMessages()
        {
            List<string> messages = new List<string>();
            List<bool> hasReceived = Enumerable.Repeat(false, this.partitionPlans.Count).ToList();
            hasReceived[this.currentTask] = true;

            while (true)
            {
                int from;
                if (this.service.Sources.TryDequeue(out from))
                {
                    hasReceived[from] = true;
                }

                string message;
                while (this.service.Messages.TryDequeue(out message))
                {
                    messages.Add(message);
                }

                bool canReturn = true;
                foreach (bool flag in hasReceived)
                {
                    if (!flag)
                    {
                        canReturn = false;
                        break;
                    }
                }

                if (canReturn)
                {
                    break;
                }
            }
            return messages;
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            AdditionalSerializationInfo additionalInfo = (AdditionalSerializationInfo)context.Context;
            this.partitionPlans = additionalInfo.PartitionPlans;
            this.currentTask = additionalInfo.TaskIndex;

            OpenHost();
        }
    }

    internal class AggregateIntermadiateResult
    {
        private readonly string receiveHostId;

        private readonly int currentTask;
        private List<PartitionPlan> partitionPlans;

        private GraphViewCommand command;

        private ReceiveHost receiveHost;

        public AggregateIntermadiateResult(string receiveHostId, int currentTask, List<PartitionPlan> partitionPlans, GraphViewCommand command)
        {
            this.receiveHostId = receiveHostId;
            this.currentTask = currentTask;
            this.partitionPlans = partitionPlans;
            this.command = command;
        }

        // For ProjectAggregation
        public bool Aggregate(List<IAggregateFunction> allAggFuncs)
        {
            List<IAggregateFunction> aggFuncs = allAggFuncs.Where(aggFunc => !(aggFunc is CapFunction)).ToList();
            if (aggFuncs.Count == 0)
            {
                return true;
            }

            int targetTask = DetermineTargetTask();

            if (this.currentTask == targetTask)
            {
                if (this.receiveHost == null)
                {
                    this.receiveHost = new ReceiveHost(this.receiveHostId, this.currentTask, this.partitionPlans);
                    this.receiveHost.OpenHost();
                }

                List<string> messages = this.receiveHost.WaitReturnAllMessages();

                foreach (string message in messages)
                {
                    List<IAggregateFunction> anotherAggFuncs = DeserializeAggregateFunctions(message, this.command);
                    for (int i = 0; i < aggFuncs.Count; i++)
                    {
                        aggFuncs[i].Merge(anotherAggFuncs[i]);
                    }
                }
                return true;
            }
            else
            {
                string message = SerializeAggregateFunctions(aggFuncs);
                SendClient sendClient = new SendClient(this.receiveHostId, this.partitionPlans);
                sendClient.SendMessage(message, targetTask, this.currentTask);
                return false;
            }
        }

        public bool Aggregate(AggregateState aggregateState)
        {
            GroupState groupState = aggregateState as GroupState;
            if (groupState != null)
            {
                int targetTask = DetermineTargetTask();

                if (this.currentTask == targetTask)
                {
                    if (this.receiveHost == null)
                    {
                        this.receiveHost = new ReceiveHost(this.receiveHostId, this.currentTask, this.partitionPlans);
                        this.receiveHost.OpenHost();
                    }

                    List<string> messages = this.receiveHost.WaitReturnAllMessages();

                    foreach (string message in messages)
                    {
                        GroupState anotherState = GroupState.Deserialize(this.command, message);
                        groupState.Merge(anotherState);
                    }
                    return true;
                }
                else
                {
                    string message = groupState.Serialize();
                    SendClient sendClient = new SendClient(this.receiveHostId, this.partitionPlans);
                    sendClient.SendMessage(message, targetTask, this.currentTask);
                    return false;
                }
            }

            throw new NotImplementedException();
        }

        // maybe use the amount of data that each task computes to determine target task.
        private int DetermineTargetTask()
        {
            return 0;
        }

        public enum AggregateFunctionType
        {
            FoldFunction,
            CountFunction,
            SumFunction,
            MaxFunction,
            MinFunction,
            MeanFunction,
            CapFunction,
            TreeFunction,
            SubgraphFunction,
            CollectionFunction,
            GroupFunction
        }

        public static string CombineSerializeResult(AggregateFunctionType type, string content)
        {
            return $"{type}:{content}";
        }

        public static IAggregateFunction DeserializeAggregateFunction(string serializeResult, GraphViewCommand command)
        {
            string[] values = serializeResult.Split(new char[]{ ':' }, 2);
            string type = values[0];
            string content = values[1];

            if (type.Equals(AggregateFunctionType.FoldFunction.ToString()))
            {
                return FoldFunction.DeserializeForAggregate(content, command);
            }
            else if (type.Equals(AggregateFunctionType.CountFunction.ToString()))
            {
                return CountFunction.DeserializeForAggregate(content);
            }
            else if (type.Equals(AggregateFunctionType.SumFunction.ToString()))
            {
                return SumFunction.DeserializeForAggregate(content);
            }
            else if (type.Equals(AggregateFunctionType.MaxFunction.ToString()))
            {
                return MaxFunction.DeserializeForAggregate(content);
            }
            else if (type.Equals(AggregateFunctionType.MinFunction.ToString()))
            {
                return MinFunction.DeserializeForAggregate(content);
            }
            else if (type.Equals(AggregateFunctionType.MeanFunction.ToString()))
            {
                return MeanFunction.DeserializeForAggregate(content);
            }
            // case: tree(). that is, tree step without sideEffectKey. And tree step is not inBatchMode.
            else if (type.Equals(AggregateFunctionType.TreeFunction.ToString()))
            {
                return TreeFunction.DeserializeForAggregate(content, command);
            }
            else
            {
                throw new GraphViewException("Should not arrive here.");
            }
        }

        public static string SerializeAggregateFunctions(List<IAggregateFunction> aggFuncs)
        {
            List<string> resultList = new List<string>();
            foreach (IAggregateFunction aggFunc in aggFuncs)
            {
                resultList.Add(aggFunc.SerializeForAggregate());
            }
            return GraphViewSerializer.SerializeWithDataContract(resultList);
        }

        public static List<IAggregateFunction> DeserializeAggregateFunctions(string serializeResult, GraphViewCommand command)
        {
            List<string> aggStrs = GraphViewSerializer.DeserializeWithDataContract<List<string>>(serializeResult);
            List<IAggregateFunction> aggFuncs = new List<IAggregateFunction>();
            foreach (string aggStr in aggStrs)
            {
                aggFuncs.Add(DeserializeAggregateFunction(aggStr, command));
            }
            return aggFuncs;
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
        private readonly int edgeFieldIndex;
        private readonly TraversalOperator.TraversalTypeEnum traversalType;

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
        private readonly GraphViewExecutionOperator inputOp;
        private string receiveOpId;

        [NonSerialized]
        private SendClient client;

        // The number of results that has been handled
        [NonSerialized]
        private int resultCount;

        public int ResultCount => this.resultCount;

        internal enum SendType
        {
            Send,
            SendAndAttachTaskId,
            Aggregate,
            SendBack,
            LastSendBack,
            Sync,
        }

        private int maxCount;
        public int AggregateTarget { get; private set; } = -1;

        private readonly GetPartitionMethod getPartitionMethod;
        private readonly SendType sendType;

        // Set following fields in deserialization
        [NonSerialized]
        private List<PartitionPlan> partitionPlans;
        [NonSerialized]
        private int taskIndex;

        private SendOperator(GraphViewExecutionOperator inputOp)
        {
            this.inputOp = inputOp;
            this.Open();
        }

        public SendOperator(GraphViewExecutionOperator inputOp, GetPartitionMethod getPartitionMethod, bool needSendBack) 
            : this(inputOp)
        {
            this.getPartitionMethod = getPartitionMethod;
            this.sendType = needSendBack ? SendType.SendAndAttachTaskId : SendType.Send;
        }

        public SendOperator(GraphViewExecutionOperator inputOp, SendType sendType)
            : this(inputOp)
        {
            Debug.Assert(sendType != SendType.Send && sendType != SendType.SendAndAttachTaskId);
            this.sendType = sendType;
        }

        public SendOperator(GraphViewExecutionOperator inputOp, int maxCount, int aggregateTarget)
            : this(inputOp)
        {
            this.sendType = SendType.Aggregate;
            this.maxCount = maxCount;
            this.AggregateTarget = aggregateTarget;
        }

        public override RawRecord Next()
        {
            RawRecord record;
            while (this.inputOp.State() && (record = this.inputOp.Next()) != null)
            {
                bool canClose = false;

                switch (this.sendType)
                {
                    case SendType.Sync:
                        this.resultCount++;
                        return record;
                        break;
                    case SendType.SendBack:
                    case SendType.LastSendBack:
                        int index = int.Parse(record[1].ToValue);
                        if (index == this.taskIndex || index == -1)
                        {
                            this.resultCount++;
                            return record;
                        }
                        else
                        {
                            this.client.SendRawRecord(record, index);
                        }
                        break;
                    case SendType.SendAndAttachTaskId:
                    case SendType.Send:
                        if (this.sendType == SendType.SendAndAttachTaskId)
                        {
                            Debug.Assert(((StringField)record[1]).Value == "-1" || ((StringField)record[1]).Value == this.taskIndex.ToString());
                            ((StringField)record[1]).Value = this.taskIndex.ToString();
                        }
                        string partition = this.getPartitionMethod.GetPartition(record);
                        if (this.partitionPlans[this.taskIndex].BelongToPartitionPlan(partition))
                        {
                            this.resultCount++;
                            return record;
                        }
                        for (int i = 0; i < this.partitionPlans.Count; i++)
                        {
                            if (i == this.taskIndex)
                            {
                                continue;
                            }
                            if (this.partitionPlans[i].BelongToPartitionPlan(partition))
                            {
                                this.client.SendRawRecord(record, i);
                                break;
                            }
                            if (i == this.partitionPlans.Count - 1)
                            {
                                throw new GraphViewException($"This partition does not belong to any partition plan! partition:{ partition }");
                            }
                        }
                        break;
                    case SendType.Aggregate:
                        // The meaning of this.resultCount in this type is different from other modes.
                        if (this.maxCount != -1 && this.resultCount == this.maxCount)
                        {
                            canClose = true;
                        }
                        this.resultCount++;

                        if (this.AggregateTarget == this.taskIndex)
                        {
                            return record;
                        }
                        else
                        {
                            this.client.SendRawRecord(record, this.AggregateTarget);
                        }

                        break;
                }

                if (canClose)
                {
                    break;
                }
            }

            string signal;
            if (this.sendType == SendType.LastSendBack)
            {
                EnumeratorOperator enumeratorOp = this.GetFirstOperator() as EnumeratorOperator;
                signal = $"{this.taskIndex},{this.resultCount},{enumeratorOp.ContainerHasMoreInput()}";
            }
            else
            {
                signal = $"{this.taskIndex},{this.resultCount}";
            }

            for (int i = 0; i < this.partitionPlans.Count; i++)
            {
                if (i == this.taskIndex)
                {
                    continue;
                }
                string message = $"{this.taskIndex},{this.resultCount}";
                this.client.SendSignal(message, i);
            }

            this.Close();
            return null;
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
            this.resultCount = 0;
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            AdditionalSerializationInfo additionalInfo = (AdditionalSerializationInfo)context.Context;
            this.partitionPlans = additionalInfo.PartitionPlans;
            this.taskIndex = additionalInfo.TaskIndex;
            this.resultCount = 0;

            this.client = new SendClient(this.receiveOpId, this.partitionPlans);
        }
    }

    [Serializable]
    internal class ReceiveOperator : GraphViewExecutionOperator
    {
        private readonly SendOperator inputOp;
        private readonly string id;

        private readonly bool needFetchRawRecord;

        [NonSerialized]
        private ReceiveHost receiveHost;

        [NonSerialized]
        private List<bool> hasBeenSignaled;
        [NonSerialized]
        private List<int> resultCountList;
        [NonSerialized]
        private bool otherContainerHasMoreInput;

        // Set following fields in deserialization
        [NonSerialized]
        private List<PartitionPlan> partitionPlans;
        [NonSerialized]
        private int taskIndex;
        [NonSerialized]
        private GraphViewCommand command;

        public ReceiveOperator(SendOperator inputOp, bool needFetchRawRecord = true)
        {
            this.inputOp = inputOp;
            this.id = Guid.NewGuid().ToString("N");
            this.inputOp.SetReceiveOpId(this.id);
            this.needFetchRawRecord = needFetchRawRecord;

            this.Open();
        }

        public override RawRecord Next()
        {
            RawRecord record;
            if (this.inputOp.State() && (record = this.inputOp.Next()) != null)
            {
                return record;
            }

            this.resultCountList[this.taskIndex] = this.inputOp.ResultCount;

            // todo : use loop temporarily. need change later.
            while (true)
            {
                if (this.needFetchRawRecord)
                {
                    string message;
                    if (this.receiveHost.TryGetMessage(out message))
                    {
                        return RawRecordMessage.DecodeMessage(message, this.command);
                    }
                }

                string acknowledge;
                while (this.receiveHost.TryGetSignal(out acknowledge))
                {
                    string[] messages = acknowledge.Split(',');
                    int ackTaskId = Int32.Parse(messages[0]);
                    int ackResultCount = Int32.Parse(messages[1]);
                    if (messages.Length == 3)
                    {
                        this.otherContainerHasMoreInput |= Boolean.Parse(messages[2]);
                    }

                    this.hasBeenSignaled[ackTaskId] = true;
                    this.resultCountList[ackTaskId] = ackResultCount;
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

            this.Close();
            return null;
        }

        public bool HasGlobalResult()
        {
            foreach (int resultCount in this.resultCountList)
            {
                if (resultCount > 0)
                {
                    return true;
                }
            }
            return false;
        }

        public bool OtherContainerHasMoreResult()
        {
            return this.otherContainerHasMoreInput;
        }

        public override GraphViewExecutionOperator GetFirstOperator()
        {
            return this.inputOp.GetFirstOperator();
        }

        public override void ResetState()
        {
            this.Open();
            this.inputOp.ResetState();
            this.hasBeenSignaled = Enumerable.Repeat(false, this.partitionPlans.Count).ToList();
            this.hasBeenSignaled[this.taskIndex] = true;
            if (this.inputOp.AggregateTarget != -1)
            {
                this.hasBeenSignaled[this.inputOp.AggregateTarget] = true;
            }
            this.resultCountList = Enumerable.Repeat(0, this.partitionPlans.Count).ToList();
            this.otherContainerHasMoreInput = false;
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            AdditionalSerializationInfo additionalInfo = (AdditionalSerializationInfo)context.Context;
            this.command = additionalInfo.Command;
            this.partitionPlans = additionalInfo.PartitionPlans;
            this.taskIndex = additionalInfo.TaskIndex;

            this.hasBeenSignaled = Enumerable.Repeat(false, this.partitionPlans.Count).ToList();
            this.hasBeenSignaled[this.taskIndex] = true;
            if (this.inputOp.AggregateTarget != -1)
            {
                this.hasBeenSignaled[this.inputOp.AggregateTarget] = true;
            }
            this.resultCountList = Enumerable.Repeat(0, this.partitionPlans.Count).ToList();
            this.otherContainerHasMoreInput = false;

            this.receiveHost = new ReceiveHost(this.id, this.taskIndex, this.partitionPlans);
            this.receiveHost.OpenHost();
        }
    }

    // for Debug
    //public class StartHostForDevelopment
    //{
    //    public static void Main(string[] args)
    //    {
    //        Uri baseAddress = new Uri("http://localhost:8000/Host1/");

    //        ServiceHost selfHost = new ServiceHost(new MessageService(), baseAddress);

    //        selfHost.AddServiceEndpoint(typeof(IMessageService), new WSHttpBinding(), "1");

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
        //                           jsonString, otherV, otherVPartition
        [DataMember]
        private Dictionary<string, Tuple<string, string, string>> forwardEdges;
        [DataMember]
        private Dictionary<string, Tuple<string, string, string>> backwardEdges;

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
            this.forwardEdges = new Dictionary<string, Tuple<string, string, string>>();
            this.backwardEdges = new Dictionary<string, Tuple<string, string, string>>();

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

            EntryField entryField = fieldObject as EntryField;
            if (entryField != null)
            {
                AnalysisFieldObject(entryField.Key);
                AnalysisFieldObject(entryField.Value);
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
                    AnalysisFieldObject(treeNode.NodeObject);
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
                    this.forwardEdges[edgeId] = new Tuple<string,string,string>(
                        edgeField.EdgeJObject.ToString(), edgeField.OtherV, edgeField.OtherVPartition);
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
                    this.backwardEdges[edgeId] = new Tuple<string, string, string>(
                        edgeField.EdgeJObject.ToString(), edgeField.OtherV, edgeField.OtherVPartition);
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

        private RawRecord ReconstructRawRecord(GraphViewCommand command)
        {
            this.vertexFields = new Dictionary<string, VertexField>();
            this.forwardEdgeFields = new Dictionary<string, EdgeField>();
            this.backwardEdgeFields = new Dictionary<string, EdgeField>();

            foreach (KeyValuePair<string, string> pair in this.vertices)
            {
                VertexField vertex;
                if (!command.VertexCache.TryGetVertexField(pair.Key, out vertex))
                {
                    command.VertexCache.AddOrUpdateVertexField(pair.Key, JObject.Parse(pair.Value));
                    command.VertexCache.TryGetVertexField(pair.Key, out vertex);
                }
                this.vertexFields[pair.Key] = vertex;
            }

            foreach (KeyValuePair<string, Tuple<string, string, string>> pair in this.forwardEdges)
            {
                JObject edgeJObject = JObject.Parse(pair.Value.Item1);
                string outVId = (string)edgeJObject[KW_EDGE_SRCV];
                string outVLable = (string)edgeJObject[KW_EDGE_SRCV_LABEL];
                string outVPartition = (string)edgeJObject[KW_EDGE_SRCV_PARTITION];
                string edgeDocId = (string)edgeJObject[KW_DOC_ID];

                //this.vertexFields[outVId].AdjacencyList.TryAddEdgeField(pair.Key,
                //    () => EdgeField.ConstructForwardEdgeField(outVId, outVLable, outVPartition, edgeDocId, edgeJObject));
                EdgeField edgeField = EdgeField.ConstructForwardEdgeField(outVId, outVLable, outVPartition, edgeDocId, edgeJObject);
                this.forwardEdgeFields[pair.Key] = new EdgeField(edgeField, pair.Value.Item2, pair.Value.Item3);
            }

            foreach (KeyValuePair<string, Tuple<string, string, string>> pair in this.backwardEdges)
            {
                JObject edgeJObject = JObject.Parse(pair.Value.Item1);
                string inVId = (string)edgeJObject[KW_EDGE_SINKV];
                string inVLable = (string)edgeJObject[KW_EDGE_SINKV_LABEL];
                string inVPartition = (string)edgeJObject[KW_EDGE_SINKV_PARTITION];
                string edgeDocId = (string)edgeJObject[KW_DOC_ID];

                //this.vertexFields[inVId].RevAdjacencyList.TryAddEdgeField(pair.Key,
                //    () => EdgeField.ConstructBackwardEdgeField(inVId, inVLable, inVPartition, edgeDocId, edgeJObject));
                EdgeField edgeField = EdgeField.ConstructBackwardEdgeField(inVId, inVLable, inVPartition, edgeDocId, edgeJObject);
                this.backwardEdgeFields[pair.Key] = new EdgeField(edgeField, pair.Value.Item2, pair.Value.Item3);
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
                for (int i = 0; i < pathField.Path.Count; i++)
                {
                    if (pathField.Path[i] == null)
                    {
                        continue;
                    }
                    PathStepField pathStepField = pathField.Path[i] as PathStepField;
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
                List<string> keyList = new List<string>(compositeField.CompositeFieldObject.Keys);
                CompositeField newCompositeField = new CompositeField(compositeField.CompositeFieldObject, compositeField.DefaultProjectionKey);
                foreach (string key in keyList)
                {
                    newCompositeField[key] = RecoverFieldObject(newCompositeField[key]);
                }
                return newCompositeField;
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
