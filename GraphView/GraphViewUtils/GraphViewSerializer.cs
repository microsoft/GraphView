using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Soap;
using System.Text;
using System.Threading.Tasks;
using GraphView.GraphViewDBPortal;

namespace GraphView
{
    internal static class GraphViewSerializer
    {
        private static string commandFile = "command.xml";
        private static string operatorsFile = "plan.xml";
        private static string sideEffectFile = "sideEffect.xml";
        private static string containerFile = "container.xml";

        public static void Serialize(GraphViewCommand command, GraphViewExecutionOperator op)
        {
            SoapFormatter serilizer = new SoapFormatter();
            Stream stream = File.Open(commandFile, FileMode.Create);
            serilizer.Serialize(stream, command);
            stream.Close();

            stream = File.Open(containerFile, FileMode.Create);
            DataContractSerializer containerSer = new DataContractSerializer(typeof(List<Container>));
            containerSer.WriteObject(stream, SerializationData.Containers);
            stream.Close();

            stream = File.Open(sideEffectFile, FileMode.Create);
            DataContractSerializer sideEffectSer = new DataContractSerializer(typeof(Dictionary<string, IAggregateFunction>),
                new List<Type>{typeof(CollectionFunction), typeof(GroupFunction), typeof(SubgraphFunction), typeof(TreeFunction) });
            sideEffectSer.WriteObject(stream, SerializationData.SideEffectStates);
            stream.Close();

            stream = File.Open(operatorsFile, FileMode.Create);
            DataContractSerializer ser = new DataContractSerializer(typeof(GraphViewExecutionOperator));
            ser.WriteObject(stream, op);
            stream.Close();
        }

        public static GraphViewExecutionOperator Deserialize(out GraphViewCommand command)
        {
            SoapFormatter deserilizer = new SoapFormatter();
            Stream stream = File.Open(commandFile, FileMode.Open);
            command = (GraphViewCommand)deserilizer.Deserialize(stream);
            SerializationData.SetCommand(command);
            stream.Close();

            stream = File.Open(containerFile, FileMode.Open);
            DataContractSerializer containerDeser = new DataContractSerializer(typeof(List<Container>));
            SerializationData.SetContainers((List<Container>)containerDeser.ReadObject(stream));
            stream.Close();

            stream = File.Open(sideEffectFile, FileMode.Open);
            DataContractSerializer sideEffectDeser = new DataContractSerializer(typeof(Dictionary<string, IAggregateFunction>),
                new List<Type> { typeof(CollectionFunction), typeof(GroupFunction), typeof(SubgraphFunction), typeof(TreeFunction) });
            SerializationData.SetSideEffectStates((Dictionary<string, IAggregateFunction>)sideEffectDeser.ReadObject(stream));
            stream.Close();

            stream = File.Open(operatorsFile, FileMode.Open);
            DataContractSerializer deser = new DataContractSerializer(typeof(GraphViewExecutionOperator));
            GraphViewExecutionOperator op = (GraphViewExecutionOperator)deser.ReadObject(stream);
            stream.Close();

            op.ResetState();
            return op;
        }

    }

    internal static class SerializationData
    {
        public static Dictionary<string, IAggregateFunction> SideEffectStates { get; private set; } = new Dictionary<string, IAggregateFunction>();

        public static List<Container> Containers { get; private set; } = new List<Container>();
        public static int index = 0;

        public static GraphViewCommand Command { get; private set; }

        public static void SetSideEffectStates(Dictionary<string, IAggregateFunction> sideEffectStates)
        {
            SerializationData.SideEffectStates = sideEffectStates;
        }

        public static void AddSideEffectState(string key, IAggregateFunction value)
        {
            SerializationData.SideEffectStates[key] = value;
        }

        public static void SetContainers(List<Container> containers)
        {
            SerializationData.Containers = containers;
        }

        public static int AddContainers(Container container)
        {
            SerializationData.Containers.Add(container);
            return SerializationData.index++;
        }

        public static void SetCommand(GraphViewCommand command)
        {
            SerializationData.Command = command;
        }
    }
}
