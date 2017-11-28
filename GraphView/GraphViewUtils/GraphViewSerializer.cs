using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Soap;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal static class GraphViewSerializer
    {
        private static string commandFile = "command.xml";
        private static string operatorsFile = "plan.xml";

        public static void SerializeCommand(GraphViewCommand command)
        {
            SoapFormatter serilizer = new SoapFormatter();
            Stream stream = File.Open(commandFile, FileMode.Create);
            serilizer.Serialize(stream, command);
            stream.Close();
        }

        public static GraphViewCommand DeserializeCommand()
        {
            SoapFormatter deserilizer = new SoapFormatter();
            Stream stream = File.Open(commandFile, FileMode.Open);
            GraphViewCommand command = (GraphViewCommand)deserilizer.Deserialize(stream);
            stream.Close();
            return command;
        }

        public static void SerializeOperators(GraphViewExecutionOperator op)
        {
            Stream stream = File.Open(operatorsFile, FileMode.Create);
            DataContractSerializer ser = new DataContractSerializer(typeof(GraphViewExecutionOperator));
            ser.WriteObject(stream, op);
            stream.Close();
        }

        public static GraphViewExecutionOperator DeserializeOperators()
        {
            Stream stream = File.Open(operatorsFile, FileMode.Open);
            DataContractSerializer deser = new DataContractSerializer(typeof(GraphViewExecutionOperator));
            GraphViewExecutionOperator op = (GraphViewExecutionOperator)deser.ReadObject(stream);
            stream.Close();
            return op;
        }

    }
}
