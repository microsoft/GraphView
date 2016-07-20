using System;
using Newtonsoft.Json.Linq;
using 
public class GraphView
{
	public class JsonAuxiliary
	{
	    public string RetrivePropertyValueFromDocument(string Documentname, string Fieldname)
	    {
	        JToken Document = 
	    }
        private Tuple<string, string, string> DecodeJObject(JObject Item)
        {
            JToken NodeInfo = ((JObject)Item)["NodeInfo"];
            JToken id = NodeInfo["id"];
            JToken edge = ((JObject)NodeInfo)["edge"];
            JToken reverse = ((JObject)NodeInfo)["reverse"];
            string ReverseEdgeID = "";
            foreach (var x in reverse)
            {
                ReverseEdgeID += "\"" + x["_sink"] + "\"" + ",";
            }
            string EdgeID = "";
            foreach (var x in edge)
            {
                EdgeID += "\"" + x["_sink"] + "\"" + ",";
            }
            return new Tuple<string, string, string>(id.ToString(), CutTheTail(EdgeID), CutTheTail(ReverseEdgeID));
        }
    }
}
