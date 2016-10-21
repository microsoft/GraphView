using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace GraphView
{
    class JsonAuxiliary
    {
        public List<string> RetriveNodePropertyFromDocument(JObject item,string pDocumentName, string pFieldName)
        {
            List<string> result = new List<string>();
            JToken document = ((JObject)item)[pDocumentName];
            JToken FieldValue = ((JObject)document)[pFieldName];
            if (FieldValue == null) return null;
            if (FieldValue is JValue) result.Add(FieldValue.ToString());
            else
            {
                foreach (var value in FieldValue as JArray)
                    result.Add(value.ToString());
            }
            return result;
        }
        public Dictionary<string,List<string>> RetriveNodePropertyFromDocument(JObject item, string pDocumentName)
        {
            Dictionary<string, List<string>> result = new Dictionary<string, List<string>>();
            JToken document = ((JObject)item)[pDocumentName];
            foreach (var a in document.Children())
            {
                string propertyname = (a as JProperty).Name;
                if ((a as JProperty).Value is JArray)
                {
                    result.Add(propertyname,new List<string>());
                    foreach (var v in (a as JProperty).Value as JArray)
                    result[propertyname].Add(v.ToString());
                }
                if ((a as JProperty).Value is JValue)
                {
                    result.Add(propertyname, new List<string>());
                        result[propertyname].Add((a as JProperty).Value.ToString());
                }
            }
            return result;
        }

        public string RetriveNodePropertyStringFromDocument(JObject item, string pDocumentName, string pFieldName)
        {
            string result = "";
            List<string> input = RetriveNodePropertyFromDocument(item, pDocumentName, pFieldName);
            foreach (var x in input) result += "\"" + x + "\",";
            if (result != "") result = result.Substring(0, result.Length - 1);
            return result;
        }
    public List<string> RetriveEdgePropertyFromDocument(JObject item, string pDocumentName, string pFieldName)
        {
            List<string> result = new List<string>();
            JToken document = ((JObject)item)[pDocumentName];
            JToken edge = ((JObject)document)["edge"];
            foreach (var x in edge)
            {
                result.Add(x[pFieldName].ToString());
            }
            return result;
        }
        public Dictionary<string, string> RetriveEdgePropertyStringFromDocument(JObject item, string pDocumentName, string pFieldName)
        {
            Dictionary<string, string> result = new Dictionary<string, string>();
            JToken document = ((JObject)item)[pDocumentName];
            JToken edge = ((JObject)document)["edge"];
            foreach (var x in edge)
            {
                foreach (var a in x.Children())
                {
                    string propertyname = (a as JProperty).Name;
                    if ((a as JProperty).Value is JArray)
                    {
                        if (!result.ContainsKey(propertyname)) result.Add(propertyname, "");
                        foreach (var v in (a as JProperty).Value as JArray)
                            result[propertyname] += "\"" + v + "\",";
                    }
                    if ((a as JProperty).Value is JValue)
                    {
                        if (!result.ContainsKey(propertyname)) result.Add(propertyname, "");
                        result[propertyname] += "\"" + (a as JProperty).Value.ToString() + "\",";
                    }
                }
            }
            foreach (var a in edge[0].Children())
            {
                string propertyname = (a as JProperty).Name;
                result[propertyname] = result[propertyname].Substring(0, result[propertyname].Length - 1);
            }
            return result;
        }
    }

}
