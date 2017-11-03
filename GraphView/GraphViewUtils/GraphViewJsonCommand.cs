using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using static GraphView.DocumentDBKeywords;

// Add DocumentDB references
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GraphView
{
    public static class GraphViewJsonCommand
    {
        public static JValue ToJValue(this WValueExpression expr)
        {
            //
            // JSON requires a lower case string if it is a boolean value
            //
            bool boolValue;
            if (!expr.SingleQuoted && bool.TryParse(expr.Value, out boolValue)) {
                return (JValue)boolValue;
            }
            else if (expr.SingleQuoted) {
                return (JValue)expr.Value;  // String value
            }
            else {
                return (JValue)JToken.Parse(expr.ToString());
            }
        }

        internal static JValue ToJValue(this StringField field)
        {
            if (field.JsonDataType == JsonDataType.Boolean)
            {
                return (JValue)bool.Parse(field.Value);
            }
            else if (field.JsonDataType == JsonDataType.String)
            {
                return (JValue)field.Value;
            }
            else
            {
                return (JValue)JToken.Parse(field.Value);
            }
        }

        public static JProperty UpdateProperty(JObject jsonObject, WValueExpression fieldName, WValueExpression fieldValue)
        {
            string key = fieldName.Value;

            //
            // Special treat when fieldValue indicates null (that is, to remove a property)
            // NOTE: fieldValue itself != null
            //
            if (!fieldValue.SingleQuoted && fieldValue.Value.Equals("null", StringComparison.OrdinalIgnoreCase)) {
                jsonObject.Property(key)?.Remove();
                return null;
            }

            //
            // JSON requires a lower case string if it is a boolean value
            //
            bool boolValue;
            if (!fieldValue.SingleQuoted && bool.TryParse(fieldValue.Value, out boolValue)) {
                jsonObject[key] = (JToken)boolValue;
            }
            else {
                jsonObject[key] = JToken.Parse(fieldValue.ToString());
            }
            return jsonObject.Property(key);
        }

        public static JProperty UpdateProperty(JObject jsonObject, string name, JValue value)
        {
            jsonObject[name] = value;
            return jsonObject.Property(name);
        }

        public static void DropProperty(JObject jsonObject, string propertyName)
        {
            jsonObject.Property(propertyName)?.Remove();
        }


        [DebuggerStepThrough]
        public static void UpdateEdgeMetaProperty(
            JObject edgeJObject, string edgeId, bool isReverseEdge, string srcOrSinkVId, string srcOrSinkVLabel, string srcOrSinkPartition)
        {
            edgeJObject[KW_EDGE_ID] = edgeId;
            if (isReverseEdge) {
                edgeJObject[KW_EDGE_SRCV] = srcOrSinkVId;
                edgeJObject[KW_EDGE_SRCV_LABEL] = (JValue)srcOrSinkVLabel ?? JValue.CreateNull();
                edgeJObject[KW_EDGE_SRCV_PARTITION] = (JValue)srcOrSinkPartition ?? JValue.CreateNull();
            }
            else {
                edgeJObject[KW_EDGE_SINKV] = srcOrSinkVId;
                edgeJObject[KW_EDGE_SINKV_LABEL] = (JValue)srcOrSinkVLabel ?? JValue.CreateNull();
                edgeJObject[KW_EDGE_SINKV_PARTITION] = (JValue)srcOrSinkPartition ?? JValue.CreateNull();
            }
        }
    }
}