using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using static GraphView.GraphViewKeywords;

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
            // Special treat when fieldValue indicates null (that is, to remove a property)
            // NOTE: fieldValue itself != null
            //
            if (!expr.SingleQuoted && expr.Value.Equals("null", StringComparison.OrdinalIgnoreCase)) {
                return null;
            }

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

        //public static void AppendVertexSinglePropertyToVertex(JObject vertexJObject, JObject)
        //{
        //    // TODO
        //}

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

        /// <summary>
        /// Drop a node's all non-reserved properties
        /// </summary>
        /// <param name="jsonObject"></param>
        /// <returns></returns>
        public static List<string> DropAllNodeProperties(JObject jsonObject)
        {
            List<string> toBeDroppedPropertiesName = new List<string>();

            foreach (var property in jsonObject.Properties().ToList()) {
                string name = property.Name;
                switch (name) {
                // Reversed properties for meta-data
                case KW_DOC_ID:
                case KW_DOC_PARTITION:
                case KW_VERTEX_EDGE:
                case KW_VERTEX_REV_EDGE:
                case KW_VERTEX_LABEL:
                //case KW_VERTEX_NEXTOFFSET:

                case "_rid":
                case "_self":
                case "_etag":
                case "_attachments":
                case "_ts":
                    continue;
                default:
                    property.Remove();
                    toBeDroppedPropertiesName.Add(name);
                    break;
                }
            }

            return toBeDroppedPropertiesName;
        }

        /// <summary>
        /// Drop an edge's all non-reserved properties
        /// </summary>
        /// <param name="jsonObject"></param>
        /// <returns></returns>
        public static List<string> DropAllEdgeProperties(JObject jsonObject)
        {
            List<string> toBeDroppedProperties = new List<string>();

            foreach (var property in jsonObject.Properties().ToList()) {
                string name = property.Name;
                switch (name) {
                // Reversed properties for meta-data
                case KW_EDGE_ID:
                //case KW_EDGE_OFFSET:
                case KW_EDGE_SRCV:
                case KW_EDGE_SRCV_LABEL:
                case KW_EDGE_SINKV:
                case KW_EDGE_SINKV_LABEL:
                case KW_EDGE_LABEL:
                    continue;
                default:
                    property.Remove();
                    toBeDroppedProperties.Add(name);
                    break;
                }
            }

            return toBeDroppedProperties;
        }

        [DebuggerStepThrough]
        public static void UpdateEdgeMetaProperty(
            JObject edgeJObject, string edgeId, bool isReverseEdge, string srcOrSinkVId, string srcOrSinkVLabel)
        {
            edgeJObject[KW_EDGE_ID] = edgeId;
            if (isReverseEdge) {
                edgeJObject[KW_EDGE_SRCV] = srcOrSinkVId;
                edgeJObject[KW_EDGE_SRCV_LABEL] = (JValue)srcOrSinkVLabel ?? JValue.CreateNull();
            }
            else {
                edgeJObject[KW_EDGE_SINKV] = srcOrSinkVId;
                edgeJObject[KW_EDGE_SINKV_LABEL] = (JValue)srcOrSinkVLabel ?? JValue.CreateNull();
            }
        }
    }
}