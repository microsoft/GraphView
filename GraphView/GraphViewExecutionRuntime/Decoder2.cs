using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace GraphView.GraphViewExecutionRuntime
{
    internal abstract class Decoder2
    {
        // Decode JObjects into List<RawRecord>
        internal abstract List<RawRecord> GetVertices(
            List<dynamic> items, List<string> nodeProperties, List<MatchEdge> reverseEdges = null);
    }

    internal class DocDbDecoder2 : Decoder2
    {
        internal override List<RawRecord> GetVertices(
            List<dynamic> items, List<string> nodeProperties, List<MatchEdge> reverseEdges = null)
        {
            if (reverseEdges == null)
                reverseEdges = new List<MatchEdge>();

            var newRecordLength = nodeProperties.Count +
                                  reverseEdges.Select(n => n.Properties.Count)
                                      .Aggregate(0, (cur, next) => cur + next);
            var results = new List<RawRecord>();

            foreach (var dynamicItem in items)
            {
                var rawRecord = new RawRecord(newRecordLength);
                var item = (JObject) dynamicItem;
                var index = 0;
                foreach (var property in nodeProperties)
                {
                    var propertyValue = item[property];
                    if (propertyValue != null)
                        rawRecord.fieldValues[index] = propertyValue.ToString();
                    ++index;
                }
                foreach (var edge in reverseEdges)
                {
                    var edgeObjectName = edge.EdgeAlias + "_Object";
                    var edgeObject = (JObject) item[edgeObjectName];

                    foreach (var property in edge.Properties)
                    {
                        var propertyValue = edgeObject[property];
                        if (propertyValue != null)
                            rawRecord.fieldValues[index] = propertyValue.ToString();
                        ++index;
                    }
                }

                results.Add(rawRecord);
            }

            return results;
        }
    }
}
