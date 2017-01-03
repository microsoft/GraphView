using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal abstract class GremlinVariable
    {

        public string VariableName { get; set; }
        public List<string> Properties = new List<string>();
        public string SetVariableName { get; set; }
        public long Low = Int64.MinValue;
        public long High = Int64.MaxValue;

        public virtual GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Undefined;
        }
    }

    internal class GremlinMatchPath
    {
        public GremlinVertexVariable2 SourceVariable { get; set; }
        public GremlinEdgeVariable2 EdgeVariable { get; set; }
        public GremlinVertexVariable2 SinkVariable { get; set; }

        public GremlinMatchPath(GremlinVertexVariable2 sourceVariable, GremlinEdgeVariable2 edgeVariable, GremlinVertexVariable2 sinkVariable)
        {
            SourceVariable = sourceVariable;
            EdgeVariable = edgeVariable;
            SinkVariable = sinkVariable;
        }
    }


    internal class OrderByRecord
    {
        public List<WExpressionWithSortOrder> SortOrderList { get; set; }

        public OrderByRecord()
        {
            SortOrderList = new List<WExpressionWithSortOrder>();
        }
    }

    internal class GroupByRecord
    {
        public List<WGroupingSpecification> GroupingSpecList { get; set; }

        public GroupByRecord()
        {
            GroupingSpecList = new List<WGroupingSpecification>();
        }
    }
}
