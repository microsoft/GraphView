using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView.GremlinTranslation
{
    internal enum GremlinVariableType
    {
        Vertex,
        Edge,
        Scalar,
        Table,
        Undefined
    }
    internal abstract class GremlinVariable
    {

        public string VariableName { get; set; }
        public List<string> Properties = new List<string>();
        public string SetVariableName { get; set; }
        public int Low = Int32.MinValue;
        public int High = Int32.MaxValue;

        public virtual GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Undefined;
        }
    }

    internal class GremlinMatchPath
    {
        public GremlinVariable SourceVariable { get; set; }
        public GremlinVariable EdgeVariable { get; set; }
        public GremlinVariable SinkVariable { get; set; }

        public GremlinMatchPath(GremlinVariable sourceVariable, GremlinVariable edgeVariable, GremlinVariable sinkVariable)
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
