using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinEdgeVariable : GremlinVariable
    {
        private static long _count = 0;

        public static string GetVariableName()
        {
            return "E_" + _count++;
        }

        public GremlinVariable SourceVariable { get; set; }
        public WEdgeType EdgeType { get; set; }

        public GremlinEdgeVariable(WEdgeType type)
        {
            VariableName = GetVariableName();
            EdgeType = type;
        }

        public GremlinEdgeVariable(GremlinVariable sourceVariable, WEdgeType type)
        {
            VariableName = GetVariableName();
            EdgeType = type;
            SourceVariable = sourceVariable;
        }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Edge;
        }
    }

}
