using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps
{
    internal class GremlinVariable
    {
        public string VariableName { get; set; }

        public override int GetHashCode()
        {
            return VariableName.GetHashCode();
        }
    }

    internal enum GremlinEdgeType
    {
        InEdge,
        OutEdge,
        BothEdge
    }

    internal class GremlinVertexVariable : GremlinVariable
    {
        public GremlinVertexVariable()
        {
            //automaticlly generate the name of node
            VariableName = "N_" + GremlinVertexVariable._count.ToString();
            _count += 1;
        }
        private static long _count = 0;
    }
    internal class GremlinEdgeVariable : GremlinVariable
    {
        public GremlinEdgeVariable()
        {
            //automaticlly generate the name of edge
            VariableName = "E_" + GremlinEdgeVariable._count.ToString();
            _count += 1;
        }

        public GremlinEdgeVariable(GremlinEdgeType type)
        {
            //automaticlly generate the name of edge
            VariableName = "E_" + GremlinEdgeVariable._count.ToString();
            _count += 1;
            EdgeType = type;
        }

        private static long _count = 0;
        public GremlinEdgeType EdgeType { get; set; }
    }

    internal class GremlinRecursiveEdgeVariable : GremlinVariable
    {
        public WSelectQueryBlock GremlinTranslationOperatorQuery { get; set; }
        public int IterationCount;
        public WBooleanExpression UntilCondition { get; set; }
    }

    internal class GremlinDerivedVariable : GremlinVariable
    {
        
    }

    public enum Scope
    {
        local,
        global
    }
}
