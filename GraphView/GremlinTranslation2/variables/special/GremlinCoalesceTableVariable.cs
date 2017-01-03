using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal abstract class GremlinCoalesceVariable : GremlinBinaryVariable
    {

        public GremlinCoalesceVariable(GremlinToSqlContext traversal1, GremlinToSqlContext traversal2)
            : base(traversal1, traversal2) { }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Table;
        }

        public WTableReference ToTableReference()
        {
            throw new NotImplementedException();
        }
    }

    internal class GremlinCoalesceVertexVariable : GremlinCoalesceVariable, ISqlTable
    {
        public GremlinCoalesceVertexVariable(GremlinToSqlContext traversal1, GremlinToSqlContext traversal2)
            : base(traversal1, traversal2) { }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Vertex;
        }

        internal override void Both(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            base.Both(currentContext, edgeLabels);
        }

        public WTableReference ToTableReference()
        {
            throw new NotImplementedException();
        }
    }

    internal class GremlinCoalesceEdgeVariable : GremlinCoalesceVariable, ISqlTable
    {
        public GremlinCoalesceEdgeVariable(GremlinToSqlContext traversal1, GremlinToSqlContext traversal2)
            : base(traversal1, traversal2) { }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Edge;
        }

        public WTableReference ToTableReference()
        {
            throw new NotImplementedException();
        }
    }

    internal class GremlinCoalesceTableVariable : GremlinCoalesceVariable, ISqlTable
    {
        public GremlinCoalesceTableVariable(GremlinToSqlContext traversal1, GremlinToSqlContext traversal2)
            : base(traversal1, traversal2) { }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Table;
        }

        public WTableReference ToTableReference()
        {
            throw new NotImplementedException();
        }
    }

    internal class GremlinCoalesceValueVariable : GremlinCoalesceVariable, ISqlTable
    {
        public GremlinCoalesceValueVariable(GremlinToSqlContext traversal1, GremlinToSqlContext traversal2)
            : base(traversal1, traversal2) { }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Scalar;
        }

        public WTableReference ToTableReference()
        {
            throw new NotImplementedException();
        }
    }
}
