using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal abstract class GremlinCoalesceVariable : GremlinBinaryVariable
    {

        public GremlinCoalesceVariable(GremlinToSqlContext2 traversal1, GremlinToSqlContext2 traversal2)
            : base(traversal1, traversal2) { }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Table;
        }
    }

    internal class GremlinCoalesceVertexVariable : GremlinCoalesceVariable, ISqlTable
    {
        public GremlinCoalesceVertexVariable(GremlinToSqlContext2 traversal1, GremlinToSqlContext2 traversal2)
            : base(traversal1, traversal2) { }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Vertex;
        }

        internal override GremlinScalarVariable DefaultProjection()
        {
            return new GremlinVariableProperty(this, "id");
        }

        internal override void Both(GremlinToSqlContext2 currentContext)
        {
            base.Both(currentContext);
        }
    }

    internal class GremlinCoalesceEdgeVariable : GremlinCoalesceVariable, ISqlTable
    {
        public GremlinCoalesceEdgeVariable(GremlinToSqlContext2 traversal1, GremlinToSqlContext2 traversal2)
            : base(traversal1, traversal2) { }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Edge;
        }
    }

    internal class GremlinCoalesceTableVariable : GremlinCoalesceVariable, ISqlTable
    {
        public GremlinCoalesceTableVariable(GremlinToSqlContext2 traversal1, GremlinToSqlContext2 traversal2)
            : base(traversal1, traversal2) { }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Table;
        }
    }

    internal class GremlinCoalesceValueVariable : GremlinCoalesceVariable
    {
        public GremlinCoalesceValueVariable(GremlinToSqlContext2 traversal1, GremlinToSqlContext2 traversal2)
            : base(traversal1, traversal2) { }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Scalar;
        }
    }
}
