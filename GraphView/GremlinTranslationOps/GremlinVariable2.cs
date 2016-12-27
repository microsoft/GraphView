using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal enum GremlinVariableType
    {
        Vertex,
        Edge,
        Scalar,
        Table,
        Undefined
    }

    internal abstract class GremlinVariable2
    {
        public string VariableName { get; set; }

        public virtual GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Undefined;
        }

        internal virtual void Populate(string name) { }

        internal virtual void And(
            GremlinToSqlContext2 currentContext, 
            GremlinToSqlContext2 subContext1,
            GremlinToSqlContext2 subContext2)
        {
        }

        internal virtual void By(GremlinToSqlContext2 currentContext, GremlinToSqlContext2 byContext)
        {
        }

        internal virtual void By(GremlinToSqlContext2 currentContext, string name)
        {
        }

        internal virtual void Count()
        {

        }
    }

    internal class OutputVariable
    {
        public GremlinVariable2 GremlinVariable { get; private set; }
        public string VariableProperty { get; private set; }

        public OutputVariable(GremlinVariable2 gremlinVariable, string variableProperty)
        {
            GremlinVariable = gremlinVariable;
            VariableProperty = variableProperty;
        }
    }

    
    internal class GremlinVertexVariable2 : GremlinVariable2
    {
        private static long _count = 0;

        public static string GetVariableName()
        {
            return "N_" + _count++;
        }

        public GremlinVertexVariable2()
        {
            VariableName = GetVariableName();
        }


    }

    internal abstract class GremlinCoalesceVariable : GremlinVariable2
    {
        protected GremlinToSqlContext2 traversal1;
        protected GremlinToSqlContext2 traversal2;

        public GremlinCoalesceVariable(GremlinToSqlContext2 traversal1, GremlinToSqlContext2 traversal2)
        {
            this.traversal1 = traversal1;
            this.traversal2 = traversal2;
        }

        internal override void Populate(string name)
        {
        }
    }

    internal class GremlinCoalesceVertexVariable : GremlinCoalesceVariable
    {
        public GremlinCoalesceVertexVariable(GremlinToSqlContext2 traversal1, GremlinToSqlContext2 traversal2)
            : base (traversal1, traversal2) { }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Vertex;
        }
    }

    internal class GremlinCoalesceEdgeVariable : GremlinCoalesceVariable
    {
        public GremlinCoalesceEdgeVariable(GremlinToSqlContext2 traversal1, GremlinToSqlContext2 traversal2)
            : base (traversal1, traversal2) { }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Edge;
        }
    }

    internal class GremlinCoalesceTableVariable : GremlinCoalesceVariable
    {
        public GremlinCoalesceTableVariable(GremlinToSqlContext2 traversal1, GremlinToSqlContext2 traversal2)
            : base (traversal1, traversal2) { }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Table;
        }
    }

    internal class GremlinCoalesceValueVariable : GremlinCoalesceVariable
    {
        public GremlinCoalesceValueVariable(GremlinToSqlContext2 traversal1, GremlinToSqlContext2 traversal2)
            : base (traversal1, traversal2) { }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Scalar;
        }
    }

    internal class GremlinGroupVariable : GremlinVariable2
    {
        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Table;
        }

        internal override void By(GremlinToSqlContext2 currentContext, GremlinToSqlContext2 byContext)
        {

        }
    }

    internal class GremlinCountVariable : GremlinVariable2
    {
        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Scalar;
        }
    }
}
