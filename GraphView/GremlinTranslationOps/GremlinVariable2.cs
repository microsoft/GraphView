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

        internal virtual void Both(GremlinToSqlContext2 currentContext)
        {
            throw new QueryCompilationException("The Both() step only applies to vertices.");
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

    internal abstract class GremlinScalarVariable : GremlinVariable2 { }

    internal class GremlinVariableProperty : GremlinScalarVariable
    {
        public GremlinVariable2 GremlinVariable { get; private set; }
        public string VariableProperty { get; private set; }

        public GremlinVariableProperty(GremlinVariable2 gremlinVariable, string variableProperty)
        {
            GremlinVariable = gremlinVariable;
            VariableProperty = variableProperty;
        }
    }

    internal class GremlinScalarSubquery : GremlinScalarVariable
    {
        public GremlinToSqlContext2 SubqueryContext { get; private set; }

        public GremlinScalarSubquery(GremlinToSqlContext2 subqueryContext)
        {
            SubqueryContext = SubqueryContext;
        }
    }
    
    internal class GremlinFreeVertexVariable : GremlinVariable2
    {
        private static long _count = 0;

        public static string GetVariableName()
        {
            return "N_" + _count++;
        }

        public GremlinFreeVertexVariable()
        {
            VariableName = GetVariableName();
        }

        internal override void Both(GremlinToSqlContext2 currentContext)
        {
            GremlinFreeVertexVariable bothVertex = new GremlinFreeVertexVariable();
            currentContext.VariableList.Add(bothVertex);
            currentContext.PivotVariable = bothVertex;
            // Also populates a path this_variable-->bothVertex in the context
        }
    }

    /// <summary>
    /// A free vertex variable is translated to a node table reference in 
    /// the FROM clause, whereas a bound vertex variable is translated into
    /// a table-valued function. 
    /// </summary>
    internal class GremlinBoundVertexVariable : GremlinVariable2
    {
        private static long _count = 0;
        private GremlinVariableProperty adjacencyList;

        public static string GetVariableName()
        {
            return "BN_" + _count++;
        }

        public GremlinBoundVertexVariable(GremlinVariableProperty adjacencyList)
        {
            VariableName = GetVariableName();
            this.adjacencyList = adjacencyList;
        }

        internal override void Both(GremlinToSqlContext2 currentContext)
        {
            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(this, "BothAdjacencyList");
            GremlinBoundVertexVariable boundVertex = new GremlinBoundVertexVariable(adjacencyList);
            currentContext.VariableList.Add(boundVertex);
            currentContext.PivotVariable = boundVertex;
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

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Table;
        }

        internal override void Populate(string name)
        {
            traversal1.Populate(name);
            traversal2.Populate(name);
        }

        internal override void Both(GremlinToSqlContext2 currentContext)
        {
            if (traversal1.PivotVariable.GetVariableType() != GremlinVariableType.Vertex &&
                traversal2.PivotVariable.GetVariableType() != GremlinVariableType.Vertex)
            {
                // If neither output of the coalesce variable is of type vertex, 
                // this coalesce variable cannot be followed by the Both() step.
                base.Both(currentContext);
            }
            else
            {
                if (traversal1.PivotVariable.GetVariableType() == GremlinVariableType.Vertex)
                {
                    traversal1.Populate("BothAdjacencyList");
                }

                if (traversal2.PivotVariable.GetVariableType() == GremlinVariableType.Vertex)
                {
                    traversal2.Populate("BothAdjacencyList");
                }

                // If one output of the coalesce variable is of type vertex,
                // the following both() is formulated as a bound vertex variable.
                GremlinVariableProperty adjacencyList = new GremlinVariableProperty(this, "BothAdjacencyList");
                GremlinBoundVertexVariable boundVertex = new GremlinBoundVertexVariable(adjacencyList);
                currentContext.PivotVariable = boundVertex;
                currentContext.VariableList.Add(boundVertex);
            }
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

        internal override void Both(GremlinToSqlContext2 currentContext)
        {
            base.Both(currentContext);
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
        public GremlinScalarVariable GroupbyKey { get; private set; }
        public GremlinScalarVariable AggregateValue { get; private set; }

        // To re-consider
        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Table;
        }

        internal override void By(GremlinToSqlContext2 currentContext, GremlinToSqlContext2 byContext)
        {
            // The BY step first sets the group-by key, and then sets the aggregation value.
            if (GroupbyKey == null)
            {
                GroupbyKey = new GremlinScalarSubquery(byContext);
            }
            else if (AggregateValue != null)
            {
                AggregateValue = new GremlinScalarSubquery(byContext);
            }
        }

        internal override void By(GremlinToSqlContext2 currentContext, string name)
        {
            if (GroupbyKey == null)
            {
                currentContext.PivotVariable.Populate(name);
                GroupbyKey = new GremlinVariableProperty(currentContext.PivotVariable, name);
            }
            else if (AggregateValue != null)
            {
                currentContext.PivotVariable.Populate(name);
                AggregateValue = new GremlinVariableProperty(currentContext.PivotVariable, name);
            }
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
