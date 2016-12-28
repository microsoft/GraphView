using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal interface ISqlTable
    {
    }

    internal interface ISqlScalar { }

    internal interface ISqlBoolean { }

    internal enum GremlinVariableType
    {
        Vertex,
        Edge,
        Table,
        Scalar,
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

        internal virtual GremlinScalarVariable DefaultProjection()
        {
            throw new NotImplementedException();
        }

        internal virtual void And(
            GremlinToSqlContext2 currentContext, 
            GremlinToSqlContext2 subContext1,
            GremlinToSqlContext2 subContext2)
        {
        }

        internal virtual void As(GremlinToSqlContext2 currentContext, string name)
        {
            currentContext.TaggedVariables[name] = this;
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

        internal virtual void Cap(GremlinToSqlContext2 currentContext, params string[] keys)
        {
            currentContext.ProjectedVariables.Clear();

            foreach (string key in keys)
            {
                if (!currentContext.TaggedVariables.ContainsKey(key))
                {
                    throw new QueryCompilationException(string.Format("The specified tag \"{0}\" is not defined.", key));
                }

                GremlinVariable2 var = currentContext.TaggedVariables[key];
                currentContext.ProjectedVariables.Add(var.DefaultProjection());
            }
        }

        internal virtual void Coalesce(
            GremlinToSqlContext2 currentContext,
            GremlinToSqlContext2 traversal1,
            GremlinToSqlContext2 traversal2)
        {
            GremlinVariableType type1 = traversal1.PivotVariable.GetVariableType();
            GremlinVariableType type2 = traversal2.PivotVariable.GetVariableType();

            if (type1 == type2)
            {
                switch (type1)
                {
                    case GremlinVariableType.Vertex:
                        GremlinCoalesceVertexVariable vertexVariable = new GremlinCoalesceVertexVariable(traversal1, traversal2);
                        currentContext.VariableList.Add(vertexVariable);
                        currentContext.TableReferences.Add(vertexVariable);
                        currentContext.PivotVariable = vertexVariable;
                        break;
                    case GremlinVariableType.Edge:
                        GremlinCoalesceEdgeVariable edgeVariable = new GremlinCoalesceEdgeVariable(traversal1, traversal2);
                        currentContext.VariableList.Add(edgeVariable);
                        currentContext.TableReferences.Add(edgeVariable);
                        currentContext.PivotVariable = edgeVariable;
                        break;
                    case GremlinVariableType.Table:
                        GremlinCoalesceTableVariable tabledValue = new GremlinCoalesceTableVariable(traversal1, traversal2);
                        currentContext.VariableList.Add(tabledValue);
                        currentContext.TableReferences.Add(tabledValue);
                        currentContext.PivotVariable = tabledValue;
                        break;
                    case GremlinVariableType.Scalar:
                        currentContext.PivotVariable = new GremlinCoalesceValueVariable(traversal1, traversal2);
                        break;
                    default:
                        break;
                }
            }
            else
            {
                GremlinCoalesceTableVariable tabledValue = new GremlinCoalesceTableVariable(traversal1, traversal2);
                currentContext.VariableList.Add(tabledValue);
                currentContext.TableReferences.Add(tabledValue);
                currentContext.PivotVariable = tabledValue;
            }
        }

        internal virtual void Count()
        {

        }

        internal virtual void Group(GremlinToSqlContext2 currentContext)
        {
            GremlinGroupVariable groupVariable = new GremlinGroupVariable();
            currentContext.VariableList.Add(groupVariable);
        }

        internal virtual void Inject(GremlinToSqlContext2 currentContext, params string[] values)
        {
            if (currentContext.VariableList.Count == 0)
            {
                GremlinInjectVariable injectVar = new GremlinInjectVariable(null, values);
                currentContext.VariableList.Add(injectVar);
                currentContext.PivotVariable = injectVar;
            } 
            else
            {
                GremlinToSqlContext2 priorContext = currentContext.Duplicate();
                currentContext.Reset();
                GremlinInjectVariable injectVar = new GremlinInjectVariable(priorContext, values);
                currentContext.VariableList.Add(injectVar);
                currentContext.PivotVariable = injectVar;
            }
        }
    }

    
    internal abstract class GremlinScalarVariable : GremlinVariable2, ISqlScalar
    {
        internal override GremlinScalarVariable DefaultProjection()
        {
            return this;
        }
    }

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

    internal abstract class GremlinTableVariable : GremlinVariable2
    {
        protected static int _count = 0;

        public string GenerateTableAlias()
        {
            return "R_" + _count++;
        }
    }
    
    internal class GremlinFreeVertexVariable : GremlinTableVariable, ISqlTable
    {
        public GremlinFreeVertexVariable()
        {
            VariableName = GenerateTableAlias();
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
    internal class GremlinBoundVertexVariable : GremlinTableVariable, ISqlTable
    {
        private GremlinVariableProperty adjacencyList;
        private List<string> projectedProperties;

        public GremlinBoundVertexVariable(GremlinVariableProperty adjacencyList)
        {
            VariableName = GenerateTableAlias();
            this.adjacencyList = adjacencyList;
            projectedProperties = new List<string>();
        }

        internal override GremlinScalarVariable DefaultProjection()
        {
            return new GremlinVariableProperty(this, "id");
        }

        internal override void Populate(string name)
        {
            projectedProperties.Add(name);
        }

        internal override void Both(GremlinToSqlContext2 currentContext)
        {
            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(this, "BothAdjacencyList");
            GremlinBoundVertexVariable boundVertex = new GremlinBoundVertexVariable(adjacencyList);
            currentContext.VariableList.Add(boundVertex);
            currentContext.PivotVariable = boundVertex;
        }
    }

    /// <summary>
    /// The abstract variable for coalesce and union 
    /// </summary>
    internal abstract class GremlinBinaryVariable : GremlinTableVariable
    {
        protected GremlinToSqlContext2 traversal1;
        protected GremlinToSqlContext2 traversal2;

        public GremlinBinaryVariable(GremlinToSqlContext2 traversal1, GremlinToSqlContext2 traversal2)
        {
            this.traversal1 = traversal1;
            this.traversal2 = traversal2;
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
            : base (traversal1, traversal2) { }

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
            : base (traversal1, traversal2) { }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Edge;
        }
    }

    internal class GremlinCoalesceTableVariable : GremlinCoalesceVariable, ISqlTable
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

    internal abstract class GremlinAggregationVariable : GremlinVariable2, ISqlScalar
    {

    }

    internal class GremlinCountVariable : GremlinAggregationVariable
    {
        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Scalar;
        }
    }

    internal class GremlinFoldVariable : GremlinAggregationVariable
    {

    }

    internal class GremlinUnfoldVariable : GremlinTableVariable, ISqlTable
    {

    }

    /// <summary>
    /// Inject variable will be translated to a derived table reference
    /// in the SQL FROM clause, concatenating results from priorContext and injected values. 
    /// </summary>
    internal class GremlinInjectVariable : GremlinTableVariable, ISqlTable
    {
        List<string> rows;
        // When priorContext is null, the corresponding table reference only contains injected values. 
        GremlinToSqlContext2 priorContext;

        public GremlinInjectVariable(GremlinToSqlContext2 priorContext, params string[] values)
        {
            this.priorContext = priorContext;
            rows = new List<string>(values);
        }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Table;
        }

        internal override GremlinScalarVariable DefaultProjection()
        {
            // When priorContext is not null, the output table has one column,
            // and the column name is determined by priorContext.
            if (priorContext != null)
            {
                return priorContext.PivotVariable.DefaultProjection();
            }
            else
            {
                VariableName = GenerateTableAlias();
                return new GremlinVariableProperty(this, "_value");
            }
        }

        internal override void Populate(string name)
        {
            if (priorContext != null)
            {
                priorContext.Populate(name);
            }
        }

        internal override void Inject(GremlinToSqlContext2 currentContext, params string[] values)
        {
            rows.AddRange(values);
        }
    }

    internal class GremlinOptionalVariable : GremlinVariable2, ISqlTable
    {

    }
}
