using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

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
        Scalar,
        Table,
        Undefined
    }
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
            currentContext.TaggedVariables[name] = new Tuple<GremlinVariable2, GremlinToSqlContext2>(this, currentContext);
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

                GremlinVariable2 var = currentContext.TaggedVariables[key].Item1;
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

        internal virtual void OutV(GremlinToSqlContext2 currentContext)
        {
            throw new QueryCompilationException("The OutV() step can only be applied to edges.");
        }

        internal virtual void Select(GremlinToSqlContext2 currentContext, string tagName)
        {
            if (!currentContext.TaggedVariables.ContainsKey(tagName))
            {
                throw new QueryCompilationException(string.Format("The specified tag \"{0}\" is not defined.", tagName));
            }

            var pair = currentContext.TaggedVariables[tagName];

            if (pair.Item2 == currentContext)
            {
                currentContext.PivotVariable = pair.Item1;
            }
            else
            {
                if (pair.Item1 is GremlinVertexVariable2)
                {
                    GremlinContextVertexVariable contextVertex = new GremlinContextVertexVariable(pair.Item1 as GremlinVertexVariable2);
                    currentContext.VariableList.Add(contextVertex);
                    currentContext.PivotVariable = contextVertex;
                }
                else if (pair.Item1 is GremlinEdgeVariable2)
                {
                    GremlinContextEdgeVariable contextEdge = new GremlinContextEdgeVariable(pair.Item1 as GremlinContextEdgeVariable);
                    currentContext.VariableList.Add(contextEdge);
                    currentContext.PivotVariable = contextEdge;
                }
            }
        }

    }
}
