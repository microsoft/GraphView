using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal abstract class GremlinCoalesceVariable : GremlinTableVariable
    {
        public List<GremlinToSqlContext> CoalesceContextList;

        public GremlinCoalesceVariable(List<GremlinToSqlContext> coalesceContextList)
        {
            CoalesceContextList = new List<GremlinToSqlContext>(coalesceContextList);
            VariableName = GenerateTableAlias();
        }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Table;
        }

        public override  WTableReference ToTableReference()
        {
            List<WScalarExpression> PropertyKeys = new List<WScalarExpression>();

            foreach (var context in CoalesceContextList)
            {
                WSelectQueryBlock queryBlock = context.ToSelectQueryBlock();
                foreach (var projectProperty in projectedProperties)
                {
                    queryBlock.SelectElements.Add(new WSelectScalarExpression()
                    {
                        SelectExpr =
                            GremlinUtil.GetColumnReferenceExpression(context.PivotVariable.VariableName, projectProperty)
                    });
                }
                PropertyKeys.Add(GremlinUtil.GetScalarSubquery(queryBlock));
            }
            var secondTableRef = GremlinUtil.GetSchemaObjectFunctionTableReference("Coalesce", PropertyKeys);
            secondTableRef.Alias = GremlinUtil.GetIdentifier(VariableName);
            return GremlinUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }

    internal class GremlinCoalesceVertexVariable : GremlinCoalesceVariable
    {
        public GremlinCoalesceVertexVariable(List<GremlinToSqlContext> coalesceContextList)
            : base(coalesceContextList) { }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Vertex;
        }

        internal override void Both(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            base.Both(currentContext, edgeLabels);
        }
    }

    internal class GremlinCoalesceEdgeVariable : GremlinCoalesceVariable
    {
        public GremlinCoalesceEdgeVariable(List<GremlinToSqlContext> coalesceContextList)
            : base(coalesceContextList) { }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Edge;
        }
    }

    internal class GremlinCoalesceTableVariable : GremlinCoalesceVariable, ISqlTable
    {
        public GremlinCoalesceTableVariable(List<GremlinToSqlContext> coalesceContextList)
            : base(coalesceContextList) { }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Table;
        }
    }

    internal class GremlinCoalesceScalarVariable : GremlinCoalesceVariable
    {
        public GremlinCoalesceScalarVariable(List<GremlinToSqlContext> coalesceContextList)
            : base(coalesceContextList) { }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Scalar;
        }
    }
}
