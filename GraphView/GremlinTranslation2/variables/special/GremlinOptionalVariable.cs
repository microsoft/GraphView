using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinOptionalVariable : GremlinTableVariable, ISqlTable
    {
        private GremlinToSqlContext context;

        public GremlinOptionalVariable(GremlinToSqlContext context)
        {
            this.context = context;
            VariableName = GenerateTableAlias();
        }

        public WTableReference ToTableReference()
        {
            List<WScalarExpression> PropertyKeys = new List<WScalarExpression>();
            WSelectQueryBlock queryBlock = context.ToSelectQueryBlock();
            foreach (var projectProperty in projectedProperties)
            {
                queryBlock.SelectElements.Add(new WSelectScalarExpression()
                {
                    SelectExpr = GremlinUtil.GetColumnReferenceExpression(context.PivotVariable.VariableName, projectProperty)
                });
            }
            PropertyKeys.Add(GremlinUtil.GetScalarSubquery(queryBlock));
            var secondTableRef = GremlinUtil.GetSchemaObjectFunctionTableReference("optional", PropertyKeys);
            secondTableRef.Alias = GremlinUtil.GetIdentifier(VariableName);
            return GremlinUtil.GetCrossApplyTableReference(null, secondTableRef);
        }

        internal override void Populate(string property)
        {
            context.Populate(property);
            base.Populate(property);
        }
    }
    internal class GremlinOptionalVertexVariable : GremlinOptionalVariable
    {
        public GremlinOptionalVertexVariable(GremlinToSqlContext context): base(context) {}

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Vertex;
        }
    }

    internal class GremlinOptionalEdgeVariable : GremlinOptionalVariable
    {
        public GremlinOptionalEdgeVariable(GremlinToSqlContext context) : base(context) { }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Edge;
        }
    }

    internal class GremlinOptionalTableVariable : GremlinOptionalVariable
    {
        public GremlinOptionalTableVariable(GremlinToSqlContext context) : base(context) { }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Table;
        }
    }

    internal class GremlinOptionalValueVariable : GremlinOptionalVariable
    {
        public GremlinOptionalValueVariable(GremlinToSqlContext context) : base(context) { }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Scalar;
        }
    }
}
