using System;
using System.Collections.Generic;
using System.Deployment.Internal;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinDerivedTableVariable: GremlinTableVariable
    {
        public GremlinToSqlContext SubqueryContext { get; set; }

        public GremlinDerivedTableVariable(GremlinToSqlContext subqueryContext)
        {
            SubqueryContext = subqueryContext;
        }

        internal override void Populate(string property)
        {
            if (!ProjectedProperties.Contains(property))
            {
                ProjectedProperties.Add(property);
            }
            SubqueryContext.Populate(property);
        }

        public override WTableReference ToTableReference()
        {
            return SqlUtil.GetDerivedTable(SubqueryContext.ToSelectQueryBlock(ProjectedProperties), VariableName);
        }
    }

    internal class GremlinFoldVariable : GremlinDerivedTableVariable
    {
        public GremlinFoldVariable(GremlinToSqlContext subqueryContext) : base(subqueryContext) {}

        internal override GremlinScalarVariable DefaultProjection()
        {
            return new GremlinVariableProperty(this, GremlinKeyword.TableValue);
        }

        public override WTableReference ToTableReference()
        {
            WSelectQueryBlock queryBlock = SubqueryContext.ToSelectQueryBlock();
            queryBlock.SelectElements.Clear();
            queryBlock.SelectElements.Add(SqlUtil.GetSelectFunctionCall(GremlinKeyword.func.Fold, SubqueryContext.PivotVariable.DefaultProjection().ToScalarExpression()));
            return SqlUtil.GetDerivedTable(queryBlock, VariableName);
        }
    }

    internal class GremlinCountVariable : GremlinDerivedTableVariable
    {
        public GremlinCountVariable(GremlinToSqlContext subqueryContext) : base(subqueryContext) {}

        internal override GremlinScalarVariable DefaultProjection()
        {
            return new GremlinVariableProperty(this, GremlinKeyword.TableValue);
        }

        public override WTableReference ToTableReference()
        {
            WSelectQueryBlock queryBlock = SubqueryContext.ToSelectQueryBlock();
            queryBlock.SelectElements.Clear();
            queryBlock.SelectElements.Add(SqlUtil.GetSelectFunctionCall(GremlinKeyword.func.Count, SqlUtil.GetStarColumnReferenceExpr()));
            return SqlUtil.GetDerivedTable(queryBlock, VariableName);
        }
    }
}
