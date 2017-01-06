using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal abstract class GremlinAggregationVariable : GremlinScalarVariable
    {
        public GremlinScalarVariable AggregateProjection { get; set; }

        public GremlinAggregationVariable(GremlinScalarVariable aggregateProjection)
        {
            AggregateProjection = aggregateProjection;
        }
    }

    internal class GremlinCountVariable : GremlinDerivedTableVariable
    {
        public GremlinCountVariable(GremlinToSqlContext subqueryContext) : base(subqueryContext)
        {
            VariableName = GenerateTableAlias();
        }

        internal override GremlinScalarVariable DefaultProjection()
        {
            return new GremlinVariableProperty(this, "_value");
        }

        public override WTableReference ToTableReference()
        {
            WSelectQueryBlock queryBlock = SubqueryContext.ToSelectQueryBlock();
            queryBlock.SelectElements.Clear();
            queryBlock.SelectElements.Add(SqlUtil.GetSelectFunctionCall("count", SqlUtil.GetStarColumnReferenceExpr()));
            return SqlUtil.GetDerivedTable(queryBlock, VariableName);
        }
    }

    internal class GremlinFoldVariable : GremlinDerivedTableVariable
    {
        public GremlinFoldVariable(GremlinToSqlContext subqueryContext) : base(subqueryContext)
        {
            VariableName = GenerateTableAlias();
        }

        internal override GremlinScalarVariable DefaultProjection()
        {
            return new GremlinVariableProperty(this, "_value");
        }

        internal override void Unfold(GremlinToSqlContext currentContext)
        {
            GremlinUnfoldVariable newVariable = new GremlinUnfoldVariable(new GremlinVariableProperty(this, "_value"));
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.PivotVariable = newVariable;
        }

        public override WTableReference ToTableReference()
        {
            WSelectQueryBlock queryBlock = SubqueryContext.ToSelectQueryBlock();
            queryBlock.SelectElements.Clear();
            queryBlock.SelectElements.Add( SqlUtil.GetSelectFunctionCall("fold", SubqueryContext.PivotVariable.DefaultProjection().ToScalarExpression()));
            return SqlUtil.GetDerivedTable(queryBlock, VariableName);
        }
    }

    internal class GremlinTreeVariable : GremlinScalarVariable
    {
        public override WScalarExpression ToScalarExpression()
        {
            return SqlUtil.GetFunctionCall("tree");
        }
    }

    internal class GremlinUnfoldVariable : GremlinTableVariable
    {
        public GremlinVariableProperty ProjectVariable { get; set; }

        public GremlinUnfoldVariable(GremlinVariableProperty propertyVariable)
        {
            ProjectVariable = propertyVariable;
            VariableName = GenerateTableAlias();
        }

        internal override GremlinScalarVariable DefaultProjection()
        {
            return new GremlinVariableProperty(this, "_value");
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(ProjectVariable.ToScalarExpression());
            var secondTableRef = SqlUtil.GetFunctionTableReference("unfold", parameters, VariableName);
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }

    internal class GremlinDedupVariable : GremlinDerivedTableVariable
    {
        public List<string> DedupLabels { get; set; }

        public GremlinDedupVariable(GremlinToSqlContext subqueryContext, List<string> dedupLabels)
            : base(subqueryContext)
        {
            DedupLabels = new List<string>(dedupLabels);
        }

        internal override GremlinScalarVariable DefaultProjection()
        {
            return SubqueryContext.PivotVariable.DefaultProjection();
        }

        public override WTableReference ToTableReference()
        {
            GremlinToSqlContext dedupContext = new GremlinToSqlContext();
            GremlinDerivedTableVariable subqueryVariable = new GremlinDerivedTableVariable(SubqueryContext);
            dedupContext.VariableList.Add(subqueryVariable);
            dedupContext.TableReferences.Add(subqueryVariable);
            dedupContext.PivotVariable = subqueryVariable;

            WSelectQueryBlock queryBlock = dedupContext.ToSelectQueryBlock();
            queryBlock.SelectElements.Clear();
            queryBlock.SelectElements.Add(SqlUtil.GetSelectFunctionCall("dedup", SubqueryContext.PivotVariable.DefaultProjection().ToScalarExpression()));

            return SqlUtil.GetDerivedTable(queryBlock, VariableName);
        }
    }


}
