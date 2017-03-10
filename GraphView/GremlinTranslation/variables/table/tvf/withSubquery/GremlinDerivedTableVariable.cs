using System;
using System.Collections.Generic;
using System.Deployment.Internal;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinDerivedTableVariable: GremlinScalarTableVariable
    {
        public GremlinToSqlContext SubqueryContext { get; set; }

        public GremlinDerivedTableVariable(GremlinToSqlContext subqueryContext)
        {
            SubqueryContext = subqueryContext;
        }

        internal override void Populate(string property)
        {
            if (ProjectedProperties.Contains(property)) return;
            base.Populate(property);

            SubqueryContext.Populate(property);
        }

        internal override List<GremlinVariable> FetchVarsFromCurrAndChildContext()
        {
            return SubqueryContext == null ? new List<GremlinVariable>() : SubqueryContext.FetchVarsFromCurrAndChildContext();
        }

        public override WTableReference ToTableReference()
        {
            return SqlUtil.GetDerivedTable(SubqueryContext.ToSelectQueryBlock(ProjectedProperties), GetVariableName());
        }
    }

    internal class GremlinFoldVariable : GremlinDerivedTableVariable
    {
        public GremlinVariable FoldVariable { get; set; }

        public GremlinFoldVariable(GremlinToSqlContext subqueryContext) : base(subqueryContext)
        {
            FoldVariable = subqueryContext.PivotVariable;
        }

        internal override GremlinVariableType GetUnfoldVariableType()
        {
            return FoldVariable.GetVariableType();
        }

        public override WTableReference ToTableReference()
        {
            WSelectQueryBlock queryBlock = SubqueryContext.ToSelectQueryBlock();
            queryBlock.SelectElements.Clear();
            //if (SubqueryContext.PivotVariable.ProjectedProperties.Count == 0)
            //{
            //    SubqueryContext.PivotVariable.ProjectedProperties.Add(GetProjectKey());
            //}

            List<WScalarExpression> foldParameters = new List<WScalarExpression> { SubqueryContext.PivotVariable.ToCompose1() };
            queryBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetFunctionCall(GremlinKeyword.func.Fold, foldParameters), GremlinKeyword.TableDefaultColumnName));
            return SqlUtil.GetDerivedTable(queryBlock, GetVariableName());
        }
    }

    internal class GremlinCountVariable : GremlinDerivedTableVariable
    {
        public GremlinCountVariable(GremlinToSqlContext subqueryContext) : base(subqueryContext) {}

        public override WTableReference ToTableReference()
        {
            WSelectQueryBlock queryBlock = SubqueryContext.ToSelectQueryBlock();
            queryBlock.SelectElements.Clear();
            queryBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetFunctionCall(GremlinKeyword.func.Count), GremlinKeyword.TableDefaultColumnName));
            return SqlUtil.GetDerivedTable(queryBlock, GetVariableName());
        }
    }

    internal class GremlinMinVariable : GremlinDerivedTableVariable
    {
        public GremlinMinVariable(GremlinToSqlContext subqueryContext) : base(subqueryContext) { }

        public override WTableReference ToTableReference()
        {
            WSelectQueryBlock queryBlock = SubqueryContext.ToSelectQueryBlock();
            queryBlock.SelectElements.Clear();
            queryBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetFunctionCall(GremlinKeyword.func.Min, SubqueryContext.PivotVariable.DefaultProjection().ToScalarExpression()), GremlinKeyword.TableDefaultColumnName));
            return SqlUtil.GetDerivedTable(queryBlock, GetVariableName());
        }
    }

    internal class GremlinMaxVariable : GremlinDerivedTableVariable
    {
        public GremlinMaxVariable(GremlinToSqlContext subqueryContext) : base(subqueryContext) { }

        public override WTableReference ToTableReference()
        {
            WSelectQueryBlock queryBlock = SubqueryContext.ToSelectQueryBlock();
            queryBlock.SelectElements.Clear();
            queryBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetFunctionCall(GremlinKeyword.func.Max, SubqueryContext.PivotVariable.DefaultProjection().ToScalarExpression()), GremlinKeyword.TableDefaultColumnName));
            return SqlUtil.GetDerivedTable(queryBlock, GetVariableName());
        }
    }

    internal class GremlinMeanVariable : GremlinDerivedTableVariable
    {
        public GremlinMeanVariable(GremlinToSqlContext subqueryContext) : base(subqueryContext) { }

        public override WTableReference ToTableReference()
        {
            WSelectQueryBlock queryBlock = SubqueryContext.ToSelectQueryBlock();
            queryBlock.SelectElements.Clear();
            queryBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetFunctionCall(GremlinKeyword.func.Mean, SubqueryContext.PivotVariable.DefaultProjection().ToScalarExpression()), GremlinKeyword.TableDefaultColumnName));
            return SqlUtil.GetDerivedTable(queryBlock, GetVariableName());
        }
    }

    internal class GremlinSumVariable : GremlinDerivedTableVariable
    {
        public GremlinSumVariable(GremlinToSqlContext subqueryContext) : base(subqueryContext) { }

        public override WTableReference ToTableReference()
        {
            WSelectQueryBlock queryBlock = SubqueryContext.ToSelectQueryBlock();
            queryBlock.SelectElements.Clear();
            queryBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetFunctionCall(GremlinKeyword.func.Sum, SubqueryContext.PivotVariable.DefaultProjection().ToScalarExpression()), GremlinKeyword.TableDefaultColumnName));
            return SqlUtil.GetDerivedTable(queryBlock, GetVariableName());
        }
    }

    internal class GremlinTreeVariable : GremlinDerivedTableVariable
    {
        public GremlinVariable PathVariable { get; set; }

        public GremlinTreeVariable(GremlinToSqlContext subqueryContext, GremlinVariable pathVariable)
            : base(subqueryContext)
        {
            PathVariable = pathVariable;
            subqueryContext.HomeVariable = this;
        }

        public override WTableReference ToTableReference()
        {
            WSelectQueryBlock queryBlock = SubqueryContext.ToSelectQueryBlock();
            queryBlock.SelectElements.Clear();
            queryBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetFunctionCall(GremlinKeyword.func.Tree, PathVariable.DefaultVariableProperty().ToScalarExpression()), GremlinKeyword.TableDefaultColumnName));
            return SqlUtil.GetDerivedTable(queryBlock, GetVariableName());
        }
    }

    internal class GremlinCapVariable : GremlinDerivedTableVariable
    {
        public List<string> SideEffectKeys { get; set; }

        public GremlinCapVariable(GremlinToSqlContext subqueryContext, List<string> sideEffectKeys)
            : base(subqueryContext)
        {
            SideEffectKeys = sideEffectKeys;
        }

        public override WTableReference ToTableReference()
        {
            WSelectQueryBlock queryBlock = SubqueryContext.ToSelectQueryBlock();
            queryBlock.SelectElements.Clear();
            List<WValueExpression> columnListExpr = new List<WValueExpression>();
            foreach (var projectProperty in ProjectedProperties)
            {
                columnListExpr.Add(SqlUtil.GetValueExpr(projectProperty));
            }
            List<WScalarExpression> capParameters = new List<WScalarExpression>();
            foreach (var sideEffectKey in SideEffectKeys)
            {
                capParameters.Add(new WColumnNameList(columnListExpr));
                capParameters.Add(SqlUtil.GetValueExpr(sideEffectKey));
            }

            queryBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetFunctionCall(GremlinKeyword.func.Cap, capParameters), GremlinKeyword.TableDefaultColumnName));
            return SqlUtil.GetDerivedTable(queryBlock, GetVariableName());
        }
    }
}
