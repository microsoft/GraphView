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
            this.SubqueryContext = subqueryContext;
        }

        internal override bool Populate(string property, string label = null)
        {
            if (base.Populate(property, label))
            {
                return this.SubqueryContext.Populate(property, null);
            }
            else if (this.SubqueryContext.Populate(property, label))
            {
                return base.Populate(property, null);
            }
            else
            {
                return false;
            }
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(this.SubqueryContext.FetchAllVars());
            return variableList;
        }

        internal override List<GremlinTableVariable> FetchAllTableVars()
        {
            List<GremlinTableVariable> variableList = new List<GremlinTableVariable> { this };
            variableList.AddRange(this.SubqueryContext.FetchAllTableVars());
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            return SqlUtil.GetDerivedTable(this.SubqueryContext.ToSelectQueryBlock(), GetVariableName());
        }
    }

    internal class GremlinFoldVariable : GremlinDerivedTableVariable
    {
        public GremlinVariable FoldVariable { get; set; }

        public GremlinFoldVariable(GremlinToSqlContext subqueryContext) : base(subqueryContext)
        {
            FoldVariable = subqueryContext.PivotVariable;
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.Add(FoldVariable);
            variableList.AddRange(this.SubqueryContext.FetchAllVars());
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            WSelectQueryBlock queryBlock = this.SubqueryContext.ToSelectQueryBlock();
            queryBlock.SelectElements.Clear();
            List<WScalarExpression> foldParameters = new List<WScalarExpression> { this.SubqueryContext.PivotVariable.ToCompose1() };
            queryBlock.SelectElements.Add(
                SqlUtil.GetSelectScalarExpr(SqlUtil.GetFunctionCall(GremlinKeyword.func.Fold, foldParameters),
                    GremlinKeyword.TableDefaultColumnName));
            return SqlUtil.GetDerivedTable(queryBlock, GetVariableName());
        }
    }

    internal class GremlinCountVariable : GremlinDerivedTableVariable
    {
        public GremlinCountVariable(GremlinToSqlContext subqueryContext) : base(subqueryContext) {}

        public override WTableReference ToTableReference()
        {
            WSelectQueryBlock queryBlock = this.SubqueryContext.ToSelectQueryBlock();
            queryBlock.SelectElements.Clear();
            queryBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetFunctionCall(GremlinKeyword.func.Count), GremlinKeyword.TableDefaultColumnName));
            return SqlUtil.GetDerivedTable(queryBlock, GetVariableName());
        }
    }

    internal class GremlinMinVariable : GremlinDerivedTableVariable
    {
        public GremlinMinVariable(GremlinToSqlContext subqueryContext) : base(subqueryContext) {}

        public override WTableReference ToTableReference()
        {
            WSelectQueryBlock queryBlock = this.SubqueryContext.ToSelectQueryBlock();
            queryBlock.SelectElements.Clear();
            queryBlock.SelectElements.Add(
                SqlUtil.GetSelectScalarExpr(
                    SqlUtil.GetFunctionCall(GremlinKeyword.func.Min,
                        this.SubqueryContext.PivotVariable.DefaultProjection().ToScalarExpression()),
                    GremlinKeyword.TableDefaultColumnName));
            return SqlUtil.GetDerivedTable(queryBlock, GetVariableName());
        }
    }

    internal class GremlinMaxVariable : GremlinDerivedTableVariable
    {
        public GremlinMaxVariable(GremlinToSqlContext subqueryContext) : base(subqueryContext) {}

        public override WTableReference ToTableReference()
        {
            WSelectQueryBlock queryBlock = this.SubqueryContext.ToSelectQueryBlock();
            queryBlock.SelectElements.Clear();
            queryBlock.SelectElements.Add(
                SqlUtil.GetSelectScalarExpr(
                    SqlUtil.GetFunctionCall(GremlinKeyword.func.Max,
                        this.SubqueryContext.PivotVariable.DefaultProjection().ToScalarExpression()),
                    GremlinKeyword.TableDefaultColumnName));
            return SqlUtil.GetDerivedTable(queryBlock, GetVariableName());
        }
    }

    internal class GremlinMeanVariable : GremlinDerivedTableVariable
    {
        public GremlinMeanVariable(GremlinToSqlContext subqueryContext) : base(subqueryContext) {}

        public override WTableReference ToTableReference()
        {
            WSelectQueryBlock queryBlock = this.SubqueryContext.ToSelectQueryBlock();
            queryBlock.SelectElements.Clear();
            queryBlock.SelectElements.Add(
                SqlUtil.GetSelectScalarExpr(
                    SqlUtil.GetFunctionCall(GremlinKeyword.func.Mean,
                        this.SubqueryContext.PivotVariable.DefaultProjection().ToScalarExpression()),
                    GremlinKeyword.TableDefaultColumnName));
            return SqlUtil.GetDerivedTable(queryBlock, GetVariableName());
        }
    }

    internal class GremlinSumVariable : GremlinDerivedTableVariable
    {
        public GremlinSumVariable(GremlinToSqlContext subqueryContext) : base(subqueryContext) {}

        public override WTableReference ToTableReference()
        {
            WSelectQueryBlock queryBlock = this.SubqueryContext.ToSelectQueryBlock();
            queryBlock.SelectElements.Clear();
            queryBlock.SelectElements.Add(
                SqlUtil.GetSelectScalarExpr(
                    SqlUtil.GetFunctionCall(GremlinKeyword.func.Sum,
                        this.SubqueryContext.PivotVariable.DefaultProjection().ToScalarExpression()),
                    GremlinKeyword.TableDefaultColumnName));
            return SqlUtil.GetDerivedTable(queryBlock, GetVariableName());
        }
    }

    internal class GremlinTreeVariable : GremlinDerivedTableVariable
    {
        public GremlinVariable PathVariable { get; set; }

        public GremlinTreeVariable(GremlinToSqlContext subqueryContext, GremlinVariable pathVariable) : base(subqueryContext)
        {
            PathVariable = pathVariable;
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.Add(PathVariable);
            variableList.AddRange(this.SubqueryContext.FetchAllVars());
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            WSelectQueryBlock queryBlock = this.SubqueryContext.ToSelectQueryBlock();
            queryBlock.SelectElements.Clear();
            queryBlock.SelectElements.Add(
                SqlUtil.GetSelectScalarExpr(
                    SqlUtil.GetFunctionCall(GremlinKeyword.func.Tree,
                        PathVariable.DefaultProjection().ToScalarExpression()), GremlinKeyword.TableDefaultColumnName));
            return SqlUtil.GetDerivedTable(queryBlock, GetVariableName());
        }
    }

    internal class GremlinCapVariable : GremlinDerivedTableVariable
    {
        public List<string> SideEffectKeys { get; set; }

        public List<GremlinVariable> SideEffectVariables { get; set; } // such as aggregate("a"), sotre("a"), as("a")

        public GremlinCapVariable(GremlinToSqlContext subqueryContext, List<GremlinVariable> sideEffectVariables, List<string> sideEffectKeys)
            : base(subqueryContext)
        {
            SideEffectKeys = sideEffectKeys;
            SideEffectVariables = sideEffectVariables;
        }

        internal override bool Populate(string property, string label = null)
        {
            bool populateSuccess = false;
            foreach (var sideEffectVariable in SideEffectVariables)
            {
                populateSuccess |= sideEffectVariable.Populate(property, label);
            }

            //if (SideEffectKeys.Count > 1 && property != GremlinKeyword.TableDefaultColumnName)
            //{
            //    throw new TranslationException("Multiple variables can only populate TableDefaultColumnName");
            //}

            if (populateSuccess)
            {
                base.Populate(property, null);
            }
            return populateSuccess;
        }

        public override WTableReference ToTableReference()
        {
            WSelectQueryBlock queryBlock = this.SubqueryContext.ToSelectQueryBlock();
            queryBlock.SelectElements.Clear();
            
            List<WValueExpression> columnListExpr = new List<WValueExpression>();

            columnListExpr.Add(SqlUtil.GetValueExpr(this.SubqueryContext.PivotVariable.DefaultProperty()));
            columnListExpr.AddRange(this.ProjectedProperties.Select(SqlUtil.GetValueExpr));
            
            List<WScalarExpression> capParameters = new List<WScalarExpression>();
            foreach (var sideEffectKey in SideEffectKeys)
            {
                capParameters.Add(new WColumnNameList(columnListExpr));
                capParameters.Add(SqlUtil.GetValueExpr(sideEffectKey));
            }

            queryBlock.SelectElements.Add(
                SqlUtil.GetSelectScalarExpr(SqlUtil.GetFunctionCall(GremlinKeyword.func.Cap, capParameters),
                    GremlinKeyword.TableDefaultColumnName));

            return SqlUtil.GetDerivedTable(queryBlock, GetVariableName());
        }
    }
}
