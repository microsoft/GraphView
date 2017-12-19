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

        public GremlinDerivedTableVariable(GremlinToSqlContext subqueryContext, GremlinVariableType variableType) : base(variableType)
        {
            this.SubqueryContext = subqueryContext;
        }

        internal override bool Populate(string property, string label = null)
        {
            bool populateSuccessfully = false;
            if (label == null || this.Labels.Contains(label))
            {
                populateSuccessfully = true;
                this.SubqueryContext.Populate(property, null);
            }
            else if (this.SubqueryContext.Populate(property, label))
            {
                populateSuccessfully = true;
            }
            if (populateSuccessfully && property != null)
            {
                base.Populate(property, null);
            }
            return populateSuccessfully;
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

        public GremlinFoldVariable(GremlinToSqlContext subqueryContext) : base(subqueryContext, GremlinVariableType.List)
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
        public GremlinCountVariable(GremlinToSqlContext subqueryContext) : base(subqueryContext, GremlinVariableType.Scalar) { }

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
        public GremlinMinVariable(GremlinToSqlContext subqueryContext) : base(subqueryContext, GremlinVariableType.Scalar) { }

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
        public GremlinMaxVariable(GremlinToSqlContext subqueryContext) : base(subqueryContext, GremlinVariableType.Scalar) { }

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
        public GremlinMeanVariable(GremlinToSqlContext subqueryContext) : base(subqueryContext, GremlinVariableType.Scalar) { }

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
        public GremlinSumVariable(GremlinToSqlContext subqueryContext) : base(subqueryContext, GremlinVariableType.Scalar) { }

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

        public GremlinTreeVariable(GremlinToSqlContext subqueryContext, GremlinVariable pathVariable) : base(subqueryContext, GremlinVariableType.Tree)
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
            : base(subqueryContext, GremlinVariableType.List)
        {
            SideEffectKeys = sideEffectKeys;
            SideEffectVariables = sideEffectVariables;
        }

        internal override bool Populate(string property, string label = null)
        {
            bool populateSuccessfully = false;
            if (label == null || this.Labels.Contains(label))
            {
                populateSuccessfully = true;
                foreach (GremlinVariable sideEffectVariable in SideEffectVariables)
                {
                    sideEffectVariable.Populate(property, null);
                }
            }
            else
            {
                foreach (GremlinVariable sideEffectVariable in SideEffectVariables)
                {
                    populateSuccessfully |= sideEffectVariable.Populate(property, label);
                }
            }
            if (populateSuccessfully && property != null)
            {
                this.ProjectedProperties.Add(property);
            }
            return populateSuccessfully;
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
