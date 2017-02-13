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
            ProjectedProperties.Add(GremlinKeyword.ScalarValue);
        }

        internal override void Populate(string property)
        {
            if (ProjectedProperties.Contains(property)) return;
            base.Populate(property);

            SubqueryContext.Populate(property);
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

        public override WTableReference ToTableReference()
        {
            WSelectQueryBlock queryBlock = SubqueryContext.ToSelectQueryBlock();
            queryBlock.SelectElements.Clear();
            List<WScalarExpression> compose1Parameters = new List<WScalarExpression>();
            //TODO
            //SubqueryContext.PivotVariable.Populate(SubqueryContext.PivotVariable.DefaultVariableProperty().VariableProperty);
            foreach (var projectProperty in SubqueryContext.PivotVariable.ProjectedProperties)
            {
                compose1Parameters.Add(FoldVariable.GetVariableProperty(projectProperty).ToScalarExpression());
                compose1Parameters.Add(SqlUtil.GetValueExpr(projectProperty));
            }
            WFunctionCall compose1 = SqlUtil.GetFunctionCall(GremlinKeyword.func.Compose1, compose1Parameters);

            List<WScalarExpression> foldParameters = new List<WScalarExpression> { compose1 };
            queryBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetFunctionCall(GremlinKeyword.func.Fold, foldParameters), GremlinKeyword.ScalarValue));
            return SqlUtil.GetDerivedTable(queryBlock, GetVariableName());
        }

        internal override void Unfold(GremlinToSqlContext currentContext)
        {
            GremlinTableVariable newVariable = GremlinUnfoldVariable.Create(this);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }
    }

    internal class GremlinCountVariable : GremlinDerivedTableVariable
    {
        public GremlinCountVariable(GremlinToSqlContext subqueryContext) : base(subqueryContext) {}

        public override WTableReference ToTableReference()
        {
            WSelectQueryBlock queryBlock = SubqueryContext.ToSelectQueryBlock();
            queryBlock.SelectElements.Clear();
            queryBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetFunctionCall(GremlinKeyword.func.Count), GremlinKeyword.ScalarValue));
            return SqlUtil.GetDerivedTable(queryBlock, GetVariableName());
        }
    }

    internal class GremlinTreeVariable : GremlinDerivedTableVariable
    {
        private GremlinVariableProperty pathVariableProperty;

        public GremlinTreeVariable(GremlinToSqlContext subqueryContext, GremlinVariableProperty pathVariableProperty)
            : base(subqueryContext)
        {
            this.pathVariableProperty = pathVariableProperty;
        }

        public override WTableReference ToTableReference()
        {
            WSelectQueryBlock queryBlock = SubqueryContext.ToSelectQueryBlock();
            queryBlock.SelectElements.Clear();
            queryBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetFunctionCall(GremlinKeyword.func.Tree, pathVariableProperty.ToScalarExpression()), GremlinKeyword.ScalarValue));
            return SqlUtil.GetDerivedTable(queryBlock, GetVariableName());
        }
    }
}
