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
        public GremlinVariable FoldVariable { get; set; }

        public GremlinFoldVariable(GremlinToSqlContext subqueryContext) : base(subqueryContext)
        {
            FoldVariable = subqueryContext.PivotVariable;
        }

        internal override GremlinVariableProperty DefaultProjection()
        {
            return new GremlinVariableProperty(this, GremlinKeyword.ScalarValue);
        }

        internal override GremlinVariableProperty DefaultVariableProperty()
        {
            return new GremlinVariableProperty(this, GremlinKeyword.ScalarValue);
        }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Scalar;
        }

        public override WTableReference ToTableReference()
        {
            WSelectQueryBlock queryBlock = SubqueryContext.ToSelectQueryBlock();
            queryBlock.SelectElements.Clear();
            List<WScalarExpression> compose1Parameters = new List<WScalarExpression>();
            foreach (var projectProperty in SubqueryContext.PivotVariable.ProjectedProperties)
            {
                compose1Parameters.Add(FoldVariable.GetVariableProperty(projectProperty).ToScalarExpression());
            }
            WFunctionCall compose1 = SqlUtil.GetFunctionCall(GremlinKeyword.func.Compose1, compose1Parameters);

            List<WScalarExpression> foldParameters = new List<WScalarExpression>();
            foldParameters.Add(compose1);
            foreach (var projectProperty in SubqueryContext.PivotVariable.ProjectedProperties)
            {
                foldParameters.Add(SqlUtil.GetValueExpr(projectProperty));
            }
            queryBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetFunctionCall(GremlinKeyword.func.Fold, foldParameters), GremlinKeyword.ScalarValue));
            return SqlUtil.GetDerivedTable(queryBlock, VariableName);
        }

        internal override void Unfold(GremlinToSqlContext currentContext)
        {
            GremlinTableVariable newVariable = null;
            switch (FoldVariable.GetVariableType())
            {
                case GremlinVariableType.Edge:
                    newVariable = new GremlinUnfoldEdgeVariable(this);
                    break;
                case GremlinVariableType.Vertex:
                    newVariable = new GremlinUnfoldVertexVariable(this);
                    break;
                case GremlinVariableType.Scalar:
                    newVariable = new GremlinUnfoldScalarVariable(this);
                    break;
                case GremlinVariableType.Table:
                    newVariable = new GremlinUnfoldTableVariable(this);
                    break;
            }
            
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
            return SqlUtil.GetDerivedTable(queryBlock, VariableName);
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
            return SqlUtil.GetDerivedTable(queryBlock, VariableName);
        }
    }
}
