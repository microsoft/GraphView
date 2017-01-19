using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinOptionalVariable : GremlinSqlTableVariable
    {
        public static GremlinTableVariable Create(GremlinVariable inputVariable, GremlinToSqlContext context)
        {
            if (inputVariable.GetVariableType() == context.PivotVariable.GetVariableType())
            {
                switch (context.PivotVariable.GetVariableType())
                {
                    case GremlinVariableType.Vertex:
                        return new GremlinOptionalVertexVariable(context, inputVariable);
                    case GremlinVariableType.Edge:
                        return new GremlinOptionalEdgeVariable(context, inputVariable);
                    case GremlinVariableType.Table:
                        return new GremlinOptionalTableVariable(context, inputVariable);
                    case GremlinVariableType.Scalar:
                        return new GremlinOptionalScalarVariable(context, inputVariable);
                }
            }
            else
            {
                return new GremlinOptionalTableVariable(context, inputVariable);
            }
            throw new QueryCompilationException();
        }

        public GremlinToSqlContext OptionalContext { get; set; }
        public GremlinVariable InputVariable { get; set; }

        public GremlinOptionalVariable(GremlinToSqlContext context, GremlinVariable inputVariable)
        {
            OptionalContext = context;
            InputVariable = inputVariable;
        }

        internal override void Populate(string property)
        {
            InputVariable.Populate(property);
            OptionalContext.Populate(property);
        }

        internal override List<GremlinVariable> PopulateAllTaggedVariable(string label, GremlinVariable parentVariable)
        {
            return OptionalContext.SelectCurrentAndChildVariable(label);
        }

        internal override List<GremlinVariable> FetchAllVariablesInCurrAndChildContext()
        {
            return OptionalContext.FetchAllVariablesInCurrAndChildContext();
        }

        internal override void PopulateGremlinPath()
        {
            OptionalContext.PopulateGremlinPath();
        }


        internal override bool ContainsLabel(string label)
        {
            foreach (var variable in OptionalContext.VariableList)
            {
                if (variable.ContainsLabel(label))
                {
                    return true;
                }
            }
            return false;
        }

        public override WTableReference ToTableReference(List<string> projectProperties, string tableName, GremlinVariable gremlinVariable)
        {
            WSelectQueryBlock firstQueryExpr = new WSelectQueryBlock();
            WSelectQueryBlock secondQueryExpr = OptionalContext.ToSelectQueryBlock();
            secondQueryExpr.SelectElements.Clear();
            firstQueryExpr.SelectElements.Add(SqlUtil.GetSelectScalarExpr(InputVariable.DefaultProjection().ToScalarExpression(), GremlinKeyword.TableDefaultColumnName));
            secondQueryExpr.SelectElements.Add(SqlUtil.GetSelectScalarExpr(OptionalContext.PivotVariable.DefaultProjection().ToScalarExpression(), GremlinKeyword.TableDefaultColumnName));
            foreach (var projectProperty in projectProperties)
            {
                if (InputVariable.ContainsProperties(projectProperty))
                {
                    firstQueryExpr.SelectElements.Add(
                        SqlUtil.GetSelectScalarExpr(
                            InputVariable.GetVariableProperty(projectProperty).ToScalarExpression(), projectProperty));
                }
                else
                {
                    firstQueryExpr.SelectElements.Add(
                        SqlUtil.GetSelectScalarExpr(SqlUtil.GetValueExpr(null), projectProperty));
                }
                if (OptionalContext.PivotVariable.ContainsProperties(projectProperty))
                {
                    secondQueryExpr.SelectElements.Add(
                        SqlUtil.GetSelectScalarExpr(
                            OptionalContext.PivotVariable.GetVariableProperty(projectProperty).ToScalarExpression(), projectProperty));
                }
                else
                {
                    secondQueryExpr.SelectElements.Add(
                        SqlUtil.GetSelectScalarExpr(SqlUtil.GetValueExpr(null), projectProperty));
                }
            }

            var WBinaryQueryExpression = SqlUtil.GetBinaryQueryExpr(firstQueryExpr, secondQueryExpr);

            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetScalarSubquery(WBinaryQueryExpression));
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Optional, parameters, gremlinVariable, tableName);
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }

    internal class GremlinOptionalVertexVariable : GremlinVertexTableVariable
    {
        public GremlinOptionalVertexVariable(GremlinToSqlContext context, GremlinVariable inputVariable)
        {
            SqlTableVariable = new GremlinOptionalVariable(context, inputVariable);
        }
    }

    internal class GremlinOptionalEdgeVariable : GremlinEdgeTableVariable
    {
        public GremlinOptionalEdgeVariable(GremlinToSqlContext context, GremlinVariable inputVariable)
        {
            SqlTableVariable = new GremlinOptionalVariable(context, inputVariable);
        }
    }

    internal class GremlinOptionalTableVariable : GremlinTableVariable
    {
        public GremlinOptionalTableVariable(GremlinToSqlContext context, GremlinVariable inputVariable)
        {
            SqlTableVariable = new GremlinOptionalVariable(context, inputVariable);
        }
    }

    internal class GremlinOptionalScalarVariable : GremlinScalarTableVariable
    {
        public GremlinOptionalScalarVariable(GremlinToSqlContext context, GremlinVariable inputVariable)
        {
            SqlTableVariable = new GremlinOptionalVariable(context, inputVariable);
        }
    }
}
