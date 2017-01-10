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

        public override WTableReference ToTableReference(List<string> projectProperties, string tableName)
        {
            List<string> firstProjectProperties = new List<string>();
            List<string> secondProjectProperties = new List<string>();

            foreach (var projectProperty in projectProperties)
            {
                if ((InputVariable.GetVariableType() == GremlinVariableType.Vertex
                     || InputVariable.GetVariableType() == GremlinVariableType.Scalar)
                    && (projectProperty == "_sink"
                        || projectProperty == "_ID"
                        || projectProperty == "_reverse_ID"
                        || projectProperty == "_other"
                        || projectProperty == "_source"
                    ))
                {
                    firstProjectProperties.Add(null);
                }
                else if (InputVariable.GetVariableType() == GremlinVariableType.Edge
                         && (projectProperty == "id"
                             || projectProperty == "_edge"
                             || projectProperty == "_reverse_edge"))
                {
                    firstProjectProperties.Add(null);
                }
                else
                {
                    firstProjectProperties.Add(projectProperty);
                }

                if ((OptionalContext.PivotVariable.GetVariableType() == GremlinVariableType.Vertex
                     || OptionalContext.PivotVariable.GetVariableType() == GremlinVariableType.Scalar)
                    && (projectProperty == "_sink"
                        || projectProperty == "_ID"
                        || projectProperty == "_reverse_ID"
                        || projectProperty == "_other"
                        || projectProperty == "_source"
                    ))
                {
                    secondProjectProperties.Add(null);
                }
                else if (OptionalContext.PivotVariable.GetVariableType() == GremlinVariableType.Edge
                         && (projectProperty == "id"
                             || projectProperty == "_edge"
                             || projectProperty == "_reverse_edge"))
                {
                    secondProjectProperties.Add(null);
                }
                else
                {
                    secondProjectProperties.Add(projectProperty);
                }
            }
            WSelectQueryBlock firstQueryExpr = SqlUtil.GetSimpleSelectQueryBlock(InputVariable.VariableName, firstProjectProperties);
            WSelectQueryBlock secondQueryExpr = OptionalContext.ToSelectQueryBlock(secondProjectProperties);

            var WBinaryQueryExpression = SqlUtil.GetBinaryQueryExpr(firstQueryExpr, secondQueryExpr);

            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetScalarSubquery(WBinaryQueryExpression));
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Optional, parameters, tableName);
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
