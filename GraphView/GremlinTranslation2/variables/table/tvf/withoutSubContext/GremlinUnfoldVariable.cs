using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinUnfoldVariable : GremlinSqlTableVariable
    {
        public static GremlinTableVariable Create(GremlinVariable inputVariable)
        {
            switch (inputVariable.GetVariableType())
            {
                case GremlinVariableType.Vertex:
                    return new GremlinUnfoldVertexVariable(inputVariable);
                case GremlinVariableType.Edge:
                    return new GremlinUnfoldEdgeVariable(inputVariable);
                case GremlinVariableType.Table:
                    return new GremlinUnfoldTableVariable(inputVariable);
                case GremlinVariableType.Scalar:
                    return new GremlinUnfoldScalarVariable(inputVariable);
            }
            throw new QueryCompilationException();
        }

        public GremlinVariable UnfoldVariable { get; set; }

        public GremlinUnfoldVariable(GremlinVariable unfoldVariable)
        {
            UnfoldVariable = unfoldVariable;
        }

        internal override void Populate(string property)
        {
            UnfoldVariable.Populate(property);
        }

        public override WTableReference ToTableReference(List<string> projectProperties, string tableName, GremlinVariable gremlinVariable)
        {
            if (UnfoldVariable is GremlinListVariable)
            {
                List<WScalarExpression> parameters = new List<WScalarExpression>();
                parameters.Add((UnfoldVariable as GremlinListVariable).ToScalarExpression());
                foreach (var projectProperty in projectProperties)
                {
                    parameters.Add(SqlUtil.GetValueExpr(projectProperty));
                }
                var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Unfold, parameters, gremlinVariable, tableName);
                return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
            }
            else
            {
                List<WScalarExpression> parameters = new List<WScalarExpression>();
                parameters.Add(UnfoldVariable.DefaultVariableProperty().ToScalarExpression());
                var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Unfold, parameters, gremlinVariable, tableName);
                return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
            }
            throw new NotImplementedException();
        }
    }

    internal class GremlinUnfoldVertexVariable : GremlinVertexTableVariable
    {
        public GremlinUnfoldVertexVariable(GremlinVariable unfoldVariable)
        {
            SqlTableVariable = new GremlinUnfoldVariable(unfoldVariable);
        }
    }

    internal class GremlinUnfoldEdgeVariable : GremlinEdgeTableVariable
    {
        public GremlinUnfoldEdgeVariable(GremlinVariable unfoldVariable)
        {
            SqlTableVariable = new GremlinUnfoldVariable(unfoldVariable);
        }
    }

    internal class GremlinUnfoldTableVariable : GremlinTableVariable
    {
        public GremlinUnfoldTableVariable(GremlinVariable unfoldVariable)
        {
            SqlTableVariable = new GremlinUnfoldVariable(unfoldVariable);
        }
    }

    internal class GremlinUnfoldScalarVariable : GremlinScalarTableVariable
    {
        public GremlinUnfoldScalarVariable(GremlinVariable unfoldVariable)
        {
            SqlTableVariable = new GremlinUnfoldVariable(unfoldVariable);
        }
    }
}
