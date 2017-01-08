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

        public override WTableReference ToTableReference(List<string> projectProperties, string tableName)
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(UnfoldVariable.DefaultProjection().ToScalarExpression());
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Unfold, parameters, tableName);
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
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

    internal class GremlinUnfoldTableVariable : GremlinEdgeTableVariable
    {
        public GremlinUnfoldTableVariable(GremlinVariable unfoldVariable)
        {
            SqlTableVariable = new GremlinUnfoldVariable(unfoldVariable);
        }
    }

    internal class GremlinUnfoldScalarVariable : GremlinEdgeTableVariable
    {
        public GremlinUnfoldScalarVariable(GremlinVariable unfoldVariable)
        {
            SqlTableVariable = new GremlinUnfoldVariable(unfoldVariable);
        }
    }
}
