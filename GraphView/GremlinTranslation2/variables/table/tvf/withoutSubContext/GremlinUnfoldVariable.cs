using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinUnfoldVariable : GremlinTableVariable
    {
        public static GremlinUnfoldVariable Create(GremlinVariable inputVariable)
        {
            switch (GetUnfoldVariableType(inputVariable))
            {
                case GremlinVariableType.Vertex:
                    return new GremlinUnfoldVertexVariable(inputVariable);
                case GremlinVariableType.Edge:
                    return new GremlinUnfoldEdgeVariable(inputVariable);
                case GremlinVariableType.Scalar:
                    return new GremlinUnfoldScalarVariable(inputVariable);
            }
            return new GremlinUnfoldVariable(inputVariable);
        }

        public static GremlinVariableType GetUnfoldVariableType(GremlinVariable inputVariable)
        {
            if (inputVariable is GremlinFoldVariable)
            {
                return (inputVariable as GremlinFoldVariable).FoldVariable.GetVariableType();
            }
            if (inputVariable is GremlinListVariable)
            {
                return (inputVariable as GremlinListVariable).GetVariableType();
            }
            if (inputVariable is GremlinSelectedVariable)
            {
                return GetUnfoldVariableType((inputVariable as GremlinSelectedVariable).RealVariable);
            }
            return inputVariable.GetVariableType();
        }

        public GremlinVariable UnfoldVariable { get; set; }

        public GremlinUnfoldVariable(GremlinVariable unfoldVariable, GremlinVariableType variableType = GremlinVariableType.Table)
            : base(variableType)
        {
            UnfoldVariable = unfoldVariable;
        }

        internal override bool ContainsLabel(string label)
        {
            return false;
        }

        internal override void Populate(string property)
        {
            if (ProjectedProperties.Contains(property)) return;
            base.Populate(property);
            UnfoldVariable.Populate(property);
        }

        public override WTableReference ToTableReference()
        {
            if (UnfoldVariable is GremlinListVariable)
            {
                List<WScalarExpression> parameters = new List<WScalarExpression>();
                parameters.Add((UnfoldVariable as GremlinListVariable).ToScalarExpression());
                foreach (var projectProperty in ProjectedProperties)
                {
                    parameters.Add(SqlUtil.GetValueExpr(projectProperty));
                }
                var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Unfold, parameters, this, VariableName);
                return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
            }
            else
            {
                List<WScalarExpression> parameters = new List<WScalarExpression>();
                parameters.Add(UnfoldVariable.DefaultVariableProperty().ToScalarExpression());
                parameters.Add(SqlUtil.GetValueExpr(UnfoldVariable.DefaultVariableProperty().VariableProperty));
                var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Unfold, parameters, this, VariableName);
                return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
            }
            throw new NotImplementedException();
        }
    }

    internal class GremlinUnfoldVertexVariable : GremlinUnfoldVariable
    {
        public GremlinUnfoldVertexVariable(GremlinVariable unfoldVariable)
            : base(unfoldVariable, GremlinVariableType.Vertex)
        {
        }
    }

    internal class GremlinUnfoldEdgeVariable : GremlinUnfoldVariable
    {
        public GremlinUnfoldEdgeVariable(GremlinVariable unfoldVariable)
            : base(unfoldVariable, GremlinVariableType.Edge)
        {
        }
    }

    internal class GremlinUnfoldScalarVariable : GremlinUnfoldVariable
    {
        public GremlinUnfoldScalarVariable(GremlinVariable unfoldVariable)
            : base(unfoldVariable, GremlinVariableType.Scalar)
        {
        }
    }
}
