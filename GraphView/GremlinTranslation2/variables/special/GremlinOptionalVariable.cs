using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinOptionalVariable : GremlinTableVariable, ISqlTable
    {
        public GremlinToSqlContext Context { get; set; }

        public GremlinOptionalVariable(GremlinToSqlContext context)
        {
            Context = context;
            VariableName = GenerateTableAlias();
        }

        public static GremlinOptionalVariable Create(GremlinVariable2 inputVariable, GremlinToSqlContext context)
        {
            if (inputVariable.GetVariableType() == context.PivotVariable.GetVariableType())
            {
                switch (context.PivotVariable.GetVariableType())
                {
                    case GremlinVariableType.Vertex:
                        return new GremlinOptionalVertexVariable(context);
                    case GremlinVariableType.Edge:
                        return new GremlinOptionalEdgeVariable(context);
                    case GremlinVariableType.Table:
                        return new GremlinOptionalTableVariable(context);
                    case GremlinVariableType.Scalar:
                        return new GremlinOptionalScalarVariable(context);
                }
            }
            else
            {
                return new GremlinOptionalTableVariable(context);
            }
            throw new NotImplementedException();
        }

        public override  WTableReference ToTableReference()
        {
            List<WScalarExpression> PropertyKeys = new List<WScalarExpression>();
            PropertyKeys.Add(GremlinUtil.GetScalarSubquery(Context.ToSelectQueryBlock(ProjectedProperties)));
            var secondTableRef = GremlinUtil.GetFunctionTableReference("optional", PropertyKeys, VariableName);
            return GremlinUtil.GetCrossApplyTableReference(null, secondTableRef);
        }

        internal override void Populate(string property)
        {
            Context.Populate(property);
            base.Populate(property);
        }
    }

    internal class GremlinOptionalVertexVariable : GremlinOptionalVariable
    {
        public GremlinOptionalVertexVariable(GremlinToSqlContext context): base(context) {}

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Vertex;
        }
    }

    internal class GremlinOptionalEdgeVariable : GremlinOptionalVariable
    {
        public GremlinOptionalEdgeVariable(GremlinToSqlContext context) : base(context) { }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Edge;
        }
    }

    internal class GremlinOptionalTableVariable : GremlinOptionalVariable
    {
        public GremlinOptionalTableVariable(GremlinToSqlContext context) : base(context) { }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Table;
        }
    }

    internal class GremlinOptionalScalarVariable : GremlinOptionalVariable
    {
        public GremlinOptionalScalarVariable(GremlinToSqlContext context) : base(context) { }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Scalar;
        }
    }
}
