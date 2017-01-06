using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinCoalesceVariable : GremlinTableVariable
    {
        public List<GremlinToSqlContext> CoalesceContextList { get; set; }

        public static GremlinTableVariable Create(List<GremlinToSqlContext> coalesceContextList)
        {
            bool isSameType = true;
            for (var i = 1; i < coalesceContextList.Count; i++)
            {
                isSameType = coalesceContextList[i - 1].PivotVariable.GetVariableType() ==
                             coalesceContextList[i].PivotVariable.GetVariableType();
            }

            if (isSameType)
            {
                switch (coalesceContextList.First().PivotVariable.GetVariableType())
                {
                    case GremlinVariableType.Vertex:
                        return new GremlinCoalesceVertexVariable(coalesceContextList);
                    case GremlinVariableType.Edge:
                        return new GremlinCoalesceEdgeVariable(coalesceContextList);
                    case GremlinVariableType.Table:
                        return new GremlinCoalesceTableVariable(coalesceContextList);
                    case GremlinVariableType.Scalar:
                        return new GremlinCoalesceScalarVariable(coalesceContextList);
                }
            }
            throw new NotImplementedException();
        }

        public GremlinCoalesceVariable(List<GremlinToSqlContext> coalesceContextList)
        {
            CoalesceContextList = new List<GremlinToSqlContext>(coalesceContextList);
            VariableName = GenerateTableAlias();
        }

        public override  WTableReference ToTableReference()
        {
            List<WScalarExpression> PropertyKeys = new List<WScalarExpression>();

            foreach (var context in CoalesceContextList)
            {
                //TODO about ProjectedProperties
                PropertyKeys.Add(SqlUtil.GetScalarSubquery(context.ToSelectQueryBlock(ProjectedProperties)));
            }
            var secondTableRef = SqlUtil.GetFunctionTableReference("Coalesce", PropertyKeys, VariableName);

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }

    internal class GremlinCoalesceVertexVariable : GremlinVertexTableVariable
    {
        public GremlinCoalesceVertexVariable(List<GremlinToSqlContext> coalesceContextList)
        {
            InnerVariable = new GremlinCoalesceVariable(coalesceContextList);
        }
    }

    internal class GremlinCoalesceEdgeVariable : GremlinEdgeTableVariable
    {
        public GremlinCoalesceEdgeVariable(List<GremlinToSqlContext> coalesceContextList)
        {
            InnerVariable = new GremlinCoalesceVariable(coalesceContextList);
        }
    }

    internal class GremlinCoalesceScalarVariable : GremlinScalarTableVariable
    {
        public GremlinCoalesceScalarVariable(List<GremlinToSqlContext> coalesceContextList)
        {
            InnerVariable = new GremlinCoalesceVariable(coalesceContextList);
        }
    }

    internal class GremlinCoalesceTableVariable : GremlinTableVariable
    {
        public GremlinCoalesceTableVariable(List<GremlinToSqlContext> coalesceContextList)
        {
            InnerVariable = new GremlinCoalesceVariable(coalesceContextList);
        }
    }
}
