using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinCoalesceVariable : GremlinSqlTableVariable
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
        }

        public override  WTableReference ToTableReference(List<string> projectProperties, string tableName)
        {
            List<WScalarExpression> PropertyKeys = new List<WScalarExpression>();

            foreach (var context in CoalesceContextList)
            {
                //TODO about ProjectedProperties
                PropertyKeys.Add(SqlUtil.GetScalarSubquery(context.ToSelectQueryBlock(projectProperties)));
            }
            var secondTableRef = SqlUtil.GetFunctionTableReference("coalesce", PropertyKeys, tableName);

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }

    internal class GremlinCoalesceVertexVariable : GremlinVertexTableVariable
    {
        public GremlinCoalesceVertexVariable(List<GremlinToSqlContext> coalesceContextList)
        {
            SqlTableVariable = new GremlinCoalesceVariable(coalesceContextList);
            VariableName = GenerateTableAlias();
        }
    }

    internal class GremlinCoalesceEdgeVariable : GremlinEdgeTableVariable
    {
        public GremlinCoalesceEdgeVariable(List<GremlinToSqlContext> coalesceContextList)
        {
            SqlTableVariable = new GremlinCoalesceVariable(coalesceContextList);
            VariableName = GenerateTableAlias();
        }
    }

    internal class GremlinCoalesceScalarVariable : GremlinScalarTableVariable
    {
        public GremlinCoalesceScalarVariable(List<GremlinToSqlContext> coalesceContextList)
        {
            SqlTableVariable = new GremlinCoalesceVariable(coalesceContextList);
            VariableName = GenerateTableAlias();
        }
    }

    internal class GremlinCoalesceTableVariable : GremlinTableVariable
    {
        public GremlinCoalesceTableVariable(List<GremlinToSqlContext> coalesceContextList)
        {
            SqlTableVariable = new GremlinCoalesceVariable(coalesceContextList);
            VariableName = GenerateTableAlias();
        }
    }
}
