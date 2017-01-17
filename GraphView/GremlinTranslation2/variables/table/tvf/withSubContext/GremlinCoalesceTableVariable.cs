using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinCoalesceVariable : GremlinSqlTableVariable
    {
        public static GremlinTableVariable Create(List<GremlinToSqlContext> coalesceContextList)
        {
            if (GremlinUtil.IsTheSameOutputType(coalesceContextList))
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
            return new GremlinCoalesceTableVariable(coalesceContextList);
        }

        public List<GremlinToSqlContext> CoalesceContextList { get; set; }

        public GremlinCoalesceVariable(List<GremlinToSqlContext> coalesceContextList)
        {
            CoalesceContextList = new List<GremlinToSqlContext>(coalesceContextList);
        }

        internal override void Populate(string property)
        {
            foreach (var context in CoalesceContextList)
            {
                context.Populate(property);
            }
        }

        internal override void PopulateGremlinPath()
        {
            foreach (var context in CoalesceContextList)
            {
                context.PopulateGremlinPath();
            }
        }

        internal override bool ContainsLabel(string label)
        {
            foreach (var context in CoalesceContextList)
            {
                foreach (var variable in context.VariableList)
                {
                    if (variable.ContainsLabel(label))
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        public override  WTableReference ToTableReference(List<string> projectProperties, string tableName, GremlinVariable gremlinVariable)
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();

            foreach (var context in CoalesceContextList)
            {
                parameters.Add(SqlUtil.GetScalarSubquery(context.ToSelectQueryBlock(projectProperties)));
            }
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Coalesce, parameters, gremlinVariable, tableName);

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }

    internal class GremlinCoalesceVertexVariable : GremlinVertexTableVariable
    {
        public GremlinCoalesceVertexVariable(List<GremlinToSqlContext> coalesceContextList)
        {
            SqlTableVariable = new GremlinCoalesceVariable(coalesceContextList);
        }
    }

    internal class GremlinCoalesceEdgeVariable : GremlinEdgeTableVariable
    {
        public GremlinCoalesceEdgeVariable(List<GremlinToSqlContext> coalesceContextList)
        {
            SqlTableVariable = new GremlinCoalesceVariable(coalesceContextList);
        }
    }

    internal class GremlinCoalesceScalarVariable : GremlinScalarTableVariable
    {
        public GremlinCoalesceScalarVariable(List<GremlinToSqlContext> coalesceContextList)
        {
            SqlTableVariable = new GremlinCoalesceVariable(coalesceContextList);
        }
    }

    internal class GremlinCoalesceTableVariable : GremlinTableVariable
    {
        public GremlinCoalesceTableVariable(List<GremlinToSqlContext> coalesceContextList)
        {
            SqlTableVariable = new GremlinCoalesceVariable(coalesceContextList);
        }
    }
}
