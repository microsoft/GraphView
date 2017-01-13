using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinUnionVariable: GremlinSqlTableVariable
    {
        public static GremlinTableVariable Create(List<GremlinToSqlContext> unionContextList)
        {
            if (GremlinUtil.IsTheSameOutputType(unionContextList))
            {
                switch (unionContextList.First().PivotVariable.GetVariableType())
                {
                    case GremlinVariableType.Vertex:
                        return new GremlinUnionVertexVariable(unionContextList);
                    case GremlinVariableType.Edge:
                        return new GremlinUnionEdgeVariable(unionContextList);
                    case GremlinVariableType.Table:
                        return new GremlinUnionTableVariable(unionContextList);
                    case GremlinVariableType.Scalar:
                        return new GremlinUnionScalarVariable(unionContextList);
                }
            }
            return new GremlinUnionTableVariable(unionContextList);
        }

        public List<GremlinToSqlContext> UnionContextList { get; set; }

        public GremlinUnionVariable(List<GremlinToSqlContext> unionContextList)
        {
            UnionContextList = unionContextList;
        }

        internal override void Populate(string property)
        {
            foreach (var context in UnionContextList)
            {
                context.Populate(property);
            }
        }

        internal override void PopulateGremlinPath()
        {
            foreach (var context in UnionContextList)
            {
                context.PopulateGremlinPath();
            }
        }

        public override WTableReference ToTableReference(List<string> projectProperties, string tableName, GremlinVariable gremlinVariable)
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();

            if (projectProperties.Count == 0)
            {
                Populate(UnionContextList.First().PivotVariable.DefaultProjection().VariableProperty);
            }
            foreach (var context in UnionContextList)
            {
                parameters.Add(SqlUtil.GetScalarSubquery(context.ToSelectQueryBlock(projectProperties)));
            }
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Union, parameters, gremlinVariable, tableName);

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }

    internal class GremlinUnionVertexVariable : GremlinVertexTableVariable
    {
        public GremlinUnionVertexVariable(List<GremlinToSqlContext> unionContextList)
        {
            SqlTableVariable = new GremlinUnionVariable(unionContextList);
        }
    }

    internal class GremlinUnionEdgeVariable : GremlinEdgeTableVariable
    {
        public GremlinUnionEdgeVariable(List<GremlinToSqlContext> unionContextList)
        {
            SqlTableVariable = new GremlinUnionVariable(unionContextList);
        }
    }

    internal class GremlinUnionScalarVariable : GremlinScalarTableVariable
    {
        public GremlinUnionScalarVariable(List<GremlinToSqlContext> unionContextList)
        {
            SqlTableVariable = new GremlinUnionVariable(unionContextList);
        }
    }

    internal class GremlinUnionTableVariable : GremlinTableVariable
    {
        public GremlinUnionTableVariable(List<GremlinToSqlContext> unionContextList)
        {
            SqlTableVariable = new GremlinUnionVariable(unionContextList);
        }

        internal override GremlinVariableProperty DefaultProjection()
        {
            string key =
                (SqlTableVariable as GremlinUnionVariable).UnionContextList.First()
                    .PivotVariable.DefaultProjection()
                    .VariableProperty;
            return new GremlinVariableProperty(this, key);
        }
    }
}
