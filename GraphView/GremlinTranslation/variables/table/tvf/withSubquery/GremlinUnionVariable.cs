using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinUnionVariable : GremlinTableVariable
    {
        public List<GremlinToSqlContext> UnionContextList { get; set; }

        public GremlinUnionVariable(List<GremlinToSqlContext> unionContextList, GremlinVariableType variableType)
            : base(variableType)
        {
            UnionContextList = unionContextList;
        }

        internal override void Populate(string property)
        {
            base.Populate(property);

            foreach (var context in UnionContextList)
            {
                context.Populate(property);
            }
        }

        internal override void PopulateStepProperty(string property)
        {
            foreach (var context in UnionContextList)
            {
                context.ContextLocalPath.PopulateStepProperty(property);
            }
        }

        internal override void PopulateLocalPath()
        {
            if (ProjectedProperties.Contains(GremlinKeyword.Path)) return;
            ProjectedProperties.Add(GremlinKeyword.Path);
            foreach (var context in UnionContextList)
            {
                context.PopulateLocalPath();
            }
        }

        internal override WScalarExpression ToStepScalarExpr()
        {
            return SqlUtil.GetColumnReferenceExpr(GetVariableName(), GremlinKeyword.Path);
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            foreach (var context in UnionContextList)
            {
                variableList.AddRange(context.FetchAllVars());
            }
            return variableList;
        }

        internal override List<GremlinVariable> FetchAllTableVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            foreach (var context in UnionContextList)
            {
                variableList.AddRange(context.FetchAllTableVars());
            }
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            if (UnionContextList.Count == 0)
            {
                foreach (var property in ProjectedProperties)
                {
                    parameters.Add(SqlUtil.GetValueExpr(property));
                }
            }
            foreach (var context in UnionContextList)
            {
                parameters.Add(SqlUtil.GetScalarSubquery(context.ToSelectQueryBlock()));
            }
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Union, parameters, GetVariableName());

            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
