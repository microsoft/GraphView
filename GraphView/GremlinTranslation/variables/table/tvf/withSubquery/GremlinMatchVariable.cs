using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinMatchVariable : GremlinTableVariable
    {
        public List<GremlinToSqlContext> MatchContextList { get; set; }

        public GremlinMatchVariable(List<GremlinToSqlContext> matchContextList, GremlinVariableType variableType)
            : base(variableType)
        {
            MatchContextList = matchContextList;
        }

        internal override void Populate(string property)
        {
            throw new NotImplementedException();
            base.Populate(property);

            foreach (var context in MatchContextList)
            {
                context.Populate(property);
            }
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            throw new NotImplementedException();

            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            foreach (var context in MatchContextList)
            {
                variableList.AddRange(context.FetchAllVars());
            }
            return variableList;
        }

        internal override List<GremlinVariable> FetchAllTableVars()
        {
            throw new NotImplementedException();

            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            foreach (var context in MatchContextList)
            {
                variableList.AddRange(context.FetchAllTableVars());
            }
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            throw new NotImplementedException();

            List<WScalarExpression> parameters = new List<WScalarExpression>();

            foreach (var context in MatchContextList)
            {
                parameters.Add(SqlUtil.GetScalarSubquery(context.ToSelectQueryBlock()));
            }
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Coalesce, parameters, GetVariableName());

            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
