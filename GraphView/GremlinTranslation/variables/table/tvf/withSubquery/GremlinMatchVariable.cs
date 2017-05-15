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
        public GremlinToSqlContext MatchContext { get; set; }

        public GremlinMatchVariable(GremlinToSqlContext matchContext, GremlinVariableType variableType)
            : base(variableType)
        {
            MatchContext = matchContext;
        }

        internal override void Populate(string property)
        {
            base.Populate(property);
            this.MatchContext.Populate(property);
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            throw new NotImplementedException();

            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(MatchContext.FetchAllVars());
            return variableList;
        }

        internal override List<GremlinVariable> FetchAllTableVars()
        {
            throw new NotImplementedException();

            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(MatchContext.FetchAllTableVars());
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetScalarSubquery(MatchContext.ToSelectQueryBlock()));
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Match, parameters, GetVariableName());

            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }

    internal class GremlinMatchStartVariable : GremlinTableVariable
    {
        public string SelectKey;

        public GremlinMatchStartVariable(string selectKey)
            : base(GremlinVariableType.Table)
        {
            this.SelectKey = selectKey;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetValueExpr(this.SelectKey));
            foreach (var property in this.ProjectedProperties)
            {
                parameters.Add(SqlUtil.GetValueExpr(property));
            }
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.MatchStart, parameters, GetVariableName());

            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }

    internal class GremlinMatchEndVariable : GremlinTableVariable
    {
        public string MatchKey;

        public GremlinMatchEndVariable(string matchKey)
            : base(GremlinVariableType.Table)
        {
            this.MatchKey = matchKey;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetValueExpr(this.MatchKey));
            foreach (var property in this.ProjectedProperties)
            {
                parameters.Add(SqlUtil.GetValueExpr(property));
            }
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.MatchEnd, parameters, GetVariableName());

            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
