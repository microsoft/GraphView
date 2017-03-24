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

        public GremlinCoalesceVariable(List<GremlinToSqlContext> coalesceContextList, GremlinVariableType variableType)
            : base(variableType)
        {
            CoalesceContextList = new List<GremlinToSqlContext>(coalesceContextList);
        }

        internal override void Populate(string property)
        {
            base.Populate(property);

            foreach (var context in CoalesceContextList)
            {
                context.Populate(property);
            }
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() {this};
            foreach (var context in CoalesceContextList)
            {
                variableList.AddRange(context.FetchAllVars());
            }
            return variableList;
        }

        internal override List<GremlinVariable> FetchAllTableVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() {this};
            foreach (var context in CoalesceContextList)
            {
                variableList.AddRange(context.FetchAllTableVars());
            }
            return variableList;
        }

        public override  WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();

            foreach (var context in CoalesceContextList)
            {
                parameters.Add(SqlUtil.GetScalarSubquery(context.ToSelectQueryBlock()));
            }
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Coalesce, parameters, GetVariableName());

            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
