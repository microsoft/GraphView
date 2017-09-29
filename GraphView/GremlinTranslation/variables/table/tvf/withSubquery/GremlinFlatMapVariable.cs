using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinFlatMapVariable: GremlinTableVariable
    {
        public GremlinToSqlContext FlatMapContext { get; set; }

        public GremlinFlatMapVariable(GremlinToSqlContext flatMapContext, GremlinVariableType variableType)
            : base(variableType)
        {
            this.FlatMapContext = flatMapContext;
        }

        internal override bool Populate(string property, string label = null)
        {
            if (base.Populate(property, label))
            {
                return FlatMapContext.Populate(property, null);
            }
            else if (this.FlatMapContext.Populate(property, label))
            {
                return base.Populate(property, null);
            }
            else
            {
                return false;
            }
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(this.FlatMapContext.FetchAllVars());
            return variableList;
        }

        internal override List<GremlinTableVariable> FetchAllTableVars()
        {
            List<GremlinTableVariable> variableList = new List<GremlinTableVariable> { this };
            variableList.AddRange(this.FlatMapContext.FetchAllTableVars());
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetScalarSubquery(this.FlatMapContext.ToSelectQueryBlock()));
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.FlatMap, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
