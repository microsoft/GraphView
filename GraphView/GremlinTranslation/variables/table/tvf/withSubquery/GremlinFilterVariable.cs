using System.Collections.Generic;

namespace GraphView
{
    internal class GremlinFilterVariable : GremlinFilterTableVariable
    {
        public WBooleanExpression Predicate { get; set; }

        public GremlinFilterVariable(GremlinVariable inputVariable, WBooleanExpression newPredicate) : base(inputVariable.GetVariableType())
        {
            this.Predicate = newPredicate;
        }

        internal override bool Populate(string property, string label = null)
        {
            return false;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            
            WSearchedCaseExpression searchCaseExpr = new WSearchedCaseExpression();
            searchCaseExpr.WhenClauses = new List<WSearchedWhenClause>();

            WSearchedWhenClause booleanIsTrueClause = new WSearchedWhenClause();
            booleanIsTrueClause.WhenExpression = this.Predicate;
            booleanIsTrueClause.ThenExpression = new WValueExpression("1");

            searchCaseExpr.WhenClauses.Add(booleanIsTrueClause);
            searchCaseExpr.ElseExpr = new WValueExpression("0");

            parameters.Add(searchCaseExpr);

            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Filter, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}