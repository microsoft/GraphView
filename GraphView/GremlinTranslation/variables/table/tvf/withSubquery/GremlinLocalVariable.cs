using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinLocalVariable : GremlinTableVariable
    {
        public GremlinToSqlContext LocalContext { get; set; }

        public GremlinLocalVariable(GremlinToSqlContext localContext, GremlinVariableType variableType)
            : base(variableType)
        {
            LocalContext = localContext;
        }

        internal override void Populate(string property)
        {
            base.Populate(property);
            LocalContext.Populate(property);
        }

        internal override void PopulateStepProperty(string property)
        {
            LocalContext.ContextLocalPath.PopulateStepProperty(property);
        }

        internal override void PopulateLocalPath()
        {
            if (ProjectedProperties.Contains(GremlinKeyword.Path)) return;
            ProjectedProperties.Add(GremlinKeyword.Path);
            LocalContext.PopulateLocalPath();
        }

        internal override WScalarExpression ToStepScalarExpr()
        {
            return SqlUtil.GetColumnReferenceExpr(GetVariableName(), GremlinKeyword.Path);
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(LocalContext.FetchAllVars());
            return variableList;
        }

        internal override List<GremlinVariable> FetchAllTableVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(LocalContext.FetchAllTableVars());
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetScalarSubquery(LocalContext.ToSelectQueryBlock()));
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Local, parameters, GetVariableName());

            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
