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
            this.LocalContext = localContext;
        }

        internal override bool Populate(string property, string label = null)
        {
            if (base.Populate(property, label))
            {
                LocalContext.Populate(property, null);
                return true;
            }
            else if (this.LocalContext.Populate(property, label))
            {
                base.Populate(property, null);
                return true;
            }
            else
            {
                return false;
            }
        }

        internal override bool PopulateStepProperty(string property, string label = null)
        {
            return this.LocalContext.ContextLocalPath.PopulateStepProperty(property, label);
        }

        internal override void PopulateLocalPath()
        {
            if (ProjectedProperties.Contains(GremlinKeyword.Path))
            {
                return;
            }
            ProjectedProperties.Add(GremlinKeyword.Path);
            this.LocalContext.PopulateLocalPath();
            this.LocalPathLengthLowerBound = this.LocalContext.MinPathLength;
        }

        internal override WScalarExpression ToStepScalarExpr(List<string> composedProperties = null)
        {
            return SqlUtil.GetColumnReferenceExpr(GetVariableName(), GremlinKeyword.Path);
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(this.LocalContext.FetchAllVars());
            return variableList;
        }

        internal override List<GremlinTableVariable> FetchAllTableVars()
        {
            List<GremlinTableVariable> variableList = new List<GremlinTableVariable> { this };
            variableList.AddRange(this.LocalContext.FetchAllTableVars());
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetScalarSubquery(this.LocalContext.ToSelectQueryBlock()));
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Local, parameters, GetVariableName());

            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
