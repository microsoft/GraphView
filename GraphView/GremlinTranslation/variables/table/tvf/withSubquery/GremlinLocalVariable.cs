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
            bool populateSuccessfully = false;
            if (label == null || this.Labels.Contains(label))
            {
                populateSuccessfully = true;
                this.LocalContext.Populate(property, null);
            }
            else if (this.LocalContext.Populate(property, label))
            {
                populateSuccessfully = true;
            }
            if (populateSuccessfully && property != null)
            {
                this.ProjectedProperties.Add(property);
            }
            return populateSuccessfully;
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

        internal override WScalarExpression ToStepScalarExpr(HashSet<string> composedProperties = null)
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
