using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinOrderVariable: GremlinTableVariable
    {
        public List<Tuple<GremlinToSqlContext, IComparer>> ByModulatingList;
        public GremlinKeyword.Scope Scope { get; set; }
        public GremlinVariable InputVariable { get; set; }
        public GremlinOrderVariable(GremlinVariable inputVariable, List<Tuple<GremlinToSqlContext, IComparer>> byModulatingList, GremlinKeyword.Scope scope)
            :base(inputVariable.GetVariableType())
        {
            this.ByModulatingList = byModulatingList;
            this.Scope = scope;
            this.InputVariable = inputVariable;
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.Add(this.InputVariable);
            foreach (var by in this.ByModulatingList)
            {
                variableList.AddRange(by.Item1.FetchAllVars());
            }
            return variableList;
        }

        internal override List<GremlinTableVariable> FetchAllTableVars()
        {
            List<GremlinTableVariable> variableList = new List<GremlinTableVariable> { this };
            foreach (var by in this.ByModulatingList)
            {
                variableList.AddRange(by.Item1.FetchAllTableVars());
            }
            return variableList;
        }

        internal override bool Populate(string property, string label = null)
        {
            bool populateSuccessfully = false;
            if (label == null || this.Labels.Contains(label))
            {
                populateSuccessfully = true;
                this.InputVariable.Populate(property, null);
            }
            else 
            {
                populateSuccessfully |= this.InputVariable.Populate(property, label);
            }
            if (populateSuccessfully && property != null)
            {
                this.ProjectedProperties.Add(property);
            }
            return populateSuccessfully;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            List<Tuple<WScalarExpression, IComparer>> orderParameters = new List<Tuple<WScalarExpression, IComparer>>();

            if (this.Scope == GremlinKeyword.Scope.Local)
            {
                parameters.Add(this.InputVariable.DefaultProjection().ToScalarExpression());
                foreach (var pair in ByModulatingList)
                {
                    WScalarExpression scalarExpr = SqlUtil.GetScalarSubquery(pair.Item1.ToSelectQueryBlock());
                    orderParameters.Add(new Tuple<WScalarExpression, IComparer>(scalarExpr, pair.Item2));
                    parameters.Add(scalarExpr);
                }
                
                foreach (var property in this.ProjectedProperties)
                {
                    parameters.Add(SqlUtil.GetValueExpr(property));
                }
            }
            else
            {
                foreach (var pair in ByModulatingList)
                {
                    WScalarExpression scalarExpr = SqlUtil.GetScalarSubquery(pair.Item1.ToSelectQueryBlock());
                    orderParameters.Add(new Tuple<WScalarExpression, IComparer>(scalarExpr, pair.Item2));
                    parameters.Add(scalarExpr);
                }
            }

            var tableRef = Scope == GremlinKeyword.Scope.Global
                ? SqlUtil.GetFunctionTableReference(GremlinKeyword.func.OrderGlobal, parameters, GetVariableName())
                : SqlUtil.GetFunctionTableReference(GremlinKeyword.func.OrderLocal, parameters, GetVariableName());
            ((WOrderTableReference) tableRef).OrderParameters = orderParameters;
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }

    internal class GremlinOrderLocalInitVariable : GremlinVariable
    {
        public GremlinOrderLocalInitVariable(GremlinVariable inputVariable) : base(inputVariable.GetVariableType())
        {
            this.VariableName = GremlinKeyword.Compose1TableDefaultName;
        }
    }
}
