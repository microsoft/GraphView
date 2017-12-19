using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinDedupVariable : GremlinFilterTableVariable
    {
        public GremlinVariable InputVariable { get; set; }
        public List<GremlinVariable> DedupVariables { get; set; }
        public GremlinToSqlContext DedupContext { get; set; }
        public GremlinKeyword.Scope Scope { get; set; }

        public GremlinDedupVariable(GremlinVariable inputVariable, 
                                    List<GremlinVariable> dedupVariables, 
                                    GremlinToSqlContext dedupContext,
                                    GremlinKeyword.Scope scope) : base(inputVariable.GetVariableType())
        {
            this.InputVariable = inputVariable;
            this.DedupVariables = new List<GremlinVariable>(dedupVariables);
            this.DedupContext = dedupContext;
            this.Scope = scope;
        }

        internal override bool Populate(string property, string label = null)
        {
            bool populateSuccessfully = false;
            if (label == null || this.Labels.Contains(label))
            {
                populateSuccessfully = true;
                if (this.InputVariable != null)
                {
                    this.InputVariable.Populate(property, null);
                }
                foreach (var variable in this.DedupVariables)
                {
                    variable.Populate(property, null);
                }
            }
            else
            {
                if (this.InputVariable != null)
                {
                    populateSuccessfully |= this.InputVariable.Populate(property, label);
                }
                foreach (var variable in this.DedupVariables)
                {
                    populateSuccessfully |= variable.Populate(property, label);
                }
            }
            return populateSuccessfully;
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.Add(this.InputVariable);
            variableList.AddRange(this.DedupVariables);
            if (this.DedupContext != null)
                variableList.AddRange(this.DedupContext.FetchAllVars());
            return variableList;
        }

        internal override List<GremlinTableVariable> FetchAllTableVars()
        {
            List<GremlinTableVariable> variableList = new List<GremlinTableVariable> { this };
            if (this.DedupContext != null)
                variableList.AddRange(this.DedupContext.FetchAllTableVars());
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            if (this.DedupVariables.Count == 0)
            {
                parameters.Add(SqlUtil.GetScalarSubquery(this.DedupContext.ToSelectQueryBlock()));
            }
            else
            {
                parameters.AddRange(
                    this.DedupVariables.Select(
                        dedupVariable => dedupVariable.DefaultProjection().ToScalarExpression()));
            }

            var tableRef = SqlUtil.GetFunctionTableReference(
                this.Scope == GremlinKeyword.Scope.Global ? GremlinKeyword.func.DedupGlobal : GremlinKeyword.func.DedupLocal,
                parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
