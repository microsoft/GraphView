using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinSelectVariable : GremlinTableVariable
    {
        public GremlinVariable InputVariable { get; set; }
        public GremlinSelectPathVariable PathVariable { get; set; }
        public List<GremlinVariable> SideEffectVariables { get; set; } // Such as aggregate("a")/store("a")..
        public List<GremlinToSqlContext> ByContexts { get; set; }
        public List<string> SelectKeys { get; set; }
        public GremlinKeyword.Pop Pop { get; set; }

        public GremlinSelectVariable(GremlinVariable inputVariable, GremlinSelectPathVariable pathVariable,  List<GremlinVariable> sideEffectVariables, 
            GremlinKeyword.Pop pop,  List<string> selectKeys,  List<GremlinToSqlContext> byContexts) : base(GremlinVariableType.Unknown)
        {
            this.InputVariable = inputVariable;
            this.PathVariable = pathVariable;
            this.SideEffectVariables = sideEffectVariables;
            this.Pop = pop;
            this.SelectKeys = selectKeys;
            this.ByContexts = byContexts;
            this.PathVariable.PopulateStepNULL(this.SelectKeys);
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(this.PathVariable.FetchAllVars());
            foreach (var sideEffectVariable in this.SideEffectVariables)
            {
                variableList.AddRange(sideEffectVariable.FetchAllVars());
            }
            foreach (var context in this.ByContexts)
            {
                variableList.AddRange(context.FetchAllVars());
            }
            return variableList;
        }

        internal override List<GremlinTableVariable> FetchAllTableVars()
        {
            List<GremlinTableVariable> variableList = new List<GremlinTableVariable> { this };
            foreach (var context in this.ByContexts)
            {
                variableList.AddRange(context.FetchAllTableVars());
            }
            return variableList;
        }

        internal override bool Populate(string property, string label = null)
        {
            bool populateSuccessfully = false;
            if (label == null || this.Labels.Contains(label))
            {
                if (SelectKeys.Count() == 1)
                {
                    populateSuccessfully = true;
                }
                foreach (string selectKey in this.SelectKeys)
                {
                    if (GremlinVariableType.NULL <= this.InputVariable.GetVariableType() && this.InputVariable.GetVariableType() <= GremlinVariableType.Map)
                    {
                        this.InputVariable.Populate(property, selectKey);
                    }
                    this.PathVariable.PopulateStepProperty(property, selectKey);
                    foreach (var sideEffectVariable in this.SideEffectVariables)
                    {
                        sideEffectVariable.Populate(property, selectKey);
                    }
                    foreach (var context in this.ByContexts)
                    {
                        context.Populate(property, selectKey);
                    }
                }
            }
            else
            {
                if (SelectKeys.Count() == 1)
                {
                    populateSuccessfully = base.Populate(property, label);
                }
                if (GremlinVariableType.NULL <= this.InputVariable.GetVariableType() && this.InputVariable.GetVariableType() <= GremlinVariableType.Map)
                {
                    this.InputVariable.Populate(property, label);
                }
                this.PathVariable.PopulateStepProperty(property, label);
                foreach (var sideEffectVariable in this.SideEffectVariables)
                {
                    sideEffectVariable.Populate(property, label);
                }
                foreach (var context in this.ByContexts)
                {
                    context.Populate(property, label);
                }
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
            List<WSelectQueryBlock> queryBlocks = new List<WSelectQueryBlock>();

            //Must toSelectQueryBlock before toCompose1 of variableList in order to populate needed columns
            //If only one selectKey, we just need to select the last one because it is a map flow.
            if (this.SelectKeys.Count == 1)
            {
                queryBlocks.Add(this.ByContexts[this.ByContexts.Count - 1].ToSelectQueryBlock(true));
            }
            else
            {
                queryBlocks.AddRange(this.ByContexts.Select(byContext => byContext.ToSelectQueryBlock(true)));
            }
            
            parameters.Add(this.InputVariable.DefaultProjection().ToScalarExpression());
            parameters.Add(this.PathVariable.DefaultProjection().ToScalarExpression());
            switch (this.Pop)
            {
                case GremlinKeyword.Pop.All:
                    parameters.Add(SqlUtil.GetValueExpr("All"));
                    break;
                case GremlinKeyword.Pop.First:
                    parameters.Add(SqlUtil.GetValueExpr("First"));
                    break;
                case GremlinKeyword.Pop.Last:
                    parameters.Add(SqlUtil.GetValueExpr("Last"));
                    break;
            }

            parameters.AddRange(this.SelectKeys.Select(SqlUtil.GetValueExpr));
            parameters.AddRange(queryBlocks.Select(SqlUtil.GetScalarSubquery));

            if (this.SelectKeys.Count == 1)
            {
                parameters.AddRange(this.ProjectedProperties.Select(SqlUtil.GetValueExpr));
                var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.SelectOne, parameters, GetVariableName());
                return SqlUtil.GetCrossApplyTableReference(tableRef);
            }
            else
            {
                var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Select, parameters, GetVariableName());
                return SqlUtil.GetCrossApplyTableReference(tableRef);
            }
        }
    }
}
