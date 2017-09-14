using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinSelectVariable : GremlinTableVariable
    {
        public GremlinContextVariable InputVariable { get; set; }
        public GremlinPathVariable PathVariable { get; set; }
        public List<GremlinVariable> SideEffectVariables { get; set; } // Such as aggregate("a")/store("a")..
        public List<GremlinToSqlContext> ByContexts { get; set; }
        public List<string> SelectKeys { get; set; }
        public GremlinKeyword.Pop Pop { get; set; }

        public GremlinSelectVariable(GremlinVariable inputVariable, 
                                    GremlinPathVariable pathVariable, 
                                    List<GremlinVariable> sideEffectVariables, 
                                    GremlinKeyword.Pop pop, 
                                    List<string> selectKeys, 
                                    List<GremlinToSqlContext> byContexts)
            : base(GremlinVariableType.Table)
        {
            InputVariable = new GremlinContextVariable(inputVariable);
            PathVariable = pathVariable;
            SideEffectVariables = sideEffectVariables;
            Pop = pop;
            SelectKeys = selectKeys;
            ByContexts = byContexts;
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(PathVariable.FetchAllVars());
            foreach (var sideEffectVariable in SideEffectVariables)
            {
                variableList.AddRange(sideEffectVariable.FetchAllVars());
            }
            foreach (var context in ByContexts)
            {
                variableList.AddRange(context.FetchAllVars());
            }
            return variableList;
        }

        internal override List<GremlinTableVariable> FetchAllTableVars()
        {
            List<GremlinTableVariable> variableList = new List<GremlinTableVariable> { this };
            foreach (var context in ByContexts)
            {
                variableList.AddRange(context.FetchAllTableVars());
            }
            return variableList;
        }

        internal override void Populate(string property)
        {
            InputVariable.Populate(property);
            if (property != GremlinKeyword.TableDefaultColumnName)
            {
                PathVariable.Populate(property);
            }
            foreach (var sideEffectVariable in SideEffectVariables)
            {
                sideEffectVariable.Populate(property);
            }
            foreach (var context in ByContexts)
            {
                context.Populate(property);
            }

            if (SelectKeys.Count() > 1 && property != GremlinKeyword.TableDefaultColumnName)
            {
                //block the select multi label to populate column 
                return;
            }
            else
            {
                base.Populate(property);
            }
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            List<WSelectQueryBlock> queryBlocks = new List<WSelectQueryBlock>();

            //Must toSelectQueryBlock before toCompose1 of variableList in order to populate needed columns
            foreach (var byContext in ByContexts)
            {
                queryBlocks.Add(byContext.ToSelectQueryBlock(true));
            }

            parameters.Add(InputVariable.DefaultProjection().ToScalarExpression());
            parameters.Add(PathVariable.DefaultProjection().ToScalarExpression());
            switch (Pop)
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

            foreach (var selectKey in SelectKeys)
            {
                parameters.Add(SqlUtil.GetValueExpr(selectKey));
            }

            if (SelectKeys.Count == 1)
            {
                parameters.Add(SqlUtil.GetScalarSubquery(queryBlocks[queryBlocks.Count - 1]));

                foreach (var projectProperty in ProjectedProperties)
                {
                    parameters.Add(SqlUtil.GetValueExpr(projectProperty));
                }
            }
            else
            {
                foreach (var block in queryBlocks)
                {
                    parameters.Add(SqlUtil.GetScalarSubquery(block));
                }
            }
            var tableRef = SqlUtil.GetFunctionTableReference(
                                SelectKeys.Count == 1 ? GremlinKeyword.func.SelectOne: GremlinKeyword.func.Select, 
                                parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
