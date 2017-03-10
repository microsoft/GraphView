using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinUnionVariable: GremlinTableVariable
    {
        public List<GremlinToSqlContext> UnionContextList { get; set; }

        public GremlinUnionVariable(List<GremlinToSqlContext> unionContextList, GremlinVariableType variableType)
            : base(variableType)
        {
            UnionContextList = unionContextList;
        }

        internal override GremlinVariableType GetUnfoldVariableType()
        {
            if (UnionContextList.Count == 0) return GremlinVariableType.Table;
            if (UnionContextList.Count == 1) return UnionContextList.First().PivotVariable.GetUnfoldVariableType();
            else throw new NotImplementedException();
        }

        internal override void Populate(string property)
        {
            if (ProjectedProperties.Contains(property)) return;
            base.Populate(property);

            foreach (var context in UnionContextList)
            {
                context.Populate(property);
            }
        }

        internal override GremlinPathStepVariable GetAndPopulatePath()
        {
            List<GremlinVariable> pathStepVariableList = new List<GremlinVariable>();
            foreach (var context in UnionContextList)
            {
                GremlinPathVariable newVariable = context.PopulateGremlinPath();
                pathStepVariableList.Add(newVariable);
            }
            return new GremlinPathStepVariable(pathStepVariableList, this);
        }

        internal override List<GremlinVariable> FetchVarsFromCurrAndChildContext()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>();
            foreach (var context in UnionContextList)
            {
                variableList.AddRange(context.FetchVarsFromCurrAndChildContext());
            }
            return variableList;
        }

        internal override List<GremlinVariable> PopulateAllTaggedVariable(string label)
        {
            List<List<GremlinVariable>> branchVariableList = new List<List<GremlinVariable>>();
            foreach (var context in UnionContextList)
            {
                var variableList = context.SelectVarsFromCurrAndChildContext(label);
                branchVariableList.Add(variableList);
            }
            return new List<GremlinVariable>() {GremlinBranchVariable.Create(label, this, branchVariableList)};
        }

        internal override bool ContainsLabel(string label)
        {
            if (base.ContainsLabel(label)) return true;
            foreach (var context in UnionContextList)
            {
                foreach (var variable in context.VariableList)
                {
                    if (variable.ContainsLabel(label))
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        //internal override WEdgeType GetEdgeType()
        //{
        //    if (UnionContextList.Count <= 1) return UnionContextList.First().PivotVariable.GetEdgeType();
        //    for (var i = 1; i < UnionContextList.Count; i++)
        //    {
        //        var isSameType = UnionContextList[i - 1].PivotVariable.GetEdgeType()
        //                          == UnionContextList[i].PivotVariable.GetEdgeType();
        //        if (isSameType == false) throw new NotImplementedException();
        //    }
        //    return UnionContextList.First().PivotVariable.GetEdgeType();
        //}

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            if (UnionContextList.Count == 0)
            {
                foreach (var property in ProjectedProperties)
                {
                    parameters.Add(SqlUtil.GetValueExpr(property));
                }
            }
            foreach (var context in UnionContextList)
            {
                parameters.Add(SqlUtil.GetScalarSubquery(context.ToSelectQueryBlock(ProjectedProperties)));
            }
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Union, parameters, GetVariableName());

            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
