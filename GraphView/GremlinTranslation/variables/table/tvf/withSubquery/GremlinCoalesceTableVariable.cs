using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinCoalesceVariable : GremlinTableVariable
    {
        public List<GremlinToSqlContext> CoalesceContextList { get; set; }

        public GremlinCoalesceVariable(List<GremlinToSqlContext> coalesceContextList, GremlinVariableType variableType)
            : base(variableType)
        {
            CoalesceContextList = new List<GremlinToSqlContext>(coalesceContextList);
            foreach (var context in coalesceContextList)
            {
                context.HomeVariable = this;
            }
        }

        internal override void Populate(string property)
        {
            if (ProjectedProperties.Contains(property)) return;
            base.Populate(property);

            foreach (var context in CoalesceContextList)
            {
                context.Populate(property);
            }
        }

        internal override bool ContainsLabel(string label)
        {
            if (base.ContainsLabel(label)) return true;
            foreach (var context in CoalesceContextList)
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

        internal override List<GremlinVariable> PopulateAllTaggedVariable(string label)
        {
            //Coalesce step should be regarded as one step, so we can't populate the tagged variable of coalesceContextList 
            return base.PopulateAllTaggedVariable(label);
        }

        internal override List<GremlinVariable> FetchVarsFromCurrAndChildContext()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>();
            foreach (var context in CoalesceContextList)
            {
                variableList.AddRange(context.FetchVarsFromCurrAndChildContext());
            }
            return variableList;
        }

        internal override GremlinVariableType GetUnfoldVariableType()
        {
            throw new NotImplementedException();
        }

        public override  WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();

            foreach (var context in CoalesceContextList)
            {
                parameters.Add(SqlUtil.GetScalarSubquery(context.ToSelectQueryBlock(ProjectedProperties)));
            }
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Coalesce, parameters, GetVariableName());

            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
