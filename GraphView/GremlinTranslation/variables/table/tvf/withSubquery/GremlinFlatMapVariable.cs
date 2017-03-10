using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinFlatMapVariable: GremlinTableVariable
    {
        public GremlinToSqlContext FlatMapContext { get; set; }

        public GremlinFlatMapVariable(GremlinToSqlContext flatMapContext, GremlinVariableType variableType)
            : base(variableType)
        {
            FlatMapContext = flatMapContext;
        }

        internal override void Populate(string property)
        {
            if (ProjectedProperties.Contains(property)) return;
            base.Populate(property);

            FlatMapContext.Populate(property);
        }

        internal override bool ContainsLabel(string label)
        {
            if (base.ContainsLabel(label)) return true;
            return false;
        }

        internal override GremlinVariableType GetUnfoldVariableType()
        {
            return FlatMapContext.PivotVariable.GetUnfoldVariableType();
        }

        internal override List<GremlinVariable> PopulateAllTaggedVariable(string label)
        {
            //flatMap step should be regarded as one step, so we can't populate the tagged variable of FlatMapContext 
            return base.PopulateAllTaggedVariable(label);
        }

        internal override List<GremlinVariable> FetchVarsFromCurrAndChildContext()
        {
            return FlatMapContext == null ? new List<GremlinVariable>() : FlatMapContext.FetchVarsFromCurrAndChildContext();
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetScalarSubquery(FlatMapContext.ToSelectQueryBlock(ProjectedProperties)));
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.FlatMap, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
