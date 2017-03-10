using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinStoreVariable : GremlinScalarTableVariable
    {
        public string SideEffectKey { get; set; }
        public GremlinToSqlContext ProjectContext { get; set; }

        public GremlinStoreVariable(GremlinToSqlContext projectContext, string sideEffectKey)
        {
            ProjectContext = projectContext;
            SideEffectKey = sideEffectKey;
            Labels.Add(sideEffectKey);
        }

        internal override GremlinVariableType GetUnfoldVariableType()
        {
            return ProjectContext.PivotVariable.GetVariableType();
        }

        internal override List<GremlinVariable> FetchVarsFromCurrAndChildContext()
        {
            return ProjectContext.FetchVarsFromCurrAndChildContext();
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            //TODO: refactor
            WSelectQueryBlock selectQueryBlock = ProjectContext.ToSelectQueryBlock();
            selectQueryBlock.SelectElements.Clear();
            selectQueryBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(ProjectContext.PivotVariable.ToCompose1(), GremlinKeyword.TableDefaultColumnName));
            parameters.Add(SqlUtil.GetScalarSubquery(selectQueryBlock));
            parameters.Add(SqlUtil.GetValueExpr(SideEffectKey));
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Store, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
