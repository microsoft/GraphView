using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinSelectVariable : GremlinTableVariable
    {
        public GremlinPathVariable PathVariable { get; set; }
        public List<GremlinToSqlContext> ByContexts { get; set; }
        public List<string> SelectKeys { get; set; }

        public GremlinSelectVariable(GremlinPathVariable pathVariable, List<string> selectKeys, List<GremlinToSqlContext> byContexts)
            : base(GremlinVariableType.Table)
        {
            PathVariable = pathVariable;
            SelectKeys = selectKeys;
            ByContexts = byContexts;
        }

        internal override List<GremlinVariable> FetchVarsFromCurrAndChildContext()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>();
            foreach (var context in ByContexts)
            {
                variableList.AddRange(context.FetchVarsFromCurrAndChildContext());
            }
            return variableList;
        }

        internal override void Populate(string property)
        {
            foreach (var context in ByContexts)
            {
                context.Populate(property);
            }
            base.Populate(property);
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            List<WSelectQueryBlock> queryBlocks = new List<WSelectQueryBlock>();

            //Must toSelectQueryBlock before toCompose1 of variableList in order to populate needed columns
            foreach (var byContext in ByContexts)
            {
                //TODO: select compose1
                WSelectQueryBlock queryBlock = byContext.ToSelectQueryBlock();
                queryBlock.SelectElements.Clear();
                queryBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(byContext.PivotVariable.ToCompose1(), GremlinKeyword.TableDefaultColumnName));
                queryBlocks.Add(queryBlock);

            }

            parameters.Add(PathVariable.DefaultProjection().ToScalarExpression());

            foreach (var selectKey in SelectKeys)
            {
                parameters.Add(SqlUtil.GetValueExpr(selectKey));
            }

            foreach (var block in queryBlocks)
            {
                parameters.Add(SqlUtil.GetScalarSubquery(block));
            }

            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Select, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
