using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinConstantOp: GremlinTranslationOperator
    {
        public object Constant;

        public GremlinConstantOp(object constant)
        {
            Constant = constant;
        }

        public override GremlinToSqlContext GetContext()
        { 
            GremlinToSqlContext inputContext = GetInputContext();

            List<object> parameter = new List<object>() {Constant};

            WUnqualifiedJoin tableReference = new WUnqualifiedJoin()
            {
                FirstTableRef = GremlinUtil.GetTableReferenceFromVariable(inputContext.CurrVariable),
                SecondTableRef = GremlinUtil.GetSchemaObjectFunctionTableReference("constant", parameter),
                UnqualifiedJoinType = UnqualifiedJoinType.CrossApply
            };

            GremlinTVFVariable newVariable = new GremlinTVFVariable(tableReference);
            inputContext.ReplaceVariable(newVariable, Labels);
            inputContext.SetCurrVariable(newVariable);
            inputContext.SetDefaultProjection(newVariable);


            return inputContext;
        }
    }
}
