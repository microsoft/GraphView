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
        public object Constant { get; set; }

        public GremlinConstantOp(object constant)
        {
            Constant = constant;
        }

        public override GremlinToSqlContext GetContext()
        { 
            GremlinToSqlContext inputContext = GetInputContext();

            List<object> parameter = new List<object>() {Constant};

            var secondTableRef = GremlinUtil.GetSchemaObjectFunctionTableReference("constant", parameter);

            var newVariable = inputContext.CrossApplyToVariable(inputContext.CurrVariable, secondTableRef, Labels);
            inputContext.SetCurrVariable(newVariable);
            inputContext.SetDefaultProjection(newVariable);

            return inputContext;
        }
    }
}
