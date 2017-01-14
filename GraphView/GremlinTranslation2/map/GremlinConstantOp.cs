using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinConstantOp: GremlinTranslationOperator
    {
        public object Constant { get; set; }
        public List<object> ConstantList { get; set; }

        public GremlinConstantOp(object constant)
        {
            Constant = constant;
        }

        public GremlinConstantOp(List<object> constantList)
        {
            ConstantList = new List<object>(constantList);
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            inputContext.PivotVariable.Constant(inputContext, Constant);

            return inputContext;
        }
    }
}
