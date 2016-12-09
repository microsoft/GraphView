using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinPropertiesOp: GremlinTranslationOperator
    {
        public List<object> PropertyKeys;

        public GremlinPropertiesOp(params object[] propertyKeys)
        {
            PropertyKeys = new List<object>();
            foreach (var propertyKey in  propertyKeys)
            {
                PropertyKeys.Add(propertyKey);
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            WUnqualifiedJoin tableReference = new WUnqualifiedJoin()
            {
                FirstTableRef = GremlinUtil.GetNamedTableReference(inputContext.CurrVariable),
                SecondTableRef = GremlinUtil.GetSchemaObjectFunctionTableReference("properties", PropertyKeys),
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
