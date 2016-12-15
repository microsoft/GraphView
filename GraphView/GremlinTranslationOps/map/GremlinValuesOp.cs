using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinValuesOp : GremlinTranslationOperator
    {
        public List<object> PropertyKeys;

        public GremlinValuesOp(params object[] propertyKeys)
        {
            PropertyKeys = new List<object>();
            foreach (var propertyKey in propertyKeys)
            {
                PropertyKeys.Add(propertyKey);
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            if (inputContext.CurrVariable is GremlinVertexVariable)
            {
                PropertyKeys.Insert(0, "node");
                var secondTableRef = GremlinUtil.GetSchemaObjectFunctionTableReference("values", PropertyKeys);

                var newVariable = inputContext.CrossApplyToVariable(inputContext.CurrVariable, secondTableRef, Labels);
                inputContext.SetCurrVariable(newVariable);
                inputContext.SetDefaultProjection(newVariable);
            }
            else if (inputContext.CurrVariable is GremlinEdgeVariable)
            {
                PropertyKeys.Insert(0, "edge");
                var oldVariable = inputContext.PathList.Find(p => p.Item2.VariableName == inputContext.CurrVariable.VariableName).Item1;
                var secondTableRef = GremlinUtil.GetSchemaObjectFunctionTableReference("values", PropertyKeys);

                var newVariable = inputContext.CrossApplyToVariable(oldVariable, secondTableRef, Labels);
                inputContext.SetCurrVariable(newVariable);
                inputContext.SetDefaultProjection(newVariable);

            }
            else
            {
                throw new NotImplementedException();
            }
            return inputContext;
        }
    }
}
