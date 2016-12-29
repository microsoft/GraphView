using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
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

            if (inputContext.CurrVariable is GremlinVertexVariable)
            {
                PropertyKeys.Insert(0, inputContext.CurrVariable.VariableName + ".*");
                //inputContext.IsUsedInTVF[inputContext.CurrVariable.VariableName] = true;
                var secondTableRef = GremlinUtil.GetSchemaObjectFunctionTableReference("properties", PropertyKeys);

                var newVariable = inputContext.CrossApplyToVariable(inputContext.CurrVariable, secondTableRef, Labels);
                inputContext.SetCurrVariable(newVariable);
                inputContext.SetDefaultProjection(newVariable);

            }
            else if (inputContext.CurrVariable is GremlinEdgeVariable)
            {
                var oldVariable = inputContext.GetSourceNode(inputContext.CurrVariable);
                PropertyKeys.Insert(0, inputContext.CurrVariable.VariableName + ".*");
                //inputContext.IsUsedInTVF[inputContext.CurrVariable.VariableName] = true;
                var secondTableRef = GremlinUtil.GetSchemaObjectFunctionTableReference("properties", PropertyKeys);

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
