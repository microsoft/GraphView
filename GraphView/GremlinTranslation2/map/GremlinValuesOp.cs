using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinValuesOp : GremlinTranslationOperator
    {
        public List<object> PropertyKeys { get; set; }

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
            GremlinVariable newVariable;

            if (inputContext.CurrVariable is GremlinVertexVariable)
            {
                if (PropertyKeys.Count > 1)
                {
                    PropertyKeys.Insert(0, inputContext.CurrVariable.VariableName + ".*");
                    var secondTableRef = GremlinUtil.GetSchemaObjectFunctionTableReference("values", PropertyKeys);
                    //inputContext.IsUsedInTVF[inputContext.CurrVariable.VariableName] = true;
                    newVariable = inputContext.CrossApplyToVariable(inputContext.CurrVariable, secondTableRef, Labels);
                }
                else if (PropertyKeys.Count == 1)
                {
                    newVariable = new GremlinScalarVariable2(inputContext.CurrVariable, PropertyKeys.First() as string);
                }
                else
                {
                    throw new NotImplementedException();;
                }
                inputContext.SetCurrVariable(newVariable);
                inputContext.SetDefaultProjection(newVariable);
            }
            else if (inputContext.CurrVariable is GremlinEdgeVariable)
            {
                if (PropertyKeys.Count > 1)
                {
                    var oldVariable = inputContext.GetSourceNode(inputContext.CurrVariable);
                    PropertyKeys.Insert(0, inputContext.CurrVariable.VariableName + ".*");
                    var secondTableRef = GremlinUtil.GetSchemaObjectFunctionTableReference("values", PropertyKeys);
                    //inputContext.IsUsedInTVF[inputContext.CurrVariable.VariableName] = true;

                    newVariable = inputContext.CrossApplyToVariable(oldVariable, secondTableRef, Labels);
                }
                else if (PropertyKeys.Count == 1)
                {
                    inputContext.CurrVariable.Properties.Add(PropertyKeys.First() as string);
                    newVariable = new GremlinScalarVariable2(inputContext.CurrVariable, PropertyKeys.First() as string);
                }
                else
                {
                    throw new NotImplementedException();
                }
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
