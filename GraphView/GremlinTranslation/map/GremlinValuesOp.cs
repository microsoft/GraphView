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
        public List<string> PropertyKeys { get; set; }

        public GremlinValuesOp(params string[] propertyKeys)
        {
            PropertyKeys = new List<string>();
            foreach (var propertyKey in propertyKeys)
            {
                PropertyKeys.Add(propertyKey);
            }
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            inputContext.PivotVariable.Values(inputContext, PropertyKeys);

            return inputContext;
        }
    }

    internal class GremlinIdOp : GremlinTranslationOperator
    {
       internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            List<string> propertyKeys = new List<string>();
            if (inputContext.PivotVariable.GetVariableType() == GremlinVariableType.Vertex)
            {
                propertyKeys.Add(GremlinKeyword.NodeID);
            }
            else if (inputContext.PivotVariable.GetVariableType() == GremlinVariableType.Edge)
            {
                propertyKeys.Add(GremlinKeyword.EdgeID);
            }
            else if (inputContext.PivotVariable.GetVariableType() == GremlinVariableType.Table)
            {
                //TODO: hack for now ! but id should be unified later  
                propertyKeys.Add(GremlinKeyword.NodeID);
            }
            else {
                throw new NotImplementedException($"Can't process this type {inputContext.PivotVariable.GetVariableType()} for now.");
            }

            inputContext.PivotVariable.Values(inputContext, propertyKeys);

            return inputContext;
        }
    }
}
