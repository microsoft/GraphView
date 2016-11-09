using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinValuesOp: GremlinTranslationOperator
    {
        public List<string> PropertyKeys;

        public GremlinValuesOp(params string[] propertyKeys) {
            PropertyKeys = new List<string>();
            foreach (var propertyKey in propertyKeys)
            {
                PropertyKeys.Add(propertyKey);
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            inputContext.ClearProjection();
            foreach (var propertyKey in PropertyKeys) {
                inputContext.AddProjection(inputContext.CurrVariable, propertyKey);
            }

            return inputContext;
        }

    }
}
