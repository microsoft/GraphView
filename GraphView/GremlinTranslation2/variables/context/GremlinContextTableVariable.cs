using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinContextTableVariable: GremlinContextVariable
    {
        public GremlinContextTableVariable(GremlinVariable contextVariable): base(contextVariable) {}

        internal override void Properties(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            foreach (var property in propertyKeys)
            {
                Populate(property);
            }
            if (RealVariable is GremlinContextVariable)
            {
                RealVariable.Properties(currentContext, propertyKeys);
            }
            else
            {
                GremlinPropertiesVariable newVariable = new GremlinPropertiesVariable(RealVariable, propertyKeys);
                currentContext.VariableList.Add(newVariable);
                currentContext.TableReferences.Add(newVariable);
                currentContext.SetPivotVariable(newVariable);
            }
        }

        internal override void Values(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            foreach (var property in propertyKeys)
            {
                Populate(property);
            }
            if (RealVariable is GremlinContextVariable)
            {
                RealVariable.Values(currentContext, propertyKeys);
            }
            else
            {
                GremlinValuesVariable newVariable = new GremlinValuesVariable(RealVariable as GremlinTableVariable, propertyKeys);
                currentContext.VariableList.Add(newVariable);
                currentContext.TableReferences.Add(newVariable);
                currentContext.SetPivotVariable(newVariable);
            }
        }
    }
}
