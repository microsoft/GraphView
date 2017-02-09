using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinGhostTableVariable : GremlinGhostVariable
    {
        public GremlinGhostTableVariable(GremlinVariable ghostVariable, GremlinVariable attachedVariable, string label)
            : base(ghostVariable, attachedVariable, label) { }

        internal override void Properties(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            throw new NotImplementedException();
            //foreach (var property in propertyKeys)
            //{
            //    Populate(property);
            //}
            //GremlinPropertiesVariable newVariable = new GremlinPropertiesVariable(RealVariable as GremlinTableVariable, propertyKeys);
            //currentContext.VariableList.Add(newVariable);
            //currentContext.TableReferences.Add(newVariable);
            //currentContext.SetPivotVariable(newVariable);
        }

        internal override void Values(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            throw new NotImplementedException();
            //if (propertyKeys.Count == 1)
            //{
            //    Populate(propertyKeys.First());
            //    GremlinVariableProperty newVariableProperty = new GremlinVariableProperty(RealVariable as GremlinTableVariable, propertyKeys.First());
            //    currentContext.VariableList.Add(newVariableProperty);
            //    currentContext.SetPivotVariable(newVariableProperty);
            //}
            //else
            //{
            //    foreach (var property in propertyKeys)
            //    {
            //        Populate(property);
            //    }
            //    GremlinValuesVariable newVariable = new GremlinValuesVariable(RealVariable as GremlinTableVariable, propertyKeys);
            //    currentContext.VariableList.Add(newVariable);
            //    currentContext.TableReferences.Add(newVariable);
            //    currentContext.SetPivotVariable(newVariable);
            //}
        }

        internal override void Key(GremlinToSqlContext currentContext)
        {
            //GremlinKeyVariable newVariable = new GremlinKeyVariable(RealVariable.DefaultVariableProperty());
            //currentContext.VariableList.Add(newVariable);
            //currentContext.TableReferences.Add(newVariable);
            //currentContext.SetPivotVariable(newVariable);
        }
    }
}
