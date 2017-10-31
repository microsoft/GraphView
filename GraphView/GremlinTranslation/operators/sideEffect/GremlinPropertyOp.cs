using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinPropertyOp: GremlinTranslationOperator
    {
        private GremlinProperty property;

        public GremlinPropertyOp(GremlinProperty property)
        {
            this.property = property;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new TranslationException("The PivotVariable of property()-step can't be null.");
            }

            if (property.Value is GraphTraversal)
            {
                GraphTraversal propertyTraversal = property.Value as GraphTraversal;
                if (inputContext.PivotVariable is GremlinAddVVariable)
                {
                    GremlinAddVVariable addVVariable = inputContext.PivotVariable as GremlinAddVVariable;
                    if (addVVariable.InputContext == null)
                    {
                        throw new TranslationException(
                            "The PivotVariable before addV()-step can't be null when there is a traversal in property-step()");
                    }
                    propertyTraversal.GetStartOp().InheritedVariableFromParent(addVVariable.InputContext);
                }
                else if (inputContext.PivotVariable is GremlinAddETableVariable)
                {
                    GremlinAddETableVariable addEVariable = inputContext.PivotVariable as GremlinAddETableVariable;
                    if (addEVariable.InputContext == null)
                    {
                        throw new TranslationException(
                            "The PivotVariable before addE()-step can't be null when there is a traversal in property-step()");
                    }
                    propertyTraversal.GetStartOp().InheritedVariableFromParent(addEVariable.InputContext);
                }
                else
                {
                    propertyTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
                }
                property.Value = propertyTraversal.GetEndOp().GetContext();
            }

            inputContext.PivotVariable.Property(inputContext, property);

            return inputContext;
        }
    }
}
