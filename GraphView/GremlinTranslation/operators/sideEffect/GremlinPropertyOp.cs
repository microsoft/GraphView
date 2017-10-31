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
        public GremlinKeyword.PropertyCardinality Cardinality;
        public string Key;
        public object Value;
        public Dictionary<string, object> MetaProperties;

        public GremlinPropertyOp(GremlinKeyword.PropertyCardinality cardinality, string key, object value,
            Dictionary<string, object> metaProperties)
        {
            this.Cardinality = cardinality;
            this.Key = key;
            this.Value = value;
            this.MetaProperties = metaProperties;
        }

        internal object ReplaceTraversalToContext(object value, GremlinToSqlContext propertyInputContext)
        {
            if (value is GraphTraversal)
            {
                GraphTraversal propertyTraversal = value as GraphTraversal;
                return propertyTraversal.GetEndOp().GetContext();
            }
            return value;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new TranslationException("The PivotVariable of property()-step can't be null.");
            }

            GremlinToSqlContext propertyInputContext = inputContext;
            if (inputContext.PivotVariable is GremlinAddVVariable)
            {
                GremlinAddVVariable addVVariable = inputContext.PivotVariable as GremlinAddVVariable;
                propertyInputContext = addVVariable.InputContext;
            }
            else if (inputContext.PivotVariable is GremlinAddETableVariable)
            {
                GremlinAddETableVariable addEVariable = inputContext.PivotVariable as GremlinAddETableVariable;
                propertyInputContext = addEVariable.InputContext;
            }

            object value = this.ReplaceTraversalToContext(this.Value, propertyInputContext);
            Dictionary <string, object> metaProperties = new Dictionary<string, object>();

            foreach (string metaKey in this.MetaProperties.Keys)
            {
                metaProperties[metaKey] = this.ReplaceTraversalToContext(this.MetaProperties[metaKey], propertyInputContext);
            }

            inputContext.PivotVariable.Property(inputContext, new GremlinProperty(this.Cardinality, this.Key, value, metaProperties));

            return inputContext;
        }
    }
}
