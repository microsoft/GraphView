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
            this.MetaProperties = metaProperties ?? new Dictionary<string, object>();
        }

        internal object ReplaceTraversalToContext(object value, GremlinToSqlContext propertyInputContext)
        {
            if (value is GraphTraversal)
            {
                GraphTraversal propertyTraversal = value as GraphTraversal;
                propertyTraversal.GetStartOp().InheritedVariableFromParent(propertyInputContext);
                return propertyTraversal.GetEndOp().GetContext();
            }
            return value;
        }

        internal GremlinProperty ToGremlinProperty(GremlinToSqlContext inputContext)
        {
            object value = this.ReplaceTraversalToContext(this.Value, inputContext);
            Dictionary<string, object> metaProperties = new Dictionary<string, object>();

            foreach (string metaKey in this.MetaProperties.Keys)
            {
                metaProperties[metaKey] = this.ReplaceTraversalToContext(this.MetaProperties[metaKey], inputContext);
            }

            return new GremlinProperty(this.Cardinality, this.Key, value, metaProperties);
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new TranslationException("The PivotVariable of property()-step can't be null.");
            }

            inputContext.PivotVariable.Property(inputContext, ToGremlinProperty(inputContext));

            return inputContext;
        }
    }
}
