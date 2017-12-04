using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace GraphView
{
    internal sealed class PropertyTuple
    {
        internal GremlinKeyword.PropertyCardinality Cardinality { get; private set; }

        internal string Name { get; private set; }
        internal JValue Value { get; private set; }
        internal ScalarSubqueryFunction TraversalOp { get; private set; }

        internal Dictionary<string, Tuple<JValue, ScalarSubqueryFunction>> MetaProperties { get; private set; }

        internal PropertyTuple(
            GremlinKeyword.PropertyCardinality cardinality,
            string name,
            JValue value,
            Dictionary<string, Tuple<JValue, ScalarSubqueryFunction>> metaProperties = null)
        {
            this.Cardinality = cardinality;
            this.Name = name;
            this.Value = value;
            this.MetaProperties = metaProperties != null ? metaProperties : new Dictionary<string, Tuple<JValue, ScalarSubqueryFunction>>();
        }

        internal PropertyTuple(
            GremlinKeyword.PropertyCardinality cardinality,
            string name,
            ScalarSubqueryFunction traversalOp,
            Dictionary<string, Tuple<JValue, ScalarSubqueryFunction>> metaProperties = null)
        {
            this.Cardinality = cardinality;
            this.Name = name;
            this.TraversalOp = traversalOp;
            this.MetaProperties = metaProperties != null ? metaProperties : new Dictionary<string, Tuple<JValue, ScalarSubqueryFunction>>();
        }

        internal void AddMetaProperty(string metaName, JValue metaValue)
        {
            this.MetaProperties[metaName] = new Tuple<JValue, ScalarSubqueryFunction>(metaValue, null);
        }

        internal void AddMetaProperty(string metaName, ScalarSubqueryFunction traversalOp)
        {
            this.MetaProperties[metaName] = new Tuple<JValue, ScalarSubqueryFunction>(null, traversalOp);
        }

        internal JValue GetPropertyJValue(RawRecord record)
        {
            return Value ?? ((StringField)TraversalOp.Evaluate(record)).ToJValue();
        }

        internal JValue GetMetaPropertyJValue(string name, RawRecord record)
        {
            Debug.Assert(MetaProperties.ContainsKey(name));
            return MetaProperties[name].Item1 ?? ((StringField) MetaProperties[name].Item2.Evaluate(record)).ToJValue();
        }
    }

}
