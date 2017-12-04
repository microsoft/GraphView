using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace GraphView
{
    [DataContract]
    internal sealed class PropertyTuple
    {
        [DataMember]
        internal GremlinKeyword.PropertyCardinality Cardinality { get; private set; }

        [DataMember]
        internal string Name { get; private set; }
        [DataMember]
        internal StringField Value { get; private set; }
        [DataMember]
        internal ScalarSubqueryFunction TraversalOp { get; private set; }

        [DataMember]
        internal Dictionary<string, Tuple<StringField, ScalarSubqueryFunction>> MetaProperties { get; private set; }

        internal PropertyTuple(
            GremlinKeyword.PropertyCardinality cardinality,
            string name,
            StringField value,
            Dictionary<string, Tuple<StringField, ScalarSubqueryFunction>> metaProperties = null)
        {
            this.Cardinality = cardinality;
            this.Name = name;
            this.Value = value;
            this.MetaProperties = metaProperties ?? new Dictionary<string, Tuple<StringField, ScalarSubqueryFunction>>();
        }

        internal PropertyTuple(
            GremlinKeyword.PropertyCardinality cardinality,
            string name,
            ScalarSubqueryFunction traversalOp,
            Dictionary<string, Tuple<StringField, ScalarSubqueryFunction>> metaProperties = null)
        {
            this.Cardinality = cardinality;
            this.Name = name;
            this.TraversalOp = traversalOp;
            this.MetaProperties = metaProperties ?? new Dictionary<string, Tuple<StringField, ScalarSubqueryFunction>>();
        }

        internal void AddMetaProperty(string metaName, StringField metaValue)
        {
            this.MetaProperties[metaName] = new Tuple<StringField, ScalarSubqueryFunction>(metaValue, null);
        }

        internal void AddMetaProperty(string metaName, ScalarSubqueryFunction traversalOp)
        {
            this.MetaProperties[metaName] = new Tuple<StringField, ScalarSubqueryFunction>(null, traversalOp);
        }

        internal JValue GetPropertyJValue(RawRecord record)
        {
            return Value?.ToJValue() ?? ((StringField)TraversalOp.Evaluate(record)).ToJValue();
        }

        internal JValue GetMetaPropertyJValue(string name, RawRecord record)
        {
            Debug.Assert(MetaProperties.ContainsKey(name));
            return MetaProperties[name].Item1?.ToJValue() ?? ((StringField) MetaProperties[name].Item2.Evaluate(record)).ToJValue();
        }
    }

}
