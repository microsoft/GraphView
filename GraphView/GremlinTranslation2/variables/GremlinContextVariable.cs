using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinContextVariable: GremlinVariable2
    {
        public GremlinVariable2 ContextVariable;

        public bool IsFromSelect;
        public GremlinKeyword.Pop Pop;
        public string SelectKey;

        public GremlinContextVariable(GremlinVariable2 contextVariable)
        {
            ContextVariable = contextVariable;
            VariableName = contextVariable.VariableName;
        }

        internal override GremlinScalarVariable DefaultProjection()
        {
            return ContextVariable.DefaultProjection();
        }

        internal override void Populate(string name, bool isAlias = false)
        {
            ContextVariable.Populate(name, isAlias);
        }

        internal override void Property(GremlinToSqlContext currentContext, Dictionary<string, object> properties)
        {
            ContextVariable.Property(currentContext, properties);
        }
    }
}
