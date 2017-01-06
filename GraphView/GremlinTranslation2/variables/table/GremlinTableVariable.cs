using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal abstract class GremlinTableVariable : GremlinVariable, ISqlTable
    {
        protected static int _count = 0;

        internal virtual string GenerateTableAlias()
        {
            return "R_" + _count++;
        }

        public List<string> ProjectedProperties { get; set; }

        public GremlinTableVariable()
        {
            ProjectedProperties = new List<string>();
        }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Table;
        }

        public virtual WTableReference ToTableReference()
        {
            throw new NotImplementedException();
        }

        internal override void Populate(string property)
        {
            if (!ProjectedProperties.Contains(property))
            {
                ProjectedProperties.Add(property);
            }
            if (!UsedProperties.Contains(property))
            {
                UsedProperties.Add(property);
            }
        }

        internal override void Range(GremlinToSqlContext currentContext, int low, int high)
        {
            Low = low;
            High = high;
        }
    }
}
