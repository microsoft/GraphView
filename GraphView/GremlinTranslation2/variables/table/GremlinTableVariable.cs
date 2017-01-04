using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal abstract class GremlinTableVariable : GremlinVariable2, ISqlTable
    {
        protected static int _count = 0;

        internal virtual string GenerateTableAlias()
        {
            return "R_" + _count++;
        }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Table;
        }

        public virtual WTableReference ToTableReference()
        {
            throw new NotImplementedException();
        }

        protected List<string> projectedProperties = new List<string>();

        internal override void Populate(string property)
        {
            if (!projectedProperties.Contains(property))
            {
                projectedProperties.Add(property);
            }
            if (!UsedProperties.Contains(property))
            {
                UsedProperties.Add(property);
            }
        }

        internal override void Range(GremlinToSqlContext currentContext, long low, long high)
        {
            Low = low;
            High = high;
        }
    }
}
