using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinGroupVariable : GremlinVariable2
    {
        public GremlinScalarVariable GroupbyKey { get; private set; }
        public GremlinScalarVariable AggregateValue { get; private set; }

        // To re-consider
        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Table;
        }

        //internal override void By(GremlinToSqlContext currentContext, GremlinToSqlContext byContext)
        //{
        //    // The BY step first sets the group-by key, and then sets the aggregation value.
        //    if (GroupbyKey == null)
        //    {
        //        GroupbyKey = new GremlinScalarSubquery(byContext);
        //    }
        //    else if (AggregateValue != null)
        //    {
        //        AggregateValue = new GremlinScalarSubquery(byContext);
        //    }
        //}

        internal override void By(GremlinToSqlContext currentContext, string name)
        {
            if (GroupbyKey == null)
            {
                currentContext.PivotVariable.Populate(name);
                GroupbyKey = new GremlinVariableProperty(currentContext.PivotVariable, name);
            }
            else if (AggregateValue != null)
            {
                currentContext.PivotVariable.Populate(name);
                AggregateValue = new GremlinVariableProperty(currentContext.PivotVariable, name);
            }
        }
    }
}
