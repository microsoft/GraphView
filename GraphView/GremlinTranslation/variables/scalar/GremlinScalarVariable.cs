using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal abstract class GremlinScalarVariable : GremlinVariable
    {
        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Scalar;
        }

        internal override GremlinVariableProperty DefaultVariableProperty()
        {
            throw new NotImplementedException();
        }

        public virtual WScalarExpression ToScalarExpression()
        {
            throw new NotImplementedException();
        }
    }
}
