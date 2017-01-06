using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal abstract class GremlinScalarVariable : GremlinVariable, ISqlScalar
    {
        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Scalar;
        }

        internal override GremlinScalarVariable DefaultProjection()
        {
            return this;
        }

        public virtual WScalarExpression ToScalarExpression()
        {
            throw new NotImplementedException();
        }
    }
}
