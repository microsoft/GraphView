﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal abstract class GremlinScalarVariable : GremlinVariable2, ISqlScalar
    {
        internal override GremlinScalarVariable DefaultProjection()
        {
            return this;
        }

        public virtual WSelectElement ToSelectElement()
        {
            throw new NotImplementedException();
        }

        public virtual WScalarExpression ToScalarExpression()
        {
            throw new NotImplementedException();
        }
    }
}