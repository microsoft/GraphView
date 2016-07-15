using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class WFragment : WSyntaxTree
    {
        internal WFunction Function { get; set; }
        internal WFragment Fragment { get; set; }
        internal int Identifer { get; set; }

        internal void Transform(ref GraphViewGremlinSematicAnalyser.Context pContext)
        {
            if (Function != null) Function.Transform(ref pContext);
            if (Fragment != null) Fragment.Transform(ref pContext);
            var Identifiers = pContext.Identifiers;
            if (Identifer != -1)
                for (int i = 0; i<pContext.PrimaryInternalAlias.Count;i++)
                pContext.PrimaryInternalAlias[i] += "." + Identifiers[Identifer];

        }
    }
}
