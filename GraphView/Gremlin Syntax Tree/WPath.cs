using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using GraphView;

namespace GraphView { 
    internal class WPath : WSyntaxTree
    {
        internal int IdentifierIndex { get; set; }
        internal WFragment Fragment { get; set; }

        internal void Transform(ref GraphViewGremlinSematicAnalyser.Context pContext)
        {
            if (Fragment != null) Fragment.Transform(ref pContext);
        }
    }
}
