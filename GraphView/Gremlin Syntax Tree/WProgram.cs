using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class WProgram : WSyntaxTree
    {
        internal List<WPath> paths { get; set; }

        internal void Transform(ref GraphViewGremlinSematicAnalyser.Context pContext)
        {
            foreach(var path in paths) path.Transform(ref pContext);
        }
    }
}
