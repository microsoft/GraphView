using System.Collections.Generic;

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
