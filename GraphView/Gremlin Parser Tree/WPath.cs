namespace GraphView
{
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
