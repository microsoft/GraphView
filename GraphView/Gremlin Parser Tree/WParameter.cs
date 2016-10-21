namespace GraphView
{
    internal class WParameter : WSyntaxTree
    {
        internal double Number { get; set; }
        internal string QuotedString { get; set; }
        internal int IdentifierIndex { get; set; }
        internal WFragment Fragment { get; set; }
    }
}
