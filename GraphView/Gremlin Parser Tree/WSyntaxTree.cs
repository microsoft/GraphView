namespace GraphView
{
    class WSyntaxTree
    {
        internal int FirstToken { get; set; }
        internal int LastToken { get; set; }

        internal WSyntaxTree()
        {
            FirstToken = -1;
            LastToken = -1;
        }
    }
}
