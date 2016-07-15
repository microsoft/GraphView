using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class WParameter : WSyntaxTree
    {
        internal string QuotedString { get; set; }
        internal int IdentifierIndex { get; set; }
        internal WFunction Function { get; set; }
    }
}
