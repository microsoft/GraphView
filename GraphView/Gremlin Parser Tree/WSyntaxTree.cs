using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
