using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinVertexVariable : GremlinVariable
    {
        private static long _count = 0;

        public static string GetVariableName()
        {
            return "N_" + _count++;
        }

        public GremlinVertexVariable()
        {
            VariableName = GetVariableName();
        }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Vertex;
        }

    }
    internal abstract class GremlinVertexVariable2 : GremlinTableVariable { }
}
