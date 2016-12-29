using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{ 
    internal class GremlinAddVVariable : GremlinVertexVariable
    {
        private static long _count = 0;

        public static string GetVariableName()
        {
            return "AddV_" + _count++;
        }

        public bool IsGenerateSql { get; set; }
        public Dictionary<string, object> Properties { get; set; }
        public string VertexLabel { get; set; }
        public WVariableReference Variable { get; set; }

        public GremlinAddVVariable(string vertexLabel)
        {
            SetVariableName = GetVariableName();
            VariableName = SetVariableName;

            Properties = new Dictionary<string, object>();
            VertexLabel = vertexLabel;
            Variable = GremlinUtil.GetVariableReference(SetVariableName);
            IsGenerateSql = false;
        }
    }
}
