using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslation
{
    internal class GremlinAddEVariable : GremlinVariable
    {
        public static string GetVariableName()
        {
            return "AddE_" + _count++;
        }

        private static long _count = 0;

        public GremlinVariable FromVariable { get; set; }
        public GremlinVariable ToVariable { get; set; }
        public Dictionary<string, object> Properties { get; set; }
        public string EdgeLabel { get; set; }
        public bool IsGenerateSql { get; set; }
        public WVariableReference Variable { get; set; }

        public GremlinAddEVariable(string edgeLabel, GremlinVariable currVariable)
        {
            SetVariableName = GetVariableName();
            VariableName = SetVariableName;
            Properties = new Dictionary<string, object>();
            FromVariable = currVariable;
            ToVariable = currVariable;
            EdgeLabel = edgeLabel;
            Variable = GremlinUtil.GetVariableReference(SetVariableName);
            IsGenerateSql = false;
        }
    }
}
