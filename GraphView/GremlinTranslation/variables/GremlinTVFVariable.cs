using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslation
{
    internal class GremlinTVFVariable : GremlinVariable
    {
        private static long _count = 0;
        public static string GetVariableName()
        {
            return "TVF_" + _count++;
        }

        public WUnqualifiedJoin TableReference { get; set; }
        public GremlinVariableType VariableType;

        public GremlinTVFVariable(WUnqualifiedJoin tableReference)
        {
            VariableName = GetVariableName();
            TableReference = tableReference;
            (TableReference.SecondTableRef as WSchemaObjectFunctionTableReference).Alias =
                GremlinUtil.GetIdentifier(VariableName);
        }

        public override GremlinVariableType GetVariableType()
        {
            return VariableType;
        }
    }
}
