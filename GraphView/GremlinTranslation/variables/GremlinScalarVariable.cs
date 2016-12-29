using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslation
{
    internal class GremlinScalarVariable2 : GremlinVariable
    {
        public GremlinVariable FromVariable { get; set; }
        public string Key { get; set; }

        public GremlinScalarVariable2(GremlinVariable variable, string key)
        {
            VariableName = variable.VariableName;
            FromVariable = variable;
            Key = key;
        }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Scalar;
        }
    }
}
