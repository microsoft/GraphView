using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslation
{
    internal class GremlinVariableReference : GremlinVariable
    {
        public WVariableReference Variable { get; set; }
        public GremlinVariable RealGremlinVariable { get; set; }
        public WSetVariableStatement Statement { get; set; }

        public GremlinVariableReference(WSetVariableStatement statement)
        {

            Variable = statement.Variable;
            SetVariableName = Variable.Name;
            VariableName = Variable.Name.Substring(1, Variable.Name.Length - 1);
            Statement = statement;
        }
    }
}
