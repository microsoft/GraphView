using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.sideEffect
{
    internal class GremlinPropertyOp: GremlinTranslationOperator
    {
        public Dictionary<string, string> Properties;

        public GremlinPropertyOp(params string[] properties)
        {
            if (properties.Length % 2 != 0) throw new Exception("The parameter of property should be even");
            if (properties.Length < 2) throw new Exception("The number of parameter of property should be larger than 2");
            Properties = new Dictionary<string, string>();
            for (int i = 0; i < properties.Length; i++)
            {
                Properties[properties[i]] = properties[i + 1];
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            if (inputContext.CurrVariable is GremlinAddEVariable)
            {

            }
            else
            {
                
            }

            return inputContext;
        }
    }
}
