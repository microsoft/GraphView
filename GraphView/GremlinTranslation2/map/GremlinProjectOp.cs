using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinProjectOp: GremlinTranslationOperator
    {
        public List<string> ProjectKeys { get; set; }

        public GremlinProjectOp(params string[] projectKeys)
        {
            ProjectKeys = new List<string>(projectKeys);
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            inputContext.PivotVariable.Project(inputContext, ProjectKeys);

            return inputContext;
        }
    }
}
