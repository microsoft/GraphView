using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps
{
    internal class GremlinByOp : GremlinTranslationOperator
    {
        public string Key;
        public GremlinTranslationOperator ParamOp;

        public GremlinByOp() { }
        public GremlinByOp(string key)
        {
            Key = key;
        }
        public GremlinByOp(GremlinTranslationOperator paramOp)
        {
            ParamOp = paramOp;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            return inputContext;
        }
    }
}
