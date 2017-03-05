using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinSelectOp: GremlinTranslationOperator, IGremlinByModulating
    {
        public List<string> SelectKeys { get; set; }
        public GremlinKeyword.Pop Pop { get; set; }

        public GremlinSelectOp(GremlinKeyword.Pop pop, params string[] selectKeys)
        {
            SelectKeys = new List<string>(selectKeys);
            Pop = pop;
        }

        public GremlinSelectOp(params string[] selectKeys)
        {
            SelectKeys = new List<string>(selectKeys);
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            if (SelectKeys.Count == 1)
            {
                switch (Pop)
                {
                    case GremlinKeyword.Pop.Default:
                        inputContext.PivotVariable.Select(inputContext, SelectKeys.First());
                        break;
                    default:
                        inputContext.PivotVariable.Select(inputContext, Pop, SelectKeys.First());
                        break;
                }
            }
            else
            {
                switch (Pop)
                {
                    case GremlinKeyword.Pop.Default:
                        inputContext.PivotVariable.Select(inputContext, SelectKeys);
                        break;
                    default:
                        inputContext.PivotVariable.Select(inputContext, Pop, SelectKeys);
                        break;
                }
            }

            return inputContext;
        }
    }
}
