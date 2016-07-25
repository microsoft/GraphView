using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class WFragment : WSyntaxTree
    {
        internal WFunction Function { get; set; }
        internal WFragment Fragment { get; set; }
        internal int Identifer { get; set; }

        internal void Transform(ref GraphViewGremlinSematicAnalyser.Context pContext)
        {
            if (Fragment != null && Function != null && Function.KeywordIndex == (int) GraphViewGremlinParser.Keywords.repeat)
            {
                int times = 0;
                if (Fragment.Function.KeywordIndex == (int) GraphViewGremlinParser.Keywords.times)
                    times = (int)Fragment.Function.Parameters.Parameter.First().Number;
                for (int i = 0; i < times; i++)
                {
                    Function.Parameters.Parameter.First().Fragment.Transform(ref pContext);
                }
                if (Fragment.Fragment != null) Fragment.Fragment.Transform(ref pContext);
            }
            else
            {
                if (Function != null) Function.Transform(ref pContext);
                if (Fragment != null) Fragment.Transform(ref pContext);
            }
            var Identifiers = pContext.Identifiers;
            if (Identifer != -1)
                for (int i = 0; i<pContext.PrimaryInternalAlias.Count;i++)
                pContext.PrimaryInternalAlias[i] += "." + Identifiers[Identifer];
        }
    }
}
