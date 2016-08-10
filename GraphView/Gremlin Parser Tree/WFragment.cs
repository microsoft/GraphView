using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView.TSQL_Syntax_Tree;

namespace GraphView
{
    internal class WFragment : WSyntaxTree
    {
        internal WFunction Function { get; set; }
        internal WFragment Fragment { get; set; }
        internal int Identifer { get; set; }

        internal void Transform(ref GraphViewGremlinSematicAnalyser.Context pContext)
        {
            var Identifiers = pContext.Identifiers;
            if (Identifer != -1)
                for (int i = 0; i < pContext.PrimaryInternalAlias.Count; i++)
                    pContext.PrimaryInternalAlias[i] += "." + Identifiers[Identifer];

            if (Fragment != null && Function != null &&
                Function.KeywordIndex == (int)GraphViewGremlinParser.Keywords.choose)
            {
                string OrigianlPath = pContext.PrimaryInternalAlias[0];
                Function.Parameters.Parameter[0].Fragment.Function.Transform(ref pContext);
                pContext.BranchNote = pContext.PrimaryInternalAlias[0];
                pContext.PrimaryInternalAlias[0] = OrigianlPath;
                pContext.ChooseMark = true;
                Fragment.Transform(ref pContext);
            }
            if (Function != null &&
    Function.KeywordIndex == (int)GraphViewGremlinParser.Keywords.coalesce)
            {
                foreach (var x in Function.Parameters.Parameter)
                {
                    GraphViewGremlinSematicAnalyser.Context branch = new GraphViewGremlinSematicAnalyser.Context(pContext);
                    x.Fragment.Transform(ref branch);
                    if (Fragment != null) Fragment.Transform(ref branch);
                    pContext.BranchContexts.Add(branch);
                }
            }
            if (Function != null && Function.KeywordIndex == (int)GraphViewGremlinParser.Keywords.option &&
                pContext.ChooseMark == true)
            {
                GraphViewGremlinSematicAnalyser.Context BranchContext =
                    new GraphViewGremlinSematicAnalyser.Context(pContext);
                BranchContext.ChooseMark = false;
                BranchContext.AliasPredicates.Last().Add(pContext.BranchNote + " = " + (Function.Parameters.Parameter[0].QuotedString == null
                        ? Function.Parameters.Parameter[0].Number.ToString()
                        : Function.Parameters.Parameter[0].QuotedString));
                Function.Parameters.Parameter[1].Fragment.Transform(ref BranchContext);
                Fragment.Transform(ref BranchContext);
                pContext.BranchContexts.Add(BranchContext);
                Fragment.Transform(ref pContext);
            }

            if (Fragment != null && Function != null && Function.KeywordIndex == (int)GraphViewGremlinParser.Keywords.repeat)
            {
                int times = 0;
                if (Fragment.Function.KeywordIndex == (int)GraphViewGremlinParser.Keywords.times)
                    times = (int)Fragment.Function.Parameters.Parameter.First().Number;
                for (int i = 0; i < times; i++)
                {
                    Function.Parameters.Parameter.First().Fragment.Transform(ref pContext);
                }
                if (Fragment.Fragment != null) Fragment.Fragment.Transform(ref pContext);
            }
            else if (Function != null && Function.KeywordIndex != (int)GraphViewGremlinParser.Keywords.choose  && pContext.ChooseMark == false && Function.KeywordIndex != (int)GraphViewGremlinParser.Keywords.coalesce)
            {
                if (Function != null) Function.Transform(ref pContext);
                if (Fragment != null) Fragment.Transform(ref pContext);
            }

        }
    }
}
