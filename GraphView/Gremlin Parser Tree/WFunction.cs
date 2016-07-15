using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class WFunction : WSyntaxTree
    {
        internal int KeywordIndex;
        internal WParameters Parameters;

        internal void Transform(ref GraphViewGremlinSematicAnalyser.Context pContext)
        {
            int index;
            string SrcNode;
            string DestNode;
            string Edge;
            switch (KeywordIndex)
            {
                case (int)GraphViewGremlinParser.Keywords.V:
                    SrcNode = "N_" + pContext.NodeCount.ToString();
                    pContext.PrimaryInternalAlias.Clear();
                    pContext.PrimaryInternalAlias.Add(SrcNode);
                    pContext.InternalAliasList.Add(SrcNode);
                    pContext.AliasPredicates.Add("");
                    pContext.NodeCount++;
                    break;
                case (int)GraphViewGremlinParser.Keywords.has:
                    foreach (var alias in pContext.PrimaryInternalAlias)
                    {
                        index = pContext.PrimaryInternalAlias.IndexOf(alias);
                        pContext.AliasPredicates[index] += alias + "." + Parameters.Parameter[0].QuotedString + " = " +
                                                           Parameters.Parameter[1].QuotedString;
                    }
                    break;
                case (int)GraphViewGremlinParser.Keywords.Out:
                    Edge = "E_" + pContext.EdgeCount.ToString();
                    pContext.InternalAliasList.Add(Edge);
                    pContext.AliasPredicates.Add("");
                    pContext.EdgeCount++;
                    index = pContext.InternalAliasList.Count;
                    pContext.AliasPredicates[index - 1] += Edge + ".type" + " = " +
                                                       Parameters.Parameter[0].QuotedString;
                    DestNode = "N_" + pContext.NodeCount.ToString();
                    foreach (var alias in pContext.PrimaryInternalAlias)
                    {
                        SrcNode = alias;
                        pContext.Paths.Add(new Tuple<string, string, string>(SrcNode, pContext.InternalAliasList.Last(),
                             DestNode));
                    }

                    pContext.PrimaryInternalAlias.Clear();
                    pContext.PrimaryInternalAlias.Add(DestNode);
                    pContext.InternalAliasList.Add(DestNode);
                    pContext.AliasPredicates.Add("");
                    pContext.NodeCount++;
                    break;
                case (int)GraphViewGremlinParser.Keywords.In:
                    Edge = "E_" + pContext.EdgeCount.ToString();
                    pContext.InternalAliasList.Add(Edge);
                    pContext.AliasPredicates.Add("");
                    pContext.EdgeCount++;
                    index = pContext.InternalAliasList.Count;
                    pContext.AliasPredicates[index - 1] += Edge + ".type" + " = " +
                                                       Parameters.Parameter[0].QuotedString;
                    DestNode = "N_" + pContext.NodeCount.ToString();
                    foreach (var alias in pContext.PrimaryInternalAlias)
                    {
                        SrcNode = alias;
                        pContext.Paths.Add(new Tuple<string, string, string>(DestNode, pContext.InternalAliasList.Last(),
                             SrcNode));
                    }

                    pContext.PrimaryInternalAlias.Clear();
                    pContext.PrimaryInternalAlias.Add(DestNode);
                    pContext.InternalAliasList.Add(DestNode);
                    pContext.AliasPredicates.Add("");
                    pContext.NodeCount++;
                    break;
                case (int)GraphViewGremlinParser.Keywords.As:
                    pContext.ExplictAliasToInternalAlias.Add(pContext.PrimaryInternalAlias[0], Parameters.Parameter[0].QuotedString);
                    break;
                case (int)GraphViewGremlinParser.Keywords.@select:
                    foreach (var a in Parameters.Parameter)
                    {
                        pContext.PrimaryInternalAlias.Clear();
                        pContext.PrimaryInternalAlias.Add(pContext.ExplictAliasToInternalAlias[a.QuotedString]);
                    }
                    break;
                case (int)GraphViewGremlinParser.Keywords.addV:
                    foreach (var a in Parameters.Parameter.FindAll(p => Parameters.Parameter.IndexOf(p) % 2 == 0))
                    {

                    }
                    break;
                case (int)GraphViewGremlinParser.Keywords.addOutE:
                    DestNode = Parameters.Parameter.First().QuotedString;
                    foreach (var a in Parameters.Parameter.GetRange(1, Parameters.Parameter.Count - 1).FindAll(p => Parameters.Parameter.IndexOf(p) % 2 == 0))
                    {
                        pContext.Properties.Add(a.QuotedString, Parameters.Parameter[Parameters.Parameter.IndexOf(a) + 1].QuotedString);
                    }
                    pContext.AddEMark = true;
                    break;
                case (int)GraphViewGremlinParser.Keywords.addInE:
                    SrcNode = Parameters.Parameter.First().QuotedString;
                    foreach (var a in Parameters.Parameter.GetRange(1, Parameters.Parameter.Count - 1).FindAll(p => Parameters.Parameter.IndexOf(p) % 2 == 0))
                    {
                        pContext.Properties.Add(a.QuotedString, Parameters.Parameter[Parameters.Parameter.IndexOf(a) + 1].QuotedString);
                    }
                    pContext.AddVMark = true;
                    break;
            }

        }
    }
}
