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
            string Parameter;
            List<string> NewPrimaryInternalAlias = new List<string>();
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
                case (int)GraphViewGremlinParser.Keywords.E:
                    SrcNode = "N_" + pContext.NodeCount.ToString();
                    pContext.InternalAliasList.Add(SrcNode);
                    pContext.AliasPredicates.Add("");
                    pContext.NodeCount++;
                    DestNode = "N_" + pContext.NodeCount.ToString();
                    pContext.InternalAliasList.Add(DestNode);
                    pContext.AliasPredicates.Add("");
                    pContext.NodeCount++;
                    Edge = "E_" + pContext.EdgeCount.ToString();
                    pContext.InternalAliasList.Add(Edge);
                    pContext.AliasPredicates.Add("");
                    pContext.EdgeCount++;
                    pContext.PrimaryInternalAlias.Clear();
                    pContext.PrimaryInternalAlias.Add(Edge);
                    pContext.Paths.Add((new Tuple<string, string, string>(SrcNode, Edge, DestNode)));
                    break;
                case (int)GraphViewGremlinParser.Keywords.next:
                    for (int i = 0; i < pContext.PrimaryInternalAlias.Count; i++)
                        pContext.PrimaryInternalAlias[i] += ".id";
                    break;
                case (int)GraphViewGremlinParser.Keywords.has:
                    foreach (var alias in pContext.PrimaryInternalAlias)
                    {
                        index = pContext.InternalAliasList.IndexOf(alias);
                        string QuotedString = Parameters.Parameter[0].QuotedString;
                        if (pContext.AliasPredicates[index] != "") pContext.AliasPredicates[index] += " AND ";
                        if (Parameters.Parameter[1].Function != null)
                        {
                            if (Parameters.Parameter[1].Function.KeywordIndex == (int)GraphViewGremlinParser.Keywords.lt)
                                pContext.AliasPredicates[index] += alias + "." + QuotedString + " < " +
                                   Parameters.Parameter[1].Function.Parameters.Parameter[0].Number.ToString();
                            if (Parameters.Parameter[1].Function.KeywordIndex == (int)GraphViewGremlinParser.Keywords.gt)
                                pContext.AliasPredicates[index] += alias + "." + QuotedString + " > " +
                                   Parameters.Parameter[1].Function.Parameters.Parameter[0].Number.ToString();
                            if (Parameters.Parameter[1].Function.KeywordIndex == (int)GraphViewGremlinParser.Keywords.eq)
                                pContext.AliasPredicates[index] += alias + "." + QuotedString + " = " +
                                   Parameters.Parameter[1].Function.Parameters.Parameter[0].Number.ToString();
                            if (Parameters.Parameter[1].Function.KeywordIndex == (int)GraphViewGremlinParser.Keywords.lte)
                                pContext.AliasPredicates[index] += alias + "." + QuotedString + " [ " +
                                   Parameters.Parameter[1].Function.Parameters.Parameter[0].Number.ToString();
                            if (Parameters.Parameter[1].Function.KeywordIndex == (int)GraphViewGremlinParser.Keywords.gte)
                                pContext.AliasPredicates[index] += alias + "." + QuotedString + " ] " +
                                   Parameters.Parameter[1].Function.Parameters.Parameter[0].Number.ToString();
                        }
                        else
                        {
                            pContext.AliasPredicates[index] += alias + "." + QuotedString + " = " +
                                                               Parameters.Parameter[1].QuotedString;
                        }
                    }
                    break;
                case (int)GraphViewGremlinParser.Keywords.Out:
                    foreach (var para in Parameters.Parameter)
                    {
                        Edge = "E_" + pContext.EdgeCount.ToString();
                        pContext.InternalAliasList.Add(Edge);
                        pContext.AliasPredicates.Add("");
                        pContext.EdgeCount++;
                        index = pContext.InternalAliasList.Count;
                        if (pContext.AliasPredicates[index - 1] != "") pContext.AliasPredicates[index] += " AND ";
                        pContext.AliasPredicates[index - 1] += Edge + ".type" + " = " +
                                                               para.QuotedString;
                        foreach (var alias in pContext.PrimaryInternalAlias)
                        {
                            DestNode = "N_" + pContext.NodeCount.ToString();
                            pContext.InternalAliasList.Add(DestNode);
                            pContext.AliasPredicates.Add("");
                            pContext.NodeCount++;
                            SrcNode = alias;
                            pContext.Paths.Add(new Tuple<string, string, string>(SrcNode, Edge,
                                DestNode));
                            NewPrimaryInternalAlias.Add(DestNode);
                        }
                    }
                    pContext.PrimaryInternalAlias.Clear();
                    foreach (var a in NewPrimaryInternalAlias)
                    {
                        pContext.PrimaryInternalAlias.Add(a);
                    }
                    break;
                case (int)GraphViewGremlinParser.Keywords.In:
                    foreach (var para in Parameters.Parameter)
                    {
                        Edge = "E_" + pContext.EdgeCount.ToString();
                        pContext.InternalAliasList.Add(Edge);
                        pContext.AliasPredicates.Add("");
                        pContext.EdgeCount++;
                        index = pContext.InternalAliasList.Count;
                        if (pContext.AliasPredicates[index - 1] != "") pContext.AliasPredicates[index] += " AND ";
                        pContext.AliasPredicates[index - 1] += Edge + ".type" + " = " +
                                                               para.QuotedString;
                        foreach (var alias in pContext.PrimaryInternalAlias)
                        {
                            SrcNode = "N_" + pContext.NodeCount.ToString();
                            pContext.InternalAliasList.Add(SrcNode);
                            pContext.AliasPredicates.Add("");
                            pContext.NodeCount++;
                            DestNode = alias;
                            pContext.Paths.Add(new Tuple<string, string, string>(SrcNode, Edge,
                                DestNode));
                            NewPrimaryInternalAlias.Add(SrcNode);
                        }
                    }
                    pContext.PrimaryInternalAlias.Clear();
                    foreach (var a in NewPrimaryInternalAlias)
                    {
                        pContext.PrimaryInternalAlias.Add(a);
                    }
                    break;
                case (int)GraphViewGremlinParser.Keywords.outE:
                    SrcNode = pContext.PrimaryInternalAlias[0];
                    DestNode = "N_" + pContext.NodeCount.ToString();
                    pContext.InternalAliasList.Add(DestNode);
                    pContext.AliasPredicates.Add("");
                    pContext.NodeCount++;
                    Edge = "E_" + pContext.EdgeCount.ToString();
                    pContext.InternalAliasList.Add(Edge);
                    if (Parameters != null)
                    {
                        Parameter = Parameters.Parameter[0].QuotedString;
                        pContext.AliasPredicates.Add(Edge + ".type = " +Parameter);
                    }
                    else pContext.AliasPredicates.Add("");
                    pContext.EdgeCount++;
                    pContext.PrimaryInternalAlias.Clear();
                    pContext.PrimaryInternalAlias.Add(Edge);
                    foreach (var alias in pContext.PrimaryInternalAlias)
                    {
                        Edge = alias;
                        pContext.Paths.Add(new Tuple<string, string, string>(SrcNode, Edge,
                             DestNode));
                    }
                    break;
                case (int)GraphViewGremlinParser.Keywords.inE:
                    DestNode = pContext.PrimaryInternalAlias[0];
                    SrcNode = "N_" + pContext.NodeCount.ToString();
                    pContext.InternalAliasList.Add(SrcNode);
                    pContext.AliasPredicates.Add("");
                    pContext.NodeCount++;
                    Edge = "E_" + pContext.EdgeCount.ToString();
                    pContext.InternalAliasList.Add(Edge);
                    if (Parameters != null)
                    {
                        Parameter = Parameters.Parameter[0].QuotedString;
                        pContext.AliasPredicates.Add(Edge + ".type = " + Parameter);
                    }
                    else pContext.AliasPredicates.Add("");
                    pContext.EdgeCount++;
                    pContext.PrimaryInternalAlias.Clear();
                    pContext.PrimaryInternalAlias.Add(Edge);
                    foreach (var alias in pContext.PrimaryInternalAlias)
                    {
                        Edge = alias;
                        pContext.Paths.Add(new Tuple<string, string, string>(SrcNode, Edge,
                             DestNode));
                    }
                    break;
                case (int)GraphViewGremlinParser.Keywords.inV:
                    SrcNode = "N_" + pContext.NodeCount.ToString();
                    pContext.InternalAliasList.Add(SrcNode);
                    pContext.AliasPredicates.Add("");
                    pContext.NodeCount++;
                    DestNode = "N_" + pContext.NodeCount.ToString();
                    pContext.InternalAliasList.Add(DestNode);
                    pContext.AliasPredicates.Add("");
                    pContext.NodeCount++;
                    Edge = pContext.PrimaryInternalAlias[0];
                    pContext.PrimaryInternalAlias.Clear();
                    pContext.PrimaryInternalAlias.Add(SrcNode);
                    pContext.Paths.Add(new Tuple<string, string, string>(SrcNode, Edge,
                             DestNode));
                    break;
                case (int)GraphViewGremlinParser.Keywords.outV:
                    SrcNode = "N_" + pContext.NodeCount.ToString();
                    pContext.InternalAliasList.Add(SrcNode);
                    pContext.AliasPredicates.Add("");
                    pContext.NodeCount++;
                    DestNode = "N_" + pContext.NodeCount.ToString();
                    pContext.InternalAliasList.Add(DestNode);
                    pContext.AliasPredicates.Add("");
                    pContext.NodeCount++;
                    Edge = pContext.PrimaryInternalAlias[0];
                    pContext.PrimaryInternalAlias.Clear();
                    pContext.PrimaryInternalAlias.Add(DestNode);
                    pContext.Paths.Add(new Tuple<string, string, string>(SrcNode, Edge,
         DestNode));
                    break;
                case (int)GraphViewGremlinParser.Keywords.As:
                    pContext.ExplictAliasToInternalAlias.Add(Parameters.Parameter[0].QuotedString, pContext.PrimaryInternalAlias[0]);
                    break;
                case (int)GraphViewGremlinParser.Keywords.@select:
                    pContext.PrimaryInternalAlias.Clear();
                    if (Parameters == null)
                    {
                        foreach (var a in pContext.ExplictAliasToInternalAlias)
                            pContext.PrimaryInternalAlias.Add(a.Value);
                    }
                    else
                    {
                        foreach (var a in Parameters.Parameter)
                            pContext.PrimaryInternalAlias.Add(pContext.ExplictAliasToInternalAlias[a.QuotedString]);
                    }
                    break;
                case (int)GraphViewGremlinParser.Keywords.addV:
                    foreach (var a in Parameters.Parameter.FindAll(p => Parameters.Parameter.IndexOf(p) % 2 == 0))
                    {
                        pContext.Properties.Add(a.QuotedString, Parameters.Parameter[Parameters.Parameter.IndexOf(a) + 1].QuotedString);
                    }
                    pContext.AddVMark = true;
                    break;
                case (int)GraphViewGremlinParser.Keywords.addOutE:
                    pContext.AddEMark = true;
                    SrcNode = Parameters.Parameter.First().QuotedString;
                    if (!pContext.ExplictAliasToInternalAlias.ContainsKey(SrcNode))
                    {
                        SrcNode = pContext.PrimaryInternalAlias[0];
                        DestNode = pContext.ExplictAliasToInternalAlias[Parameters.Parameter[1].QuotedString];
                        pContext.Properties.Add("type",Parameters.Parameter[0].QuotedString);
                        for (int i = 2; i < Parameters.Parameter.Count; i += 2)
                            pContext.Properties.Add(Parameters.Parameter[i].QuotedString,
                                Parameters.Parameter[i + 1].QuotedString);
                    }
                    else
                    {
                        SrcNode = pContext.ExplictAliasToInternalAlias[Parameters.Parameter[0].QuotedString];
                        DestNode = pContext.ExplictAliasToInternalAlias[Parameters.Parameter[2].QuotedString];
                        pContext.Properties.Add("type",Parameters.Parameter[1].QuotedString);
                        for (int i = 3; i < Parameters.Parameter.Count; i += 2)
                            pContext.Properties.Add(Parameters.Parameter[i].QuotedString,
                                Parameters.Parameter[i + 1].QuotedString);
                    }
                    pContext.PrimaryInternalAlias.Clear();
                    pContext.PrimaryInternalAlias.Add(SrcNode);
                    pContext.PrimaryInternalAlias.Add(DestNode);
                    break;
                case (int)GraphViewGremlinParser.Keywords.addInE:
                    pContext.AddEMark = true;
                    SrcNode = Parameters.Parameter.First().QuotedString;
                    if (!pContext.ExplictAliasToInternalAlias.ContainsKey(SrcNode))
                    {
                        DestNode = pContext.PrimaryInternalAlias[0];
                        SrcNode = Parameters.Parameter[1].QuotedString;
                        pContext.Properties.Add("type",Parameters.Parameter[0].QuotedString);
                        for (int i = 2; i < Parameters.Parameter.Count; i += 2)
                            pContext.Properties.Add(Parameters.Parameter[i].QuotedString,
                                Parameters.Parameter[i + 1].QuotedString);
                    }
                    else
                    {
                        SrcNode = pContext.ExplictAliasToInternalAlias[Parameters.Parameter[0].QuotedString];
                        DestNode = pContext.ExplictAliasToInternalAlias[Parameters.Parameter[2].QuotedString];
                        pContext.Properties.Add("type",Parameters.Parameter[1].QuotedString);
                        for (int i = 3; i < Parameters.Parameter.Count; i += 2)
                            pContext.Properties.Add(Parameters.Parameter[i].QuotedString,
                                Parameters.Parameter[i + 1].QuotedString);
                    }
                    pContext.PrimaryInternalAlias.Clear();
                    pContext.PrimaryInternalAlias.Add(SrcNode);
                    pContext.PrimaryInternalAlias.Add(DestNode);
                    break;
                case (int)GraphViewGremlinParser.Keywords.values:
                    string ValuePara = Parameters.Parameter[0].QuotedString;
                    for (int i = 0; i < pContext.PrimaryInternalAlias.Count; i++)
                        pContext.PrimaryInternalAlias[i] += "." + ValuePara;
                    break;
                default:
                    break;
            }

        }
    }
}
