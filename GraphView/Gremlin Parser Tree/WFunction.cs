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
                case (int)GraphViewGremlinParser.Keywords.has:
                    foreach (var alias in pContext.PrimaryInternalAlias)
                    {
                        index = pContext.PrimaryInternalAlias.IndexOf(alias);
                        string QuotedString = Parameters.Parameter[0].QuotedString;
                        QuotedString = QuotedString.Substring(1, QuotedString.Length - 2);
                        pContext.AliasPredicates[index] += alias + "." + QuotedString + " = " +
                                                           Parameters.Parameter[1].QuotedString;
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
                case (int)GraphViewGremlinParser.Keywords.values:
                    string ValuePara = Parameters.Parameter[0].QuotedString;
                    ValuePara = ValuePara.Substring(1, ValuePara.Length - 2);
                    for (int i = 0; i < pContext.PrimaryInternalAlias.Count; i++)
                        pContext.PrimaryInternalAlias[i] += "." + ValuePara;
                    break;
                default:
                    break;
            }

        }
    }
}
