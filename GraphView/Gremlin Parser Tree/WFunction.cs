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
        internal WFragment Fragment;

        internal void AddNewAlias(string alias, ref GraphViewGremlinSematicAnalyser.Context context, string predicates ="")
        {
            context.InternalAliasList.Add(alias);
            context.AliasPredicates.Add(new List<string>());
            if (alias[0] == 'N') context.NodeCount++;
            else context.EdgeCount++;
            if (predicates != "")
                context.AliasPredicates.Last().Add(predicates);
        }

        internal void ChangePrimaryAlias(string alias, ref GraphViewGremlinSematicAnalyser.Context context)
        {
            context.PrimaryInternalAlias.Clear();
            context.PrimaryInternalAlias.Add(alias);
        }
        internal void Transform(ref GraphViewGremlinSematicAnalyser.Context pContext)
        {
            int index;
            string SrcNode;
            string DestNode;
            string Edge;
            string Parameter;
            List<string> StatueKeeper = new List<string>();
            List<string> NewPrimaryInternalAlias = new List<string>();
            switch (KeywordIndex)
            {
                case (int)GraphViewGremlinParser.Keywords.V:
                    SrcNode = "N_" + pContext.NodeCount.ToString();
                    AddNewAlias(SrcNode,ref pContext);
                    ChangePrimaryAlias(SrcNode,ref pContext);
                    break;
                case (int)GraphViewGremlinParser.Keywords.E:
                    SrcNode = "N_" + pContext.NodeCount.ToString();
                    AddNewAlias(SrcNode, ref pContext);
                    DestNode = "N_" + pContext.NodeCount.ToString();
                    AddNewAlias(DestNode, ref pContext);
                    Edge = "E_" + pContext.EdgeCount.ToString();
                    AddNewAlias(Edge, ref pContext);
                    ChangePrimaryAlias(Edge, ref pContext);
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
                        if (Parameters.Parameter[1].Fragment != null)
                        {
                            if (Parameters.Parameter[1].Fragment.Function.KeywordIndex == (int)GraphViewGremlinParser.Keywords.lt)
                                pContext.AliasPredicates[index].Add(alias + "." + QuotedString + " < " +
                                   Parameters.Parameter[1].Fragment.Function.Parameters.Parameter[0].Number.ToString());
                            if (Parameters.Parameter[1].Fragment.Function.KeywordIndex == (int)GraphViewGremlinParser.Keywords.gt)
                                pContext.AliasPredicates[index].Add(alias + "." + QuotedString + " > " +
                                   Parameters.Parameter[1].Fragment.Function.Parameters.Parameter[0].Number.ToString());
                            if (Parameters.Parameter[1].Fragment.Function.KeywordIndex == (int)GraphViewGremlinParser.Keywords.eq)
                                pContext.AliasPredicates[index].Add(alias + "." + QuotedString + " = " +
                                   Parameters.Parameter[1].Fragment.Function.Parameters.Parameter[0].Number.ToString());
                            if (Parameters.Parameter[1].Fragment.Function.KeywordIndex == (int)GraphViewGremlinParser.Keywords.lte)
                                pContext.AliasPredicates[index].Add(alias + "." + QuotedString + " [ " +
                                   Parameters.Parameter[1].Fragment.Function.Parameters.Parameter[0].Number.ToString());
                            if (Parameters.Parameter[1].Fragment.Function.KeywordIndex == (int)GraphViewGremlinParser.Keywords.gte)
                                pContext.AliasPredicates[index].Add(alias + "." + QuotedString + " ] " +
                                   Parameters.Parameter[1].Fragment.Function.Parameters.Parameter[0].Number.ToString());
                            if (Parameters.Parameter[1].Fragment.Function.KeywordIndex == (int)GraphViewGremlinParser.Keywords.neq)
                                pContext.AliasPredicates[index].Add(alias + "." + QuotedString + " ! " +
                                   Parameters.Parameter[1].Fragment.Function.Parameters.Parameter[0].Number.ToString());
                        }
                        else
                        {
                            pContext.AliasPredicates[index].Add(alias + "." + QuotedString + " = " +
                                                               Parameters.Parameter[1].QuotedString);
                        }
                    }
                    break;
                case (int)GraphViewGremlinParser.Keywords.Out:
                    foreach (var para in Parameters.Parameter)
                    {
                        Edge = "E_" + pContext.EdgeCount.ToString();
                        AddNewAlias(Edge,ref pContext);
                        index = pContext.InternalAliasList.Count;
                        pContext.AliasPredicates[index - 1].Add(Edge + ".type" + " = " +
                                                               para.QuotedString);
                        foreach (var alias in pContext.PrimaryInternalAlias)
                        {
                            DestNode = "N_" + pContext.NodeCount.ToString();
                            AddNewAlias(DestNode,ref pContext);
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
                        AddNewAlias(Edge,ref pContext);
                        index = pContext.InternalAliasList.Count;
                        pContext.AliasPredicates[index - 1].Add(Edge + ".type" + " = " +
                                                               para.QuotedString);
                        foreach (var alias in pContext.PrimaryInternalAlias)
                        {
                            SrcNode = "N_" + pContext.NodeCount.ToString();
                            AddNewAlias(SrcNode,ref pContext);
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
                    pContext.AliasPredicates.Add(new List<string>());
                    pContext.NodeCount++;
                    Edge = "E_" + pContext.EdgeCount.ToString();
                    pContext.InternalAliasList.Add(Edge);
                    if (Parameters != null)
                    {
                        Parameter = Parameters.Parameter[0].QuotedString;
                        pContext.AliasPredicates.Add(new List<string>());
                        pContext.AliasPredicates.Last().Add(Edge + ".type = " +Parameter);
                    }
                    else pContext.AliasPredicates.Add(new List<string>());
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
                    AddNewAlias(SrcNode,ref pContext);
                    Edge = "E_" + pContext.EdgeCount.ToString();
                    pContext.InternalAliasList.Add(Edge);
                    if (Parameters != null)
                    {
                        Parameter = Parameters.Parameter[0].QuotedString;
                        pContext.AliasPredicates.Add(new List<string>());
                        pContext.AliasPredicates.Last().Add(Edge + ".type = " + Parameter);
                    }
                    else pContext.AliasPredicates.Add(new List<string>());
                    pContext.EdgeCount++;
                    ChangePrimaryAlias(Edge,ref pContext);
                    foreach (var alias in pContext.PrimaryInternalAlias)
                    {
                        Edge = alias;
                        pContext.Paths.Add(new Tuple<string, string, string>(SrcNode, Edge,
                             DestNode));
                    }
                    break;
                case (int)GraphViewGremlinParser.Keywords.inV:
                    Edge = pContext.PrimaryInternalAlias[0];
                    var ExistInPath = pContext.Paths.Find(p => p.Item2 == Edge);
                    ChangePrimaryAlias(ExistInPath.Item3,ref pContext);
                    break;
                case (int)GraphViewGremlinParser.Keywords.outV:
                    Edge = pContext.PrimaryInternalAlias[0];
                    var ExistOutPath = pContext.Paths.Find(p => p.Item2 == Edge);
                    ChangePrimaryAlias(ExistOutPath.Item1, ref pContext);
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
                case (int)GraphViewGremlinParser.Keywords.@where:
                    foreach (var alias in pContext.PrimaryInternalAlias)
                    {
                        index = pContext.InternalAliasList.IndexOf(alias);
                        string QuotedString = Parameters.Parameter[0].QuotedString;
                        if (Parameters.Parameter[0].Fragment != null)
                        {
                            var CompId =
                                pContext.ExplictAliasToInternalAlias[
                                    Parameters.Parameter[0].Fragment.Function.Parameters.Parameter[0].QuotedString]+".id";
                            if (Parameters.Parameter[0].Fragment.Function.KeywordIndex ==
                                (int) GraphViewGremlinParser.Keywords.eq)
                                pContext.AliasPredicates[index].Add(alias + ".id = " + CompId);
                            if (Parameters.Parameter[0].Fragment.Function.KeywordIndex == (int)GraphViewGremlinParser.Keywords.neq)
                                pContext.AliasPredicates[index].Add(alias + ".id = " + CompId);
                        }
                    }
                    break;
                case (int)GraphViewGremlinParser.Keywords.match:
                    foreach (var x in pContext.PrimaryInternalAlias)
                    {
                        StatueKeeper.Add(x);
                    }
                    foreach (var piece in Parameters.Parameter)
                    {
                        if (piece.Fragment != null) piece.Fragment.Transform(ref pContext);
                        pContext.PrimaryInternalAlias.Clear();
                        foreach (var x in StatueKeeper)
                        {
                            pContext.PrimaryInternalAlias.Add(x);
                        }
                    }
                    break;
                default:
                    break;
            }

        }
    }
}
