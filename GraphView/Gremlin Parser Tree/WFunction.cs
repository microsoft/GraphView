using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class WFunction : WSyntaxTree
    {
        internal int KeywordIndex;
        internal WParameters Parameters;
        internal WFragment Fragment;

        internal void AddNewAlias(string alias, ref GraphViewGremlinSematicAnalyser.Context context)
        {
            context.InternalAliasList.Add(alias);
            if (alias[0] == 'N') context.NodeCount++;
            else context.EdgeCount++;
        }

        internal void ChangePrimaryAlias(string alias, ref GraphViewGremlinSematicAnalyser.Context context)
        {
            context.PrimaryInternalAlias.Clear();
            context.PrimaryInternalAlias.Add(alias);
        }

        internal void AddNewPredicates(ref GraphViewGremlinSematicAnalyser.Context sink, WBooleanExpression source)
        {
            if (source != null && sink.AliasPredicates != null)
                sink.AliasPredicates = new WBooleanBinaryExpression()
                {
                    BooleanExpressionType = BooleanBinaryExpressionType.And,
                    FirstExpr = source,
                    SecondExpr = sink.AliasPredicates
                };
            if (source != null && sink.AliasPredicates == null)
                sink.AliasPredicates = source;
        }

        internal WBooleanExpression AddNewOrPredicates(List<WBooleanExpression> ListOfBoolean)
        {
            WBooleanExpression res = null;
            foreach (var x in ListOfBoolean)
            {
                if (x != null && res != null)
                    res = new WBooleanBinaryExpression()
                    {
                        BooleanExpressionType = BooleanBinaryExpressionType.Or,
                        FirstExpr = x,
                        SecondExpr = res
                    };
                if (x != null && res == null)
                    res = x;
            }
            return res;
        }
        internal WBooleanExpression AddNewAndPredicates(List<WBooleanExpression> ListOfBoolean)
        {
            WBooleanExpression res = null;
            foreach (var x in ListOfBoolean)
            {
                if (x != null && res != null)
                    res = new WBooleanBinaryExpression()
                    {
                        BooleanExpressionType = BooleanBinaryExpressionType.And,
                        FirstExpr = x,
                        SecondExpr = res
                    };
                if (x != null && res == null)
                    res = x;
            }
            return res;
        }
        internal WMultiPartIdentifier CutStringIntoMultiPartIdentifier(string identifier)
        {
            var MultiIdentifierList = new List<Identifier>();
            while (identifier.IndexOf('.') != -1)
            {
                int cutpoint = identifier.IndexOf('.');
                MultiIdentifierList.Add(new Identifier()
                {
                    Value = identifier.Substring(0, cutpoint)
                });
                identifier = identifier.Substring(cutpoint + 1,
                    identifier.Length - cutpoint - 1);
            }
            MultiIdentifierList.Add(new Identifier() { Value = identifier });
            return new WMultiPartIdentifier() {Identifiers = MultiIdentifierList };
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
            WBooleanExpression GeneratedBooleanExpression = null;
            switch (KeywordIndex)
            {
                case (int)GraphViewGremlinParser.Keywords.V:
                    SrcNode = "N_" + pContext.NodeCount.ToString();
                    AddNewAlias(SrcNode, ref pContext);
                    ChangePrimaryAlias(SrcNode, ref pContext);
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
                    SrcNode = "N_" + pContext.NodeCount.ToString();
                    AddNewAlias(SrcNode, ref pContext);
                    ChangePrimaryAlias(SrcNode, ref pContext);

                    foreach (var alias in pContext.PrimaryInternalAlias)
                    {
                        string QuotedString = Parameters.Parameter[0].QuotedString;
                        string PropertyName = alias + "." + QuotedString;
                        var MultiIdentifierName = CutStringIntoMultiPartIdentifier(PropertyName);
                        if (Parameters.Parameter[1].Fragment != null)
                        {
                            string PropertyValue =
                                Parameters.Parameter[1].Fragment.Function.Parameters.Parameter[0].Number.ToString();
                            var MultiIdentifierValue = CutStringIntoMultiPartIdentifier(PropertyValue);

                            WScalarExpression ValueExpression = null;
                            if (MultiIdentifierValue.Identifiers.Count > 1)
                                ValueExpression = new WColumnReferenceExpression()
                                {
                                    MultiPartIdentifier = MultiIdentifierValue
                                };
                            else
                            {
                                double temp;
                                if (double.TryParse(MultiIdentifierValue.Identifiers.First().Value, out temp))
                                    ValueExpression = new WValueExpression(MultiIdentifierValue.Identifiers.First().Value, false);
                                else
                                    ValueExpression = new WValueExpression(MultiIdentifierValue.Identifiers.First().Value, true);
                            }


                            switch (Parameters.Parameter[1].Fragment.Function.KeywordIndex)
                            {
                                case (int) GraphViewGremlinParser.Keywords.lt:
                                    GeneratedBooleanExpression = new WBooleanComparisonExpression()
                                    {
                                        ComparisonType = BooleanComparisonType.LessThan,
                                        FirstExpr =
                                            new WColumnReferenceExpression()
                                            {
                                                MultiPartIdentifier = MultiIdentifierName
                                            },
                                        SecondExpr = ValueExpression
                                    };
                                    break;
                                case (int) GraphViewGremlinParser.Keywords.gt:
                                    GeneratedBooleanExpression = new WBooleanComparisonExpression()
                                    {
                                        ComparisonType = BooleanComparisonType.GreaterThan,
                                        FirstExpr =
                                            new WColumnReferenceExpression()
                                            {
                                                MultiPartIdentifier = MultiIdentifierName
                                            },
                                        SecondExpr = ValueExpression
                                    };
                                    break;
                                case (int) GraphViewGremlinParser.Keywords.lte:
                                    GeneratedBooleanExpression = new WBooleanComparisonExpression()
                                    {
                                        ComparisonType = BooleanComparisonType.LessThanOrEqualTo,
                                        FirstExpr =
                                            new WColumnReferenceExpression()
                                            {
                                                MultiPartIdentifier = MultiIdentifierName
                                            },
                                        SecondExpr = ValueExpression
                                    };
                                    break;
                                case (int) GraphViewGremlinParser.Keywords.gte:
                                    GeneratedBooleanExpression = new WBooleanComparisonExpression()
                                    {
                                        ComparisonType = BooleanComparisonType.GreaterThanOrEqualTo,
                                        FirstExpr =
                                            new WColumnReferenceExpression()
                                            {
                                                MultiPartIdentifier = MultiIdentifierName
                                            },
                                        SecondExpr = ValueExpression
                                    };
                                    break;
                                case (int) GraphViewGremlinParser.Keywords.neq:
                                    GeneratedBooleanExpression = new WBooleanComparisonExpression()
                                    {
                                        ComparisonType = BooleanComparisonType.NotEqualToExclamation,
                                        FirstExpr =
                                            new WColumnReferenceExpression()
                                            {
                                                MultiPartIdentifier = MultiIdentifierName
                                            },
                                        SecondExpr = ValueExpression
                                    };
                                    break;
                                case (int)GraphViewGremlinParser.Keywords.within:
                                    List<WBooleanExpression> BooleanListOr = new List<WBooleanExpression>();
                                    foreach (var x in Parameters.Parameter[1].Fragment.Function.Parameters.Parameter)
                                    {
                                        if (x.QuotedString != null)
                                        {
                                            BooleanListOr.Add(new WBooleanComparisonExpression()
                                            {
                                                ComparisonType = BooleanComparisonType.Equals,
                                                FirstExpr =
                                            new WColumnReferenceExpression()
                                            {
                                                MultiPartIdentifier = MultiIdentifierName
                                            },
                                                SecondExpr = new WValueExpression(x.QuotedString,true)
                                            });
                                        }
                                    }
                                    GeneratedBooleanExpression = AddNewOrPredicates(BooleanListOr);
                                    break;
                                case (int)GraphViewGremlinParser.Keywords.without:
                                    List<WBooleanExpression> BooleanListAnd = new List<WBooleanExpression>();
                                    foreach (var x in Parameters.Parameter[1].Fragment.Function.Parameters.Parameter)
                                    {
                                        if (x.QuotedString != null)
                                        {
                                            BooleanListAnd.Add(new WBooleanComparisonExpression()
                                            {
                                                ComparisonType = BooleanComparisonType.NotEqualToExclamation,
                                                FirstExpr =
                                            new WColumnReferenceExpression()
                                            {
                                                MultiPartIdentifier = MultiIdentifierName
                                            },
                                                SecondExpr = new WValueExpression(x.QuotedString, true)
                                            });
                                        }
                                    }
                                    GeneratedBooleanExpression = AddNewAndPredicates(BooleanListAnd);
                                    break;
                            }
                        }
                        else
                        {
                            WScalarExpression ValueExpression = null;
                            if (Parameters.Parameter[1].QuotedString != null)
                                ValueExpression = new WValueExpression(Parameters.Parameter[1].QuotedString, true);
                            else
                                ValueExpression = new WValueExpression(Parameters.Parameter[1].Number.ToString(), true);
                            GeneratedBooleanExpression = new WBooleanComparisonExpression()
                            {
                                ComparisonType = BooleanComparisonType.Equals,
                                FirstExpr =
                                    new WColumnReferenceExpression()
                                    {
                                        MultiPartIdentifier = MultiIdentifierName
                                    },
                                SecondExpr = ValueExpression
                            };
                        }
                        if(GeneratedBooleanExpression != null)
                        AddNewPredicates(ref pContext, GeneratedBooleanExpression);
                    }
                    break;
                case (int)GraphViewGremlinParser.Keywords.Out:
                    if (Parameters == null)
                    {
                        Edge = "E_" + pContext.EdgeCount.ToString();
                        AddNewAlias(Edge, ref pContext);
                        index = pContext.InternalAliasList.Count;
                        foreach (var alias in pContext.PrimaryInternalAlias)
                        {
                            DestNode = "N_" + pContext.NodeCount.ToString();
                            AddNewAlias(DestNode, ref pContext);
                            SrcNode = alias;
                            pContext.Paths.Add(new Tuple<string, string, string>(SrcNode, Edge,
                                DestNode));
                            NewPrimaryInternalAlias.Add(DestNode);
                        }
                    }
                    else
                        foreach (var para in Parameters.Parameter)
                        {
                            Edge = "E_" + pContext.EdgeCount.ToString();
                            AddNewAlias(Edge, ref pContext);
                            index = pContext.InternalAliasList.Count;
                            GeneratedBooleanExpression = new WBooleanComparisonExpression()
                            {
                                ComparisonType = BooleanComparisonType.Equals,
                                FirstExpr =
                                    new WColumnReferenceExpression()
                                    {
                                        MultiPartIdentifier =CutStringIntoMultiPartIdentifier(Edge + ".type")
                                    },
                                SecondExpr = new WValueExpression(para.QuotedString,true)
                            };
                            AddNewPredicates(ref pContext,GeneratedBooleanExpression);
                            foreach (var alias in pContext.PrimaryInternalAlias)
                            {
                                DestNode = "N_" + pContext.NodeCount.ToString();
                                AddNewAlias(DestNode, ref pContext);
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
                    if (Parameters == null)
                    {
                        Edge = "E_" + pContext.EdgeCount.ToString();
                        AddNewAlias(Edge, ref pContext);
                        index = pContext.InternalAliasList.Count;
                        foreach (var alias in pContext.PrimaryInternalAlias)
                        {
                            SrcNode = "N_" + pContext.NodeCount.ToString();
                            AddNewAlias(SrcNode, ref pContext);
                            DestNode = alias;
                            pContext.Paths.Add(new Tuple<string, string, string>(SrcNode, Edge,
                                DestNode));
                            NewPrimaryInternalAlias.Add(SrcNode);
                        }
                    }
                    else
                        foreach (var para in Parameters.Parameter)
                        {
                            Edge = "E_" + pContext.EdgeCount.ToString();
                            AddNewAlias(Edge, ref pContext);
                            index = pContext.InternalAliasList.Count;
                            GeneratedBooleanExpression = new WBooleanComparisonExpression()
                            {
                                ComparisonType = BooleanComparisonType.Equals,
                                FirstExpr =
                                    new WColumnReferenceExpression()
                                    {
                                        MultiPartIdentifier = CutStringIntoMultiPartIdentifier(Edge + ".type")
                                    },
                                SecondExpr = new WValueExpression(para.QuotedString, true)
                            };
                            AddNewPredicates(ref pContext, GeneratedBooleanExpression);
                            foreach (var alias in pContext.PrimaryInternalAlias)
                            {
                                SrcNode = "N_" + pContext.NodeCount.ToString();
                                AddNewAlias(SrcNode, ref pContext);
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
                    pContext.NodeCount++;
                    Edge = "E_" + pContext.EdgeCount.ToString();
                    pContext.InternalAliasList.Add(Edge);
                    if (Parameters != null)
                    {
                        Parameter = Parameters.Parameter[0].QuotedString;
                        GeneratedBooleanExpression = new WBooleanComparisonExpression()
                        {
                            ComparisonType = BooleanComparisonType.Equals,
                            FirstExpr =
                                new WColumnReferenceExpression()
                                {
                                    MultiPartIdentifier = CutStringIntoMultiPartIdentifier(Edge + ".type")
                                },
                            SecondExpr = new WValueExpression(Parameter, true)
                        };
                        AddNewPredicates(ref pContext, GeneratedBooleanExpression);
                    }
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
                    AddNewAlias(SrcNode, ref pContext);
                    Edge = "E_" + pContext.EdgeCount.ToString();
                    pContext.InternalAliasList.Add(Edge);
                    if (Parameters != null)
                    {
                        Parameter = Parameters.Parameter[0].QuotedString;
                        GeneratedBooleanExpression = new WBooleanComparisonExpression()
                        {
                            ComparisonType = BooleanComparisonType.Equals,
                            FirstExpr =
                                new WColumnReferenceExpression()
                                {
                                    MultiPartIdentifier = CutStringIntoMultiPartIdentifier(Edge + ".type")
                                },
                            SecondExpr = new WValueExpression(Parameter, true)
                        };
                        AddNewPredicates(ref pContext, GeneratedBooleanExpression);
                    }
                    pContext.EdgeCount++;
                    ChangePrimaryAlias(Edge, ref pContext);
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
                    ChangePrimaryAlias(ExistInPath.Item3, ref pContext);
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
                        pContext.Properties.Add("type", Parameters.Parameter[0].QuotedString);
                        for (int i = 2; i < Parameters.Parameter.Count; i += 2)
                            pContext.Properties.Add(Parameters.Parameter[i].QuotedString,
                                Parameters.Parameter[i + 1].QuotedString);
                    }
                    else
                    {
                        SrcNode = pContext.ExplictAliasToInternalAlias[Parameters.Parameter[0].QuotedString];
                        DestNode = pContext.ExplictAliasToInternalAlias[Parameters.Parameter[2].QuotedString];
                        pContext.Properties.Add("type", Parameters.Parameter[1].QuotedString);
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
                        pContext.Properties.Add("type", Parameters.Parameter[0].QuotedString);
                        for (int i = 2; i < Parameters.Parameter.Count; i += 2)
                            pContext.Properties.Add(Parameters.Parameter[i].QuotedString,
                                Parameters.Parameter[i + 1].QuotedString);
                    }
                    else
                    {
                        SrcNode = pContext.ExplictAliasToInternalAlias[Parameters.Parameter[0].QuotedString];
                        DestNode = pContext.ExplictAliasToInternalAlias[Parameters.Parameter[2].QuotedString];
                        pContext.Properties.Add("type", Parameters.Parameter[1].QuotedString);
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
                        index = pContext.InternalAliasList.IndexOf(alias.IndexOf('.') == -1 ? alias : alias.Substring(0, alias.IndexOf('.')));
                        string QuotedString = Parameters.Parameter[0].QuotedString;
                        if (Parameters.Parameter[0].Fragment != null)
                        {
                            string Comp1 = alias;
                            string Comp2 = "";
                            if (Comp1.IndexOf('.') == -1)
                                Comp1 += ".id";
                            if (Comp2.IndexOf('.') == -1)
                                Comp2 = pContext.ExplictAliasToInternalAlias[
                                    Parameters.Parameter[0].Fragment.Function.Parameters.Parameter[0].QuotedString];
                            else
                                Comp2 = pContext.ExplictAliasToInternalAlias[
        Parameters.Parameter[0].Fragment.Function.Parameters.Parameter[0].QuotedString] + ".id";
                            if (Parameters.Parameter[0].Fragment.Function.KeywordIndex ==
                                (int) GraphViewGremlinParser.Keywords.eq)
                            {
                                GeneratedBooleanExpression = new WBooleanComparisonExpression()
                                {
                                    ComparisonType = BooleanComparisonType.NotEqualToExclamation,
                                    FirstExpr =
                                        new WColumnReferenceExpression()
                                        {
                                            MultiPartIdentifier = CutStringIntoMultiPartIdentifier(Comp1)
                                        },
                                    SecondExpr = new WColumnReferenceExpression()
                                    {
                                        MultiPartIdentifier = CutStringIntoMultiPartIdentifier(Comp2)
                                    }
                                };
                                AddNewPredicates(ref pContext, GeneratedBooleanExpression);
                            }
                            if (Parameters.Parameter[0].Fragment.Function.KeywordIndex ==
                                (int) GraphViewGremlinParser.Keywords.neq)
                            {
                                GeneratedBooleanExpression = new WBooleanComparisonExpression()
                                {
                                    ComparisonType = BooleanComparisonType.NotEqualToExclamation,
                                    FirstExpr =
                                        new WColumnReferenceExpression()
                                        {
                                            MultiPartIdentifier = CutStringIntoMultiPartIdentifier(Comp1)
                                        },
                                    SecondExpr = new WColumnReferenceExpression()
                                    {
                                        MultiPartIdentifier = CutStringIntoMultiPartIdentifier(Comp2)
                                    }
                                };
                                AddNewPredicates(ref pContext, GeneratedBooleanExpression);
                            }
                        }
                    }
                    break;
                case (int)GraphViewGremlinParser.Keywords.match:
                    StatueKeeper.Clear();
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
                case (int)GraphViewGremlinParser.Keywords.aggregate:
                    pContext.ExplictAliasToInternalAlias.Add(Parameters.Parameter[0].QuotedString, pContext.PrimaryInternalAlias[0]);
                    break;
                case (int)GraphViewGremlinParser.Keywords.and:
                    foreach (var piece in Parameters.Parameter)
                        if (piece.Fragment != null) piece.Fragment.Transform(ref pContext);
                    break;
                case (int)GraphViewGremlinParser.Keywords.or:
                    StatueKeeper.Clear();
                    foreach (var x in pContext.PrimaryInternalAlias)
                    {
                        StatueKeeper.Add(x);
                    }
                    NewPrimaryInternalAlias.Clear();
                    foreach (var piece in Parameters.Parameter)
                    {
                        if (piece.Fragment != null) piece.Fragment.Transform(ref pContext);
                        foreach (var x in pContext.PrimaryInternalAlias)
                        {
                            NewPrimaryInternalAlias.Add(x);
                        }
                        pContext.PrimaryInternalAlias.Clear();
                        foreach (var x in StatueKeeper)
                        {
                            pContext.PrimaryInternalAlias.Add(x);
                        }
                    }
                    foreach (var x in NewPrimaryInternalAlias)
                    {
                        pContext.PrimaryInternalAlias.Add(x);
                    }
                    break;
                case (int)GraphViewGremlinParser.Keywords.drop:
                    pContext.RemoveMark = true;
                    break;
                case (int)GraphViewGremlinParser.Keywords.limit:
                    if (Parameters.Parameter[0] != null)
                        pContext.limit = (int)Parameters.Parameter[0].Number;
                    break;
                case (int)GraphViewGremlinParser.Keywords.order:
                    pContext.ByWhat = new WOrderByClause() {OrderByElements = new List<WExpressionWithSortOrder>()};
                    pContext.ByWhat.OrderByElements.Add(new WExpressionWithSortOrder() { ScalarExpr = new WValueExpression(pContext.PrimaryInternalAlias[0], false), SortOrder = SortOrder.NotSpecified });
                    pContext.OrderMark = true;
                    break;
                case (int)GraphViewGremlinParser.Keywords.by:
                    SortOrder order = SortOrder.NotSpecified;
                    if (Parameters.Parameter.Last().Fragment != null)
                    {
                        if(Parameters.Parameter.Last().Fragment.Function.KeywordIndex ==
                        (int) GraphViewGremlinParser.Keywords.decr)
                            order = SortOrder.Descending;
                        if (Parameters.Parameter.Last().Fragment.Function.KeywordIndex ==
  (int)GraphViewGremlinParser.Keywords.incr)
                            order = SortOrder.Ascending;
                    }
                    foreach (var x in Parameters.Parameter)
                        {
                        if(x.QuotedString != null)
                            pContext.ByWhat.OrderByElements.Add(new WExpressionWithSortOrder() {ScalarExpr = new WValueExpression(x.QuotedString,false),SortOrder = order});
                        }
                    pContext.OrderMark = false;
                    break;
                default:
                    break;
            }

        }
    }
}
