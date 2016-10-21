using System;
using System.Collections.Generic;
using System.Linq;
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

        internal void AddNewPrimaryAlias(string alias, ref GraphViewGremlinSematicAnalyser.Context context)
        {
            context.PrimaryInternalAlias.Add(new WColumnReferenceExpression() { MultiPartIdentifier = CutStringIntoMultiPartIdentifier(alias)});
        }
        internal void ChangePrimaryAlias(string alias, ref GraphViewGremlinSematicAnalyser.Context context)
        {
            context.PrimaryInternalAlias.Clear();
            context.PrimaryInternalAlias.Add(new WColumnReferenceExpression() {MultiPartIdentifier = CutStringIntoMultiPartIdentifier(alias) });
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
            List<WScalarExpression> StatueKeeper = new List<WScalarExpression>();
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
                        if(pContext.PrimaryInternalAlias[i] is WColumnReferenceExpression)
                        (pContext.PrimaryInternalAlias[i] as WColumnReferenceExpression).MultiPartIdentifier.Identifiers.Add(new Identifier() {Value = "id"});
                    break;
                case (int)GraphViewGremlinParser.Keywords.has:
                    SrcNode = "N_" + pContext.PrimaryInternalAlias[0];

                    foreach (var alias in pContext.PrimaryInternalAlias)
                    {
                        string QuotedString = Parameters.Parameter[0].QuotedString;
                        string PropertyName = alias + "." + QuotedString;
                        var MultiIdentifierName = CutStringIntoMultiPartIdentifier(PropertyName);
                        if (Parameters.Parameter[1].Fragment != null)
                        {
                            string PropertyValue = "";
                            WMultiPartIdentifier MultiIdentifierValue = null;
                            WScalarExpression ValueExpression = null;
                            if (!double.IsNaN(Parameters.Parameter[1].Fragment.Function.Parameters.Parameter[0].Number))
                            {
                                PropertyValue =
                                    Parameters.Parameter[1].Fragment.Function.Parameters.Parameter[0].Number.ToString();
                                MultiIdentifierValue = CutStringIntoMultiPartIdentifier(PropertyValue);
                                ValueExpression = new WValueExpression(MultiIdentifierValue.Identifiers.First().Value, false);
                            }
                            else
                            {
                                PropertyValue =
                             Parameters.Parameter[1].Fragment.Function.Parameters.Parameter[0].QuotedString;
                                MultiIdentifierValue = CutStringIntoMultiPartIdentifier(PropertyValue);
                                if (MultiIdentifierValue.Identifiers.Count > 1)
                                    ValueExpression = new WColumnReferenceExpression()
                                    {
                                        MultiPartIdentifier = MultiIdentifierValue
                                    };
                                else ValueExpression = new WValueExpression(MultiIdentifierValue.Identifiers.First().Value, true);
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
                                case (int)GraphViewGremlinParser.Keywords.eq:
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
                            if (alias is WColumnReferenceExpression)
                            {
                                DestNode = "N_" + pContext.NodeCount.ToString();
                                AddNewAlias(DestNode, ref pContext);
                                SrcNode = alias.ToString();
                                pContext.Paths.Add(new Tuple<string, string, string>(SrcNode, Edge,
                                    DestNode));
                                NewPrimaryInternalAlias.Add(DestNode);
                            }
                        }
                    }
                    else
                        foreach (var para in Parameters.Parameter)
                        {
                            Edge = "E_" + pContext.EdgeCount.ToString();
                            AddNewAlias(Edge, ref pContext);
                            index = pContext.InternalAliasList.Count;
                            WValueExpression predicatesValue = null;
                            if (para.QuotedString != null)
                                predicatesValue = new WValueExpression(para.QuotedString, true);
                            else
                                predicatesValue = new WValueExpression(para.Number.ToString(), false);
                            GeneratedBooleanExpression = new WBooleanComparisonExpression()
                            {
                                ComparisonType = BooleanComparisonType.Equals,
                                FirstExpr =
                                    new WColumnReferenceExpression()
                                    {
                                        MultiPartIdentifier =CutStringIntoMultiPartIdentifier(Edge + ".type")
                                    },
                                SecondExpr = predicatesValue
                            };
                            AddNewPredicates(ref pContext,GeneratedBooleanExpression);
                            foreach (var alias in pContext.PrimaryInternalAlias)
                            {
                                if (alias is WColumnReferenceExpression)
                                {
                                    DestNode = "N_" + pContext.NodeCount.ToString();
                                    AddNewAlias(DestNode, ref pContext);
                                    SrcNode = alias.ToString();
                                    pContext.Paths.Add(new Tuple<string, string, string>(SrcNode, Edge,
                                        DestNode));
                                    NewPrimaryInternalAlias.Add(DestNode);
                                }
                            }
                        }
                    pContext.PrimaryInternalAlias.Clear();
                    foreach (var a in NewPrimaryInternalAlias)
                    {
                        AddNewPrimaryAlias(a,ref pContext);
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
                            if (alias is WColumnReferenceExpression)
                            {
                                SrcNode = "N_" + pContext.NodeCount.ToString();
                                AddNewAlias(SrcNode, ref pContext);
                                DestNode = alias.ToString();
                                pContext.Paths.Add(new Tuple<string, string, string>(SrcNode, Edge,
                                    DestNode));
                                NewPrimaryInternalAlias.Add(SrcNode);
                            }
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
                                if (alias is WColumnReferenceExpression)
                                {
                                    SrcNode = "N_" + pContext.NodeCount.ToString();
                                    AddNewAlias(SrcNode, ref pContext);
                                    DestNode = alias.ToString();
                                    pContext.Paths.Add(new Tuple<string, string, string>(SrcNode, Edge,
                                        DestNode));
                                    NewPrimaryInternalAlias.Add(SrcNode);
                                }
                            }
                        }
                    pContext.PrimaryInternalAlias.Clear();
                    foreach (var a in NewPrimaryInternalAlias)
                    {
                        AddNewPrimaryAlias(a,ref pContext);
                    }
                    break;
                case (int)GraphViewGremlinParser.Keywords.outE:
                    SrcNode = pContext.PrimaryInternalAlias[0].ToString();
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
                    ChangePrimaryAlias(Edge, ref pContext);
                    foreach (var alias in pContext.PrimaryInternalAlias)
                    {
                        if (alias is WColumnReferenceExpression)
                        {
                            Edge = alias.ToString();
                            pContext.Paths.Add(new Tuple<string, string, string>(SrcNode, Edge,
                                DestNode));
                        }
                    }
                    break;
                case (int)GraphViewGremlinParser.Keywords.inE:
                    DestNode = pContext.PrimaryInternalAlias[0].ToString();
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
                        if (alias is WColumnReferenceExpression)
                        {
                            Edge = alias.ToString();
                            pContext.Paths.Add(new Tuple<string, string, string>(SrcNode, Edge,
                                DestNode));
                        }
                    }
                    break;
                case (int)GraphViewGremlinParser.Keywords.inV:
                    Edge = pContext.PrimaryInternalAlias[0].ToString();
                    var ExistInPath = pContext.Paths.Find(p => p.Item2 == Edge);
                    ChangePrimaryAlias(ExistInPath.Item3, ref pContext);
                    break;
                case (int)GraphViewGremlinParser.Keywords.outV:
                    Edge = pContext.PrimaryInternalAlias[0].ToString();
                    var ExistOutPath = pContext.Paths.Find(p => p.Item2 == Edge);
                    ChangePrimaryAlias(ExistOutPath.Item1, ref pContext);
                    break;
                case (int)GraphViewGremlinParser.Keywords.As:
                    pContext.ExplictAliasToInternalAlias.Add(Parameters.Parameter[0].QuotedString, pContext.PrimaryInternalAlias[0].ToString());
                    break;
                case (int)GraphViewGremlinParser.Keywords.@select:
                    pContext.PrimaryInternalAlias.Clear();
                    if (Parameters == null)
                    {
                        foreach (var a in pContext.ExplictAliasToInternalAlias)
                            AddNewPrimaryAlias(a.Value, ref pContext);
                    }
                    else
                    {
                        foreach (var a in Parameters.Parameter)
                            AddNewPrimaryAlias(pContext.ExplictAliasToInternalAlias[a.QuotedString],ref pContext);
                    }
                    break;
                case (int)GraphViewGremlinParser.Keywords.addV:
                    foreach (var a in Parameters.Parameter.FindAll(p => Parameters.Parameter.IndexOf(p) % 2 == 0))
                    {
                        if (Parameters.Parameter[Parameters.Parameter.IndexOf(a) + 1].QuotedString != null)
                        pContext.Properties.Add(a.QuotedString, Parameters.Parameter[Parameters.Parameter.IndexOf(a) + 1].QuotedString);
                        else
                        pContext.Properties.Add(a.QuotedString, Parameters.Parameter[Parameters.Parameter.IndexOf(a) + 1].Number);
                    }
                    pContext.AddVMark = true;
                    break;
                case (int)GraphViewGremlinParser.Keywords.addOutE:
                    pContext.AddEMark = true;
                    SrcNode = Parameters.Parameter.First().QuotedString;
                    if (!pContext.ExplictAliasToInternalAlias.ContainsKey(SrcNode))
                    {
                        SrcNode = pContext.PrimaryInternalAlias[0].ToString();
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
                    AddNewPrimaryAlias(SrcNode,ref pContext);
                    AddNewPrimaryAlias(DestNode, ref pContext);
                    break;
                case (int)GraphViewGremlinParser.Keywords.addInE:
                    pContext.AddEMark = true;
                    SrcNode = Parameters.Parameter.First().QuotedString;
                    if (!pContext.ExplictAliasToInternalAlias.ContainsKey(SrcNode))
                    {
                        DestNode = pContext.PrimaryInternalAlias[0].ToString();
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
                    AddNewPrimaryAlias(SrcNode, ref pContext);
                    AddNewPrimaryAlias(DestNode, ref pContext);
                    break;
                case (int)GraphViewGremlinParser.Keywords.values:
                    string ValuePara = Parameters.Parameter[0].QuotedString;
                    for (int i = 0; i < pContext.PrimaryInternalAlias.Count; i++)
                        if (pContext.PrimaryInternalAlias[i] is WColumnReferenceExpression)
                            (pContext.PrimaryInternalAlias[i] as WColumnReferenceExpression).MultiPartIdentifier.Identifiers.Add(new Identifier() { Value = ValuePara });
                    break;
                case (int)GraphViewGremlinParser.Keywords.@where:
                    StatueKeeper.Clear();
                    foreach(var x in pContext.PrimaryInternalAlias)
                    StatueKeeper.Add(x);
                    if (Parameters != null && Parameters.Parameter != null)
                    {
                        foreach(var x in Parameters.Parameter) if(x.Fragment != null) x.Fragment.Transform(ref pContext);
                    }
                    pContext.PrimaryInternalAlias.Clear();
                    foreach(var x in StatueKeeper) pContext.PrimaryInternalAlias.Add(x);
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
                    pContext.ExplictAliasToInternalAlias.Add(Parameters.Parameter[0].QuotedString, pContext.PrimaryInternalAlias[0].ToString());
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
                            NewPrimaryInternalAlias.Add(x.ToString());
                        }
                        pContext.PrimaryInternalAlias.Clear();
                        foreach (var x in StatueKeeper)
                        {
                            pContext.PrimaryInternalAlias.Add(x);
                        }
                    }
                    foreach (var x in NewPrimaryInternalAlias)
                    {
                        AddNewPrimaryAlias(x, ref pContext);
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
                    pContext.ByWhat.OrderByElements.Add(new WExpressionWithSortOrder() { ScalarExpr = new WValueExpression(pContext.PrimaryInternalAlias[0].ToString(), false), SortOrder = SortOrder.NotSpecified });
                    pContext.OrderMark = true;
                    break;
                case (int)GraphViewGremlinParser.Keywords.by:
                    if (!pContext.OrderMark && pContext.ByWhat.OrderByElements == null) throw new SyntaxErrorException("Order() should be called before a by().");

                    SortOrder order = SortOrder.NotSpecified;
                    if (Parameters != null)
                    {
                        if (Parameters.Parameter.Last().Fragment != null)
                        {
                            if (Parameters.Parameter.Last().Fragment.Function.KeywordIndex ==
                                (int) GraphViewGremlinParser.Keywords.decr)
                                order = SortOrder.Descending;
                            if (Parameters.Parameter.Last().Fragment.Function.KeywordIndex ==
                                (int) GraphViewGremlinParser.Keywords.incr)
                                order = SortOrder.Ascending;
                        }

                        // .order.by("key", incr/decr)
                        foreach (var x in Parameters.Parameter.GetRange(0, Parameters.Parameter.Count))
                        {
                            if (x.QuotedString != null)
                            {
                                // this is the first by(), override the info created by the Order() step
                                if (pContext.OrderMark)
                                    pContext.ByWhat.OrderByElements.RemoveAt(pContext.ByWhat.OrderByElements.Count - 1);
                                pContext.ByWhat.OrderByElements.Add(new WExpressionWithSortOrder()
                                {
                                    ScalarExpr = new WValueExpression(pContext.PrimaryInternalAlias[0].ToString() + "." + x.QuotedString, false),
                                    SortOrder = order
                                });
                            }
                            else
                                pContext.ByWhat.OrderByElements.Last().SortOrder = order;
                        }
                    }
                    // .order().by()
                    else
                        pContext.ByWhat.OrderByElements.Last().SortOrder = order;
                    pContext.OrderMark = false;
                    break;
                case (int)GraphViewGremlinParser.Keywords.count:
                    pContext.Group = new WGroupByClause()
                    {
                        GroupingSpecifications =
                            new List<WGroupingSpecification>()
                            {
                                new WExpressionGroupingSpec() {Expression = pContext.PrimaryInternalAlias.First()}
                            }
                    };
                    pContext.PrimaryInternalAlias.Clear();
                    pContext.PrimaryInternalAlias.Add(new WFunctionCall() {FunctionName = new Identifier() {Value = "count"} });
                    break;
                case (int)GraphViewGremlinParser.Keywords.max:
                    pContext.Group = new WGroupByClause()
                    {
                        GroupingSpecifications =
                            new List<WGroupingSpecification>()
                            {
                                new WExpressionGroupingSpec() {Expression = pContext.PrimaryInternalAlias.First()}
                            }
                    };
                    WFunctionCall max = new WFunctionCall()
                    {
                        FunctionName = new Identifier() {Value = "max"},
                        Parameters = new List<WScalarExpression>() {pContext.PrimaryInternalAlias.First()}
                    };
                    pContext.PrimaryInternalAlias.Clear();
                    pContext.PrimaryInternalAlias.Add(max);
                    break;
                case (int)GraphViewGremlinParser.Keywords.min:
                    pContext.Group = new WGroupByClause()
                    {
                        GroupingSpecifications =
                            new List<WGroupingSpecification>()
                            {
                                new WExpressionGroupingSpec() {Expression = pContext.PrimaryInternalAlias.First()}
                            }
                    };
                    WFunctionCall min = new WFunctionCall()
                    {
                        FunctionName = new Identifier() { Value = "min" },
                        Parameters = new List<WScalarExpression>() { pContext.PrimaryInternalAlias.First() }
                    };
                    pContext.PrimaryInternalAlias.Clear();
                    pContext.PrimaryInternalAlias.Add(min);
                    break;
                case (int)GraphViewGremlinParser.Keywords.mean:
                    pContext.Group = new WGroupByClause()
                    {
                        GroupingSpecifications =
                            new List<WGroupingSpecification>()
                            {
                                new WExpressionGroupingSpec() {Expression = pContext.PrimaryInternalAlias.First()}
                            }
                    };
                    WFunctionCall mean = new WFunctionCall()
                    {
                        FunctionName = new Identifier() { Value = "mean" },
                        Parameters = new List<WScalarExpression>() { pContext.PrimaryInternalAlias.First() }
                    };
                    pContext.PrimaryInternalAlias.Clear();
                    pContext.PrimaryInternalAlias.Add(mean);
                    break;
                case (int)GraphViewGremlinParser.Keywords.addE:
                    pContext.DoubleAddEMark = true;
                    pContext.AddEMark = true;
                    SrcNode = pContext.PrimaryInternalAlias[0].ToString();
                    for (int i = 0; i < Parameters.Parameter.Count; i += 2)
                    {
                        if (Parameters.Parameter[i + 1].QuotedString != null)
                        pContext.Properties.Add(Parameters.Parameter[i].QuotedString,
                            Parameters.Parameter[i + 1].QuotedString);
                        else
                            pContext.Properties.Add(Parameters.Parameter[i].QuotedString,
    Parameters.Parameter[i + 1].Number);

                    }
                    break;
                case (int)GraphViewGremlinParser.Keywords.repeat:
                    Edge = "P_" + pContext.PathCount.ToString();
                    AddNewAlias(Edge, ref pContext);
                    foreach (var alias in pContext.PrimaryInternalAlias)
                    {
                        if (alias is WColumnReferenceExpression)
                        {
                            DestNode = "N_" + pContext.NodeCount.ToString();
                            AddNewAlias(DestNode, ref pContext);
                            SrcNode = alias.ToString();
                            pContext.Paths.Add(new Tuple<string, string, string>(SrcNode, Edge,
                                DestNode));
                            NewPrimaryInternalAlias.Add(DestNode);
                        }
                    }
                    pContext.PrimaryInternalAlias.Clear();
                    foreach (var a in NewPrimaryInternalAlias)
                    {
                        AddNewPrimaryAlias(a, ref pContext);
                    }
                    break;
                case (int)GraphViewGremlinParser.Keywords.path:
                    pContext.OutputPathMark = true;
                    break;

                default:
                    break;
            }

        }
    }
}
