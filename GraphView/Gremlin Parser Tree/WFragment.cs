using System.Collections.Generic;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class WFragment : WSyntaxTree
    {
        internal WFunction Function { get; set; }
        internal WFragment Fragment { get; set; }
        internal int Identifer { get; set; }
        internal void AddNewAndPredicates(ref GraphViewGremlinSematicAnalyser.Context sink, WBooleanExpression source)
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
            return new WMultiPartIdentifier() { Identifiers = MultiIdentifierList };
        }
        internal void Transform(ref GraphViewGremlinSematicAnalyser.Context pContext)
        {
            WBooleanExpression GeneratedBooleanExpression = null;
            var Identifiers = pContext.Identifiers;
            if (Identifer != -1)
                for (int i = 0; i < pContext.PrimaryInternalAlias.Count; i++)
                    (pContext.PrimaryInternalAlias[i] as WColumnReferenceExpression).MultiPartIdentifier.Identifiers.Add(new Identifier() {Value = Identifiers[Identifer]});
            if (Fragment != null && Function != null &&
                Function.KeywordIndex == (int)GraphViewGremlinParser.Keywords.choose)
            {
                string OrigianlPath = pContext.PrimaryInternalAlias[0].ToString();
                Function.Parameters.Parameter[0].Fragment.Function.Transform(ref pContext);
                pContext.BranchNote = pContext.PrimaryInternalAlias[0].ToString();
                pContext.PrimaryInternalAlias[0] = new WColumnReferenceExpression()
                {
                    MultiPartIdentifier = CutStringIntoMultiPartIdentifier(OrigianlPath)
                };
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
                if (Function.Parameters.Parameter[0].QuotedString == null)
                {
                    GeneratedBooleanExpression = new WBooleanComparisonExpression()
                    {
                        ComparisonType = BooleanComparisonType.Equals,
                        FirstExpr =
                            new WColumnReferenceExpression()
                            {
                                MultiPartIdentifier = CutStringIntoMultiPartIdentifier(pContext.BranchNote)
                            },
                        SecondExpr = new WColumnReferenceExpression()
                        {
                            MultiPartIdentifier =
                                CutStringIntoMultiPartIdentifier(Function.Parameters.Parameter[0].Number.ToString())
                        }
                    };
                    AddNewAndPredicates(ref BranchContext, GeneratedBooleanExpression);
                }
                else
                {
                    GeneratedBooleanExpression = new WBooleanComparisonExpression()
                    {
                        ComparisonType = BooleanComparisonType.Equals,
                        FirstExpr =
                            new WColumnReferenceExpression()
                            {
                                MultiPartIdentifier = CutStringIntoMultiPartIdentifier(pContext.BranchNote)
                            },
                        SecondExpr = new WColumnReferenceExpression()
                        {
                            MultiPartIdentifier =
                                CutStringIntoMultiPartIdentifier(Function.Parameters.Parameter[0].QuotedString)
                        }
                    };
                    AddNewAndPredicates(ref BranchContext, GeneratedBooleanExpression);
                }
                Function.Parameters.Parameter[1].Fragment.Transform(ref BranchContext);
                Fragment.Transform(ref BranchContext);
                pContext.BranchContexts.Add(BranchContext);
                Fragment.Transform(ref pContext);
            }

            if (Function != null && Function.KeywordIndex != (int)GraphViewGremlinParser.Keywords.choose && pContext.ChooseMark == false && Function.KeywordIndex != (int)GraphViewGremlinParser.Keywords.coalesce)
            {
                if (Function != null) Function.Transform(ref pContext);
                if (Fragment != null) Fragment.Transform(ref pContext);
            }

        }
    }
}
