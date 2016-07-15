using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using GraphView;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    public class GraphViewGremlinParser
    {
        internal enum TokenType
        {
            Null,
            LeftParenthesis,
            RightParenthesis,
            LeftBrace,
            RightBrace,
            Integer,
            Double,
            Binary,
            NodeType,
            DoubleQuotedString,
            SingleQuotedString,
            ArithmeticOperator1,
            ArithmeticOperator2,
            ComparisonOperator,
            BindOperator,
            Colon,
            NameToken,
            DotToken,
            Comma,
            LeftBracket,
            RightBracket,
            ValueToken,
            AsToken,
            Space,
            Keyword,
            Identifier
        };
        internal enum Keywords
        {
            addOutE,//0 // supported
            addInE,// supported
            addV,// supported
            property,
            aggregate,
            and,//5
            As,// supported
            by,
            cap,
            coalesce,
            count,//10
            choose,
            coin,
            constant,
            cyclicPath,
            dedup,//15
            drop,
            fold,
            group,
            groupCount,
            has,//20
            inject,
            Is,
            limit,
            local,
            mapKeys,//25
            mapValues,
            match,
            max,
            mean,
            min,//30
            or,
            order,
            path,
            range,
            repeat,//35
            sack,
            sample,
            select,// supported
            simplePath,
            store,//40
            subGraph,
            sum,
            tail,
            timeLimit,
            tree,//45
            unfold,
            union,
            valueMap,
            Out, // supported
            In,//50 // supported
            both,
            outE,
            inE,
            bothE,
            outV,//55
            inV,
            bothV,
            otherV,
            where,
            values,//60
            label,
            V, // supported
            E,// supported
            next,
            g//65// supported
        }
        internal static Dictionary<String, Keywords> KeyWordDic = new Dictionary<string, Keywords>(StringComparer.OrdinalIgnoreCase)
        {
           {"addOutE", Keywords.addOutE},
           {"addInE", Keywords.addInE},
           {"addV", Keywords.addV},
           {"property", Keywords.property},
           {"aggregate", Keywords.aggregate},
           {"and", Keywords.and},
           {"as", Keywords.As},
           {"by", Keywords.by},
           {"cap", Keywords.cap},
           {"coalesce", Keywords.coalesce},
           {"count", Keywords.count},
           {"choose", Keywords.choose},
           {"coin", Keywords.coin},
           {"constant", Keywords.constant},
           {"cyclicPath", Keywords.cyclicPath},
           {"dedup", Keywords.dedup},
           {"drop", Keywords.drop},
           {"fold", Keywords.fold},
           {"group", Keywords.group},
           {"groupCount", Keywords.groupCount},
           {"has", Keywords.has},
           {"inject", Keywords.inject},
           {"Is", Keywords.Is},
           {"limit", Keywords.limit},
           {"local", Keywords.local},
           {"mapKeys", Keywords.mapKeys},
           {"mapValues", Keywords.mapValues},
           {"match", Keywords.match},
           {"max", Keywords.max},
           {"mean", Keywords.mean},
           {"min", Keywords.min},
           {"or", Keywords.or},
           {"order", Keywords.order},
           {"path", Keywords.path},
           {"range", Keywords.range},
           {"repeat", Keywords.repeat},
           {"sack", Keywords.sack},
           {"sample", Keywords.sample},
           {"select", Keywords.select},
           {"simplePath", Keywords.simplePath},
           {"store", Keywords.store},
           {"subGraph", Keywords.subGraph},
           {"sum", Keywords.sum},
           {"tail", Keywords.tail},
           {"timeLimit", Keywords.timeLimit},
           {"tree", Keywords.tree},
           {"unfold", Keywords.unfold},
           {"union", Keywords.union},
           {"valueMap", Keywords.valueMap},
           {"Out", Keywords.Out},
           {"In", Keywords.In},
           {"both", Keywords.both},
           {"outE", Keywords.outE},
           {"inE", Keywords.inE},
           {"bothE", Keywords.bothE},
           {"outV", Keywords.outV},
           {"inV", Keywords.inV},
           {"bothV", Keywords.bothV},
           {"otherV", Keywords.otherV},
           {"where", Keywords.where},
           {"values" ,Keywords.values},
           {"label",Keywords.label },
           {"V", Keywords.V },
           {"E", Keywords.E },
           {"g",Keywords.g },
           {"next", Keywords.next }
        };
        internal class Token
        {
            public TokenType type;
            public string value;
            public int position;
        }

        internal class Keyword : Token
        {
            public Keywords KeywordIndex;
        }

        internal class Identifier : Token
        {
            public int IdentifierIndex;
        }



        internal static class LexicalAnalyzer
        {
            internal static Dictionary<Regex, TokenType> TokenRules = null;

            internal static void Initialize()
            {
                TokenRules = new Dictionary<Regex, TokenType>
                {
                    {new Regex(@"\("), TokenType.LeftParenthesis},
                    {new Regex(@"\)"), TokenType.RightParenthesis},
                    {new Regex(@"{"), TokenType.LeftBrace},
                    {new Regex(@"}"), TokenType.RightBrace},
                    {new Regex(@"(\+|-|~|&|\^|\|)"), TokenType.ArithmeticOperator1},
                    {new Regex(@"(\*|/|%)"), TokenType.ArithmeticOperator2},
                    {new Regex(@"(\<=|\>=|=|\<\>|\<|\>|!\>|!\<|!=)"), TokenType.ComparisonOperator},
                    {new Regex(@":="), TokenType.BindOperator},
                    {new Regex(@":"), TokenType.Colon},
                    {new Regex(@"(0[x|X][0-9a-fA-F]+)"), TokenType.Binary},
                    {new Regex(@"([0-9]+\.[0-9]+)"), TokenType.Double},
                    {new Regex(@"([0-9]+)"), TokenType.Integer},
                    {new Regex(@"(""([^""\\]|\\.)*"")"), TokenType.DoubleQuotedString},
                    {new Regex(@"('([^'\\]|\\.)*')"), TokenType.SingleQuotedString},
                    {new Regex(@"([a-zA-Z_][0-9a-zA-Z_]*)"), TokenType.NameToken},
                    {new Regex(@"\."), TokenType.DotToken},
                    {new Regex(@"\,"), TokenType.Comma},
                    {new Regex(@"\["), TokenType.LeftBracket},
                    {new Regex(@"\]"), TokenType.RightBracket},
                    {new Regex(@"(\s+)"), TokenType.Space}
                };
            }

            internal static Tuple<List<Token>, List<string>> Tokenize(string text, ref string ErrorKey)
            {
                if (TokenRules == null) Initialize();

                List<string> Identifiers = new List<string>();
                List<Token> tokens = new List<Token>();
                int position = 0;
                int LastPosition = position;
                TokenType Type;
                while (position < text.Length)
                {
                    Type = TokenType.Null;
                    var result = TokenRules
                        .Select(p => Tuple.Create(p.Key.Match(text, position), p.Value))
                        .FirstOrDefault(t => t.Item1.Index == position && t.Item1.Success);
                    if (result == null)
                    {
                        ErrorKey = text.Substring(LastPosition, position - LastPosition + 1);
                        return null;
                    }

                    if (KeyWordDic.ContainsKey(result.Item1.Value)) Type = TokenType.Keyword;
                    if (result.Item2 == TokenType.NameToken && !KeyWordDic.ContainsKey(result.Item1.Value))
                        Type = TokenType.Identifier;
                    if (Type == TokenType.Null)
                        Type = result.Item2;

                    if (Type != TokenType.Space)
                    {
                        if (Type == TokenType.DoubleQuotedString)
                        {
                            tokens.Add(new Token()
                            {
                                type = result.Item2,
                                value = result.Item1.Value.Substring(1, result.Item1.Value.Length - 2),
                                position = position
                            });
                        }
                        if (Type == TokenType.Keyword)
                        {
                            tokens.Add(new Keyword()
                            {
                                type = Type,
                                value = result.Item1.Value,
                                position = position,
                                KeywordIndex = KeyWordDic[result.Item1.Value]
                            });
                        }
                        else if (Type == TokenType.Identifier)
                        {
                            int identifier = -1;
                            if (Identifiers.Contains((result.Item1.Value)))
                                identifier = Identifiers.IndexOf((result.Item1.Value));
                            else
                            {
                                Identifiers.Add(result.Item1.Value);
                                identifier = Identifiers.IndexOf((result.Item1.Value));
                            }
                            tokens.Add(new Identifier()
                            {
                                type = Type,
                                value = result.Item1.Value,
                                position = position,
                                IdentifierIndex = identifier
                            });
                        }
                        else
                        {
                            tokens.Add(new Token()
                            {
                                type = Type,
                                value = result.Item1.Value,
                                position = position
                            });
                        }
                    }
                    position += result.Item1.Length;
                    LastPosition = position;
                }
                return new Tuple<List<Token>, List<string>>(tokens, Identifiers);
            }
        }

        internal static bool ReadToken(
            List<Token> tokens,
            TokenType type,
            ref int pNextToken,
            ref string TokenValue,
            ref int pFarestError)
        {
            if (tokens.Count == pNextToken)
            {
                pFarestError = pNextToken;
                return false;
            }
            else if (tokens[pNextToken].type == type)
            {
                TokenValue = tokens[pNextToken].value;
                pNextToken++;
                return true;
            }
            else
            {
                pFarestError = Math.Max(pFarestError, pNextToken);
                return false;
            }
        }

        internal static bool ReadToken(
    List<Token> tokens,
    TokenType type,
    ref int pNextToken,
    ref int TokenIndex,
    ref int pFarestError)
        {
            if (tokens.Count == pNextToken)
            {
                pFarestError = pNextToken;
                return false;
            }
            else if (tokens[pNextToken].type == type)
            {
                if (tokens[pNextToken].type == TokenType.Identifier)
                    TokenIndex = (tokens[pNextToken] as Identifier).IdentifierIndex;
                if (tokens[pNextToken].type == TokenType.Keyword)
                    TokenIndex = (int)((tokens[pNextToken] as Keyword).KeywordIndex);
                pNextToken++;
                return true;
            }
            else
            {
                pFarestError = Math.Max(pFarestError, pNextToken);
                return false;
            }
        }

        internal static bool ReadToken(
            List<Token> tokens,
            string text,
            ref int pNextToken,
            ref string TokenValue,
            ref int pFarestError)
        {
            if (tokens.Count == pNextToken)
            {
                pFarestError = pNextToken;
                return false;
            }
            else if (string.Equals(tokens[pNextToken].value, text, StringComparison.OrdinalIgnoreCase))
            {
                TokenValue = tokens[pNextToken].value;
                pNextToken++;
                return true;
            }
            else
            {
                pFarestError = Math.Max(pFarestError, pNextToken);
                return false;
            }
        }

        internal List<string> Identifiers;
        internal List<Token> TokenList;
        internal int NextToken;
        internal int FarestError;

        internal GraphViewGremlinParser(List<Token> pTokens, List<string> pIdentifiers)
        {
            Identifiers = pIdentifiers;
            TokenList = pTokens;
            NextToken = 0;
            FarestError = -1;
        }
        internal WProgram Parse()
        {
            WPath pPath;
            WProgram result = new WProgram();
            result.paths = new List<WPath>();
            while ((pPath = ParsePath()) != null)
            {
                result.paths.Add((pPath));
            }
            return result;
        }
        internal WPath ParsePath()
        {
            int pIdentifier = ParseIdentifier();
            bool pEqual = ParseEqual();
            WFragment pFragment = ParseFragment();
            if (pIdentifier != null && pEqual && pFragment != null)
                return new WPath() { Fragment = pFragment, IdentifierIndex = pIdentifier };
            if (pFragment != null)
                return new WPath() { Fragment = pFragment, IdentifierIndex = -1 };
            return null;

        }

        internal WFragment ParseFragment()
        {
            WFunction pFunction = ParseFunction();
            bool pFullStop = ParseFullStop();
            if (pFullStop)
            {
                WFragment pFragment = ParseFragment();
                if (pFunction != null && pFullStop && pFragment != null)
                    return new WFragment() { Fragment = pFragment, Function = pFunction, Identifer = -1 };
            }
            if (pFunction != null)
                return new WFragment() { Function = pFunction, Identifer = -1 };
            int pIdentifier = ParseIdentifier();
            pFullStop = ParseFullStop();
            if (pFullStop)
            {
                WFragment pFragment = ParseFragment();
                if (pIdentifier != -1 && pFullStop && pFragment != null)
                    return new WFragment() { Identifer = pIdentifier, Function = pFunction };
            }
            if (pIdentifier != -1)
                return new WFragment() { Identifer = pIdentifier };
            return null;
        }

        internal WFunction ParseFunction()
        {
            int pKeyword = ParseKeyword();
            bool pLeftBrace = ParseLeftBrace();
            if (pLeftBrace)
            {
                WParameters pParameters = ParseParameters();
                bool pRightBrace = ParseRightBrace();
                if (pKeyword != -1 && pLeftBrace && pParameters != null && pRightBrace)
                    return new WFunction() { KeywordIndex = pKeyword, Parameters = pParameters };
            }
            if (pKeyword != -1)
                return new WFunction() { KeywordIndex = pKeyword };
            return null;
        }

        internal WParameters ParseParameters()
        {
            WParameters result = new WParameters();
            WParameter pParameter;
            result.Parameter = new List<WParameter>();
            while ((pParameter = ParseParameter()) != null && ParseColon())
            {
                result.Parameter.Add(pParameter);
            }
            if (pParameter != null)
            {
                result.Parameter.Add(pParameter);
                return result;
            }
            return null;

        }

        internal WParameter ParseParameter()
        {
            string pQuotedString = ParseQuotedString();
            if (pQuotedString != null) return new WParameter() { QuotedString = pQuotedString, IdentifierIndex = -1 };
            int pidentifier = ParseIdentifier();
            if (pidentifier != -1) return new WParameter() { IdentifierIndex = pidentifier };
            WFunction pfunction = ParseFunction();
            if (pfunction != null) return new WParameter() { Function = pfunction, IdentifierIndex = -1 };
            return null;
        }

        internal int ParseIdentifier()
        {
            int index = 0;
            if (ReadToken(TokenList, TokenType.Identifier, ref NextToken, ref index, ref FarestError))
                return index;
            else return -1;
        }

        internal int ParseKeyword()
        {
            int index = 0;
            if (ReadToken(TokenList, TokenType.Keyword, ref NextToken, ref index, ref FarestError))
                return index;
            else return -1;
        }

        internal string ParseQuotedString()
        {
            string QuotedString = "";
            if (ReadToken(TokenList, TokenType.SingleQuotedString, ref NextToken, ref QuotedString, ref FarestError))
                return QuotedString;
            else return null;
        }

        internal bool ParseEqual()
        {
            string temp = "";
            return (ReadToken(TokenList, "=", ref NextToken, ref temp, ref FarestError));
        }

        internal bool ParseLeftBrace()
        {
            string temp = "";
            return (ReadToken(TokenList, "(", ref NextToken, ref temp, ref FarestError));
        }

        internal bool ParseRightBrace()
        {
            string temp = "";
            return (ReadToken(TokenList, ")", ref NextToken, ref temp, ref FarestError));
        }

        internal bool ParseColon()
        {
            string temp = "";
            return (ReadToken(TokenList, ",", ref NextToken, ref temp, ref FarestError));
        }

        internal bool ParseFullStop()
        {
            string temp = "";
            return (ReadToken(TokenList, ".", ref NextToken, ref temp, ref FarestError));
        }

        internal bool ParseSpliter()
        {
            string temp = "";
            return (ReadToken(TokenList, ";", ref NextToken, ref temp, ref FarestError));
        }
    }

    public class GraphViewGremlinSematicAnalyser
    {
        internal struct Context
        {
            internal List<string> PrimaryInternalAlias { get; set; }
            internal List<string> InternalAliasList { get; set; }
            internal List<string> AliasPredicates { get; set; }
            internal List<Tuple<string, string, string>> Paths { get; set; }
            internal List<string> Identifiers { get; set; }
            internal Dictionary<string, string> Properties { get; set; }
            internal Dictionary<string, string> ExplictAliasToInternalAlias { get; set; }
            internal int NodeCount;
            internal int EdgeCount;
            internal bool AddEMark;
            internal bool AddVMark;
        }

        internal Context SematicContext;
        internal WProgram ParserTree;
        internal WSqlStatement SqlTree;
        internal GraphViewGremlinSematicAnalyser(WProgram pParserTree, List<string> pIdentifiers)
        {
            SematicContext = new Context()
            {
                PrimaryInternalAlias = new List<string>(),
                InternalAliasList = new List<string>(),
                AliasPredicates = new List<string>(),
                Paths = new List<Tuple<string, string, string>>(),
                Identifiers = pIdentifiers,
                ExplictAliasToInternalAlias = new Dictionary<string, string>(),
                NodeCount = 0,
                EdgeCount = 0,
                AddEMark = false,
                AddVMark = false
            };
            ParserTree = pParserTree;
        }

        public void Analyse()
        {
            ParserTree.Transform(ref SematicContext);
        }

        public void Transform()
        {
            if (SematicContext.AddVMark)
            {
                var columnV = new List<WScalarExpression>();
                var columnK = new List<WColumnReferenceExpression>();
                foreach (var property in SematicContext.Properties)
                {
                    var value = new WValueExpression(property.Value, true);
                    columnV.Add(value);
                    var key = new WColumnReferenceExpression()
                    {
                        MultiPartIdentifier =
                            new WMultiPartIdentifier()
                            {
                                Identifiers = new List<Identifier>() {new Identifier() {Value = property.Key}}
                            }
                    };
                }
                var row = new List<WRowValue>() { new WRowValue() { ColumnValues = columnV } };
                var source = new WValuesInsertSource() { RowValues = row };
                var target = new WNamedTableReference() {TableObjectString = "Node"};
                var InsertStatement = new WInsertSpecification()
                {
                    Columns = columnK,
                    InsertSource = source,
                    Target = target
                };
            }
            if (SematicContext.AddVMark)
            {
                var columnV = new List<WScalarExpression>();
                var columnK = new List<WColumnReferenceExpression>();
                foreach (var property in SematicContext.Properties)
                {
                    var value = new WValueExpression(property.Value, true);
                    columnV.Add(value);
                    var key = new WColumnReferenceExpression()
                    {
                        MultiPartIdentifier =
                            new WMultiPartIdentifier()
                            {
                                Identifiers = new List<Identifier>() { new Identifier() { Value = property.Key } }
                            }
                    };
                }
                var row = new List<WRowValue>() { new WRowValue() { ColumnValues = columnV } };
                var source = new WValuesInsertSource() { RowValues = row };
                var target = new WNamedTableReference() { TableObjectString = "Edge" };
                var InsertStatement = new WInsertSpecification()
                {
                    Columns = columnK,
                    InsertSource = source,
                    Target = target
                };
            }
            if (!SematicContext.AddEMark && !SematicContext.AddVMark)
            {
                var SelectStatement = new WSelectStatement();
                var SelectBlock = SelectStatement.QueryExpr as WSelectQueryBlock;
                var NewFromClause = new WFromClause() { TableReferences = new List<WTableReference>() };
                foreach (var a in SematicContext.InternalAliasList)
                {
                    var TR = new WNamedTableReference() { Alias = new Identifier() { Value = a } };
                    NewFromClause.TableReferences.Add(TR);
                }
                var NewMatchClause = new WMatchClause() { Paths = new List<WMatchPath>() };
                foreach (var path in SematicContext.Paths)
                {
                    var PathEdges = new List<Tuple<WSchemaObjectName, WEdgeColumnReferenceExpression>>();
                    PathEdges.Add(new Tuple<WSchemaObjectName, WEdgeColumnReferenceExpression>
                        (new WSchemaObjectName()
                        {
                            Identifiers = new List<Identifier>()
                            {
                                new Identifier() {Value = path.Item1}
                            }
                        },
                            new WEdgeColumnReferenceExpression()
                            {
                                MultiPartIdentifier =
                                    new WMultiPartIdentifier()
                                    {
                                        Identifiers = new List<Identifier>() {new Identifier() {Value = path.Item2}}
                                    },
                                Alias = path.Item2
                            }));
                    var TailNode = new WSchemaObjectName()
                    {
                        Identifiers = new List<Identifier>()
                            {
                                new Identifier() {Value = path.Item3}
                            }
                    };
                    var NewPath = new WMatchPath() { PathEdgeList = PathEdges, Tail = TailNode };
                    NewMatchClause.Paths.Add((NewPath));
                }
                var NewSelectElementClause = new List<WSelectElement>();
                foreach (var alias in SematicContext.PrimaryInternalAlias)
                {
                    var pIdentifiers = new List<Identifier>();
                    var TempString = alias;
                    while (TempString.IndexOf('.') != -1)
                    {
                        int cutpoint = TempString.IndexOf('.');
                        pIdentifiers.Add(new Identifier() { Value = TempString.Substring(0, cutpoint) });
                        TempString = TempString.Substring(cutpoint + 1, TempString.Length - cutpoint - 1);
                    }
                    pIdentifiers.Add(new Identifier() { Value = TempString });
                    var SelectExpr = new WColumnReferenceExpression() { MultiPartIdentifier = new WMultiPartIdentifier() { Identifiers = pIdentifiers } };
                    var element = new WSelectScalarExpression() { SelectExpr = SelectExpr };
                    NewSelectElementClause.Add(element);
                }
                var NewWhereClause = new WWhereClause();
                var Condition = new WBooleanBinaryExpression();
                List<WBooleanExpression> BooleanList = new List<WBooleanExpression>();
                foreach (var expr in SematicContext.AliasPredicates)
                {
                    if (expr == "") continue;
                    int cutpoint = expr.IndexOf('=');
                    string firstExpr = expr.Substring(0, cutpoint);
                    string secondExpr = expr.Substring(cutpoint + 1, expr.Length - cutpoint - 1);
                    var pIdentifiers = new List<Identifier>();
                    while (firstExpr.IndexOf('.') != -1)
                    {
                        int cutpoint2 = firstExpr.IndexOf('.');
                        pIdentifiers.Add(new Identifier() { Value = firstExpr.Substring(0, cutpoint2) });
                        firstExpr = firstExpr.Substring(cutpoint2 + 1, firstExpr.Length - cutpoint2 - 1);
                    }
                    pIdentifiers.Add(new Identifier() { Value = firstExpr });
                    var FirstRef = new WColumnReferenceExpression() { MultiPartIdentifier = new WMultiPartIdentifier() { Identifiers = pIdentifiers } };
                    pIdentifiers = new List<Identifier>();
                    while (secondExpr.IndexOf('.') != -1)
                    {
                        int cutpoint2 = secondExpr.IndexOf('.');
                        pIdentifiers.Add(new Identifier() { Value = secondExpr.Substring(0, cutpoint2) });
                        secondExpr = secondExpr.Substring(cutpoint2 + 1, firstExpr.Length - cutpoint2 - 1);
                    }
                    pIdentifiers.Add(new Identifier() { Value = secondExpr });
                    var SecondRef = new WColumnReferenceExpression() { MultiPartIdentifier = new WMultiPartIdentifier() { Identifiers = pIdentifiers } };
                    WBooleanComparisonExpression BBE = new WBooleanComparisonExpression()
                    {
                        ComparisonType = BooleanComparisonType.Equals,
                        FirstExpr = FirstRef,
                        SecondExpr = SecondRef
                    };
                    BooleanList.Add(BBE);
                }
                if (BooleanList.Count > 1)
                {
                    if (BooleanList.Count == 1)
                    {
                        NewWhereClause = new WWhereClause() {SearchCondition = BooleanList[0]};
                    }
                    else
                    {
                        Condition = new WBooleanBinaryExpression()
                        {
                            BooleanExpressionType = BooleanBinaryExpressionType.And,
                            FirstExpr = BooleanList[0],
                            SecondExpr = BooleanList[1]
                        };
                        for (int i = 2; i < BooleanList.Count; i++)
                        {
                            Condition = new WBooleanBinaryExpression()
                            {
                                BooleanExpressionType = BooleanBinaryExpressionType.And,
                                FirstExpr = Condition,
                                SecondExpr = BooleanList[i]
                            };
                        }
                    }
                }

                NewWhereClause = new WWhereClause() { SearchCondition = Condition };
                SelectBlock = new WSelectQueryBlock()
                {
                    FromClause = NewFromClause,
                    SelectElements = NewSelectElementClause,
                    WhereClause = NewWhereClause,
                    MatchClause = NewMatchClause
                };
                SelectStatement = new WSelectStatement() { QueryExpr = SelectBlock };
                SqlTree = SelectStatement;
            }
        }

    }
}

