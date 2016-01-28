// GraphView
// 
// Copyright (c) 2015 Microsoft Corporation
// 
// All rights reserved. 
// 
// MIT License
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
// 
using GraphView;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Policy;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml.Schema;

namespace GraphView
{
    /// <summary>
    /// The parser first extracts all tokens containing MATCH clauses, then replace them with multiline comment.
    /// The original parser will parse remaining tokens and return a syntax tree. Finally, MATCH clauses
    /// are inserted into appropriate QueryBlock by traversing the syntax tree.
    /// </summary>
    public class GraphViewParser
    {
        private IList<WMatchClause> _matchList;
        private IList<bool> _matchFlag;
        private List<TSqlParserToken> _tokens;

        public GraphViewParser()
        {
            _matchList = new List<WMatchClause>();
            _matchFlag = new List<bool>();
        }

        // read token by token's type
        private static bool ReadToken(
            IList<TSqlParserToken> tokens,
            TSqlTokenType type,
            ref string result,
            ref int nextToken,
            ref int farestError)
        {
            if (tokens.Count == nextToken)
            {
                farestError = nextToken;
                return false;
            }
            if (tokens[nextToken].TokenType == type)
            {
                result = tokens[nextToken].Text;
                nextToken++;
                while (nextToken < tokens.Count && tokens[nextToken].TokenType > (TSqlTokenType)236)
                    nextToken++;
                return true;
            }
            farestError = Math.Max(farestError, nextToken);
            return false;
        }

        // read token by token's text
        private static bool ReadToken(
            IList<TSqlParserToken> tokens,
            string value,
            ref int nextToken,
            ref int farestError)
        {
            if (tokens.Count == nextToken)
            {
                farestError = nextToken;
                return false;
            }
            if (string.Equals(tokens[nextToken].Text, value, StringComparison.OrdinalIgnoreCase))
            {
                nextToken++;
                while (nextToken < tokens.Count && tokens[nextToken].TokenType > (TSqlTokenType)236)
                    nextToken++;
                return true;
            }
            farestError = Math.Max(farestError, nextToken);
            return false;
        }

        private static bool ParseMatchPathEdge(
            IList<TSqlParserToken> tokens,
            ref int nextToken,
            ref WEdgeColumnReferenceExpression result,
            ref int farestError)
        {
            var currentToken = nextToken;
            Identifier edgeIdentifier = null;
            if (!ParseQuotedIdentifier(tokens, ref currentToken, ref edgeIdentifier, ref farestError))
                return false;

            int line = tokens[currentToken].Line;
            nextToken = currentToken;
            string alias = null;
            int maxLen = 1;
            int minLen = 1;

            string errorKey = "";
            var edgeTokens = LexicalAnalyzer.Tokenize(edgeIdentifier.Value, ref errorKey);
            if (!string.IsNullOrEmpty(errorKey))
                throw new SyntaxErrorException(line, errorKey);
            int curEdgeToken = 0;
            int edgeFareastError = 0;
            string edgeName = null;
            string strValue = null;
            Dictionary<string, string> attributeValueDict = null;


            // Gets edge name
            if (!ReadToken(edgeTokens, AnnotationTokenType.NameToken, ref curEdgeToken, ref edgeName, ref edgeFareastError))
                throw new SyntaxErrorException(line, edgeTokens[edgeFareastError].value);
            edgeIdentifier.Value = edgeName;

            // Gets path info
            if (ReadToken(edgeTokens, "*", ref curEdgeToken, ref strValue, ref edgeFareastError))
            {
                string lengStr = "";

                // Gets path minimal length
                if (ReadToken(edgeTokens, AnnotationTokenType.Integer, ref curEdgeToken, ref lengStr,
                    ref edgeFareastError))
                {
                    if (!int.TryParse(lengStr, out minLen) || minLen<0)
                        throw new SyntaxErrorException(line, lengStr, "Min length should be an integer no less than zero");
                    for (int i = 0; i < 2; i++)
                    {
                        if (!ReadToken(edgeTokens, ".", ref curEdgeToken, ref strValue,
                            ref edgeFareastError))
                            throw new SyntaxErrorException(line, lengStr,
                                "Two dots should be followed by the minimal length integer");
                    }

                    // Gets path maximal length
                    if (!ReadToken(edgeTokens, AnnotationTokenType.Integer, ref curEdgeToken, ref lengStr,
                        ref edgeFareastError) || !int.TryParse(lengStr, out maxLen) || maxLen < minLen)
                        throw new SyntaxErrorException(line, lengStr,
                            "Max length should be an integer no less than the min length");
                }
                else
                {
                    minLen = 0;
                    maxLen = -1;
                }
                // Gets edge alias
                if (ReadToken(edgeTokens, "as", ref curEdgeToken, ref strValue, ref edgeFareastError))
                {
                    if (!ReadToken(edgeTokens, AnnotationTokenType.NameToken, ref curEdgeToken, ref alias, ref edgeFareastError))
                        throw new SyntaxErrorException(line, edgeTokens[edgeFareastError].value);
                }

                // Gets predicates on attributes
                NestedObject jsonNestedObject = null;
                int braceToken = curEdgeToken;
                if (ReadToken(edgeTokens, AnnotationTokenType.LeftBrace, ref curEdgeToken, ref strValue,
                    ref edgeFareastError))
                {
                    if (!ParseNestedObject(edgeTokens, ref braceToken, ref jsonNestedObject, ref edgeFareastError, true))
                        throw new SyntaxErrorException(line, "{", "Invalid json string");
                    else
                        attributeValueDict =
                            (jsonNestedObject as CollectionObject).Collection.ToDictionary(e => e.Key.ToLower(),
                                e =>
                                    (e.Value is StringObject)
                                        ? "'" + ((StringObject) e.Value).Value + "'"
                                        : (e.Value as NormalObject).Value);
                }
            }

            // Gets edge alias
            if (ReadToken(edgeTokens, "as", ref curEdgeToken, ref strValue, ref edgeFareastError))
            {
                if (!ReadToken(edgeTokens, AnnotationTokenType.NameToken, ref curEdgeToken, ref alias, ref edgeFareastError))
                    throw new SyntaxErrorException(line, edgeTokens[edgeFareastError].value);
            }

            result = new WEdgeColumnReferenceExpression
            {
                ColumnType = ColumnType.Regular,
                Alias = alias,
                LastTokenIndex = currentToken - 1,
                FirstTokenIndex = currentToken - 1,
                MaxLength = maxLen,
                MinLength = minLen,
                MultiPartIdentifier = new WMultiPartIdentifier(edgeIdentifier),
                AttributeValueDict = attributeValueDict
            };
            return true;
        }

        private static bool ParseMatchPathPart(
            IList<TSqlParserToken> tokens,
            ref int nextToken,
            ref Tuple<WSchemaObjectName, WEdgeColumnReferenceExpression> result,
            ref int farestError)
        {
            var currentToken = nextToken;

            WSchemaObjectName node = null;
            WEdgeColumnReferenceExpression edge = null;

            if (!ParseSchemaObjectName(tokens, ref currentToken, ref node, ref farestError))
                return false;

            if (!ReadToken(tokens, "-", ref currentToken, ref farestError))
                return false;

            if (!ParseMatchPathEdge(tokens, ref currentToken, ref edge, ref farestError))
                return false;

            if (!ReadToken(tokens, "-", ref currentToken, ref farestError))
                return false;
            if (!ReadToken(tokens, ">", ref currentToken, ref farestError))
                return false;

            nextToken = currentToken;
            result = new Tuple<WSchemaObjectName, WEdgeColumnReferenceExpression>(node, edge);
            return true;
        }

        private static bool ParseMatchPath(
            IList<TSqlParserToken> tokens,
            ref int nextToken,
            ref WMatchPath result,
            ref int farestError)
        {
            var currentToken = nextToken;
            var firstToken = nextToken;

            var nodeList = new List<Tuple<WSchemaObjectName, WEdgeColumnReferenceExpression>>();

            Tuple<WSchemaObjectName, WEdgeColumnReferenceExpression> tuple = null;
            while (ParseMatchPathPart(tokens, ref currentToken, ref tuple, ref farestError))
            {
                nodeList.Add(tuple);
            }

            if (nodeList.Count == 0)
                return false;

            WSchemaObjectName tail = null;

            if (!ParseSchemaObjectName(tokens, ref currentToken, ref tail, ref farestError))
                return false;

           

            result = new WMatchPath
            {
                PathEdgeList = nodeList,
                Tail = tail,
                FirstTokenIndex = firstToken,
                LastTokenIndex = currentToken - 1
            };
            nextToken = currentToken;
            return true;
        }

        private static bool ParseMatchClause(
            IList<TSqlParserToken> tokens,
            ref int nextToken,
            ref WMatchClause result,
            ref int farestError)
        {
            var currentToken = nextToken;
            var firstToken = nextToken;
            var paths = new List<WMatchPath>();
            WMatchPath path = null;
            if (!ReadToken(tokens, "MATCH", ref currentToken, ref farestError))
                return false;
            while (ParseMatchPath(tokens, ref currentToken, ref path, ref farestError))
            {
                paths.Add(path);
                if (!ReadToken(tokens, ",", ref currentToken, ref farestError))
                    break;
            }
            if (paths.Count == 0)
                return false;
            result = new WMatchClause
            {
                Paths = paths,
                FirstTokenIndex = firstToken,
                LastTokenIndex = currentToken - 1,
            };
            nextToken = currentToken;
            return true;
        }

        private static bool ParseQuotedIdentifier(
            IList<TSqlParserToken> tokens,
            ref int nextToken,
            ref Identifier result,
            ref int farestError)
        {
            var identifierName = "";
            QuoteType quoteType;
            if (!ReadToken(tokens, TSqlTokenType.QuotedIdentifier, ref identifierName, ref nextToken, ref farestError))
                return false;
            var decodedIdentifierName = Identifier.DecodeIdentifier(identifierName, out quoteType);
            result = new Identifier
            {
                Value = decodedIdentifierName,
                QuoteType = quoteType,
            };
            return true;
        }

        private static bool ParseIdentifier(
            IList<TSqlParserToken> tokens,
            ref int nextToken,
            ref Identifier result,
            ref int farestError)
        {
            var currentToken = nextToken;
            var identifierName = "";
            QuoteType quoteType;
            if (!ReadToken(tokens, TSqlTokenType.Identifier, ref identifierName, ref currentToken, ref farestError) &&
                !ReadToken(tokens, TSqlTokenType.QuotedIdentifier, ref identifierName, ref currentToken, ref farestError) &&
                !ReadToken(tokens, TSqlTokenType.AsciiStringOrQuotedIdentifier, ref identifierName, ref currentToken, ref farestError))
                return false;
            var decodedIdentifierName = Identifier.DecodeIdentifier(identifierName, out quoteType);
            result = new Identifier
            {
                Value = decodedIdentifierName,
                QuoteType = quoteType,
                FirstTokenIndex = nextToken,
                LastTokenIndex = nextToken
            };
            nextToken = currentToken;

            return true;
        }

        private static bool ParseMultiPartIdentifier(
            IList<TSqlParserToken> tokens,
            ref int nextToken,
            ref WMultiPartIdentifier result,
            ref int farestError)
        {
            var firstToken = nextToken;
            var currentToken = nextToken;
            var identifiers = new List<Identifier>();
            Identifier identifier = null;
            if (!ParseIdentifier(tokens, ref currentToken, ref identifier, ref farestError))
                return false;
            identifiers.Add(identifier);
            while (ReadToken(tokens, ".", ref currentToken, ref farestError))
            {
                ParseIdentifier(tokens, ref currentToken, ref identifier, ref farestError);
                identifiers.Add(identifier);
            }
            result = new WMultiPartIdentifier
            {
                Identifiers = identifiers,
                FirstTokenIndex = firstToken,
                LastTokenIndex = currentToken - 1,
            };
            nextToken = currentToken;
            return true;
        }

        private static bool ParseSchemaObjectName(
            IList<TSqlParserToken> tokens,
            ref int nextToken,
            ref WSchemaObjectName result,
            ref int farestError)
        {
            var firstToken = nextToken;
            var currentToken = nextToken;
            var identifiers = new List<Identifier>();
            Identifier identifier = null;
            if (!ParseIdentifier(tokens, ref currentToken, ref identifier, ref farestError))
                return false;
            identifiers.Add(identifier);
            for (var i = 0; i < 3; ++i)
            {
                if (!ReadToken(tokens, ".", ref currentToken, ref farestError))
                    break;
                ParseIdentifier(tokens, ref currentToken, ref identifier, ref farestError);
                identifiers.Add(identifier);
            }
            result = new WSchemaObjectName
            {
                Identifiers = identifiers,
                FirstTokenIndex = firstToken,
                LastTokenIndex = currentToken - 1,
            };
            nextToken = currentToken;
            return true;
        }

        private static bool ParseIntegerLiteral(
            IList<TSqlParserToken> tokens,
            ref int nextToken,
            ref Literal result,
            ref int farestError)
        {
            var currentToken = nextToken;
            var value = "";
            if (!ReadToken(tokens, TSqlTokenType.Integer, ref value, ref currentToken, ref farestError))
                return false;
            result = new IntegerLiteral
            {
                FirstTokenIndex = currentToken - 1,
                LastTokenIndex = currentToken - 1,
                Value = value,
            };
            nextToken = currentToken;
            return true;
        }

        private static bool ParseMaxLiteral(
            IList<TSqlParserToken> tokens,
            ref int nextToken,
            ref Literal result,
            ref int farestError)
        {
            var currentToken = nextToken;
            if (!ReadToken(tokens, "max", ref currentToken, ref farestError))
                return false;
            result = new MaxLiteral
            {
                FirstTokenIndex = currentToken - 1,
                LastTokenIndex = currentToken - 1,
                Value = "max",
            };
            nextToken = currentToken;
            return true;
        }

        private static bool ParseLiteralList(
            IList<TSqlParserToken> tokens,
            ref int nextToken,
            ref List<Literal> result,
            ref int farestError)
        {
            var currentToken = nextToken;
            var literalList = new List<Literal>();
            if (!ReadToken(tokens, "(", ref currentToken, ref farestError))
                return false;
            Literal literal = null;
            if (ParseMaxLiteral(tokens, ref currentToken, ref literal, ref farestError) ||
                ParseIntegerLiteral(tokens, ref currentToken, ref literal, ref farestError))
                literalList.Add(literal);
            else
                return false;

            if (ReadToken(tokens, ",", ref currentToken, ref farestError))
            {
                if (literalList.First().LiteralType == LiteralType.Integer &&
                    ParseIntegerLiteral(tokens, ref currentToken, ref literal, ref farestError))
                    literalList.Add(literal);
                else
                    return false;
            }

            if (!ReadToken(tokens, ")", ref currentToken, ref farestError))
                return false;
            nextToken = currentToken;
            result = literalList;
            return true;
        }

        private static bool ParseParameterizedDataType(
            IList<TSqlParserToken> tokens,
            ref int nextToken,
            ref WDataTypeReference result,
            ref int farestError)
        {
            var firstToken = nextToken;
            var currentToken = nextToken;
            WSchemaObjectName varName = null;
            if (!ParseSchemaObjectName(tokens, ref currentToken, ref varName, ref farestError))
                return false;
            List<Literal> parameters = null;
            ParseLiteralList(tokens, ref currentToken, ref parameters, ref farestError);

            var dataTypeReference = new WParameterizedDataTypeReference
            {
                Name = varName,
                Parameters = parameters,
                FirstTokenIndex = firstToken,
                LastTokenIndex = currentToken - 1,
            };
            nextToken = currentToken;
            result = dataTypeReference;
            return true;
        }

        //private static bool ParseXmlDataType(
        //    IList<TSqlParserToken> tokens,
        //    ref int nextToken,
        //    ref WDataTypeReference result,
        //    ref int farestError)
        //{
        //    var firstToken = nextToken;
        //    var currentToken = nextToken;
        //    WSchemaObjectName varName = null;
        //    WSchemaObjectName xmlSchemaCollection = null;
        //    if (!ParseSchemaObjectName(tokens, ref currentToken, ref varName, ref farestError))
        //        return false;
        //    var xmlDataTypeOption = XmlDataTypeOption.None;
        //    if (ReadToken(tokens, "(", ref currentToken, ref farestError))
        //    {
        //        if (ReadToken(tokens, "document", ref currentToken, ref farestError))
        //            xmlDataTypeOption = XmlDataTypeOption.Document;
        //        else if (ReadToken(tokens, "content", ref currentToken, ref farestError))
        //            xmlDataTypeOption = XmlDataTypeOption.Content;
        //        if (!ParseSchemaObjectName(tokens, ref currentToken, ref xmlSchemaCollection, ref farestError))
        //            return false;
        //        if (!ReadToken(tokens, ")", ref currentToken, ref farestError))
        //            return false;
        //    }
        //    var dataTypeReference = new WXmlDataTypeReference
        //    {
        //        Name = varName,
        //        XmlDataTypeOption = xmlDataTypeOption,
        //        XmlSchemaCollection = xmlSchemaCollection,
        //        FirstTokenIndex = firstToken,
        //        LastTokenIndex = currentToken - 1,
        //    };
        //    nextToken = currentToken;
        //    result = dataTypeReference;
        //    return true;
        //}

        private static bool ParseDataType(
            IList<TSqlParserToken> tokens,
            ref int nextToken,
            ref WDataTypeReference result,
            ref int farestError)
        {
            return (ParseParameterizedDataType(tokens, ref nextToken, ref result, ref farestError));
        }

        private static bool ParseNodeTableColumn(
            IList<TSqlParserToken> tokens,
            ref int nextToken,
            ref WNodeTableColumn result,
            ref int farestError)
        {
            var currentToken = nextToken;
            var firstToken = nextToken;
            var lastToken = nextToken;
            var metaDataStr = "";
            // Quoted identifier is an expression inside a pair of brackets [ ... ]
            if (!ReadToken(tokens, TSqlTokenType.QuotedIdentifier, ref metaDataStr, ref currentToken, ref farestError))
                return false;

            Identifier columnName = null;
            if (!ParseIdentifier(tokens, ref currentToken, ref columnName, ref farestError))
                return false;

            WDataTypeReference dataType = null;
            if (!ParseDataType(tokens, ref currentToken, ref dataType, ref farestError))
                return false;

            HashSet<string> metaDataFields = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                "columnrole",
                "reference",
                "attributes"
            }; 

            metaDataStr = '{' + metaDataStr.Substring(1, metaDataStr.Length - 2) + '}';

            var metaStrToken = tokens[firstToken];
            var tokenErrorKey = "";
            List<AnnotationToken> annotationTokens = LexicalAnalyzer.Tokenize(metaDataStr, ref tokenErrorKey);
            if (annotationTokens == null)
            {
                throw new SyntaxErrorException(metaStrToken.Line, tokenErrorKey);
            }
            int startToken = 0, fareastError = -1;
            NestedObject nestedObj = null;
            ParseNestedObject(annotationTokens, ref startToken, ref nestedObj, ref fareastError);

            if (nestedObj == null || nestedObj.GetType() != typeof(CollectionObject))
            {
                throw new SyntaxErrorException(metaStrToken.Line, annotationTokens[1].value);
            }

            CollectionObject annotationObj = nestedObj as CollectionObject;
            var invalidFields = annotationObj.Collection.Where(e => !metaDataFields.Contains(e.Key)).Select(e=>e.Key).ToList();
            if (invalidFields.Count > 0)
            {
                var invalidFieldsStr = new StringBuilder();
                foreach (var field in invalidFields)
                {
                    invalidFieldsStr.AppendFormat("'{0}' ", field);
                }
                throw new SyntaxErrorException(metaStrToken.Line, invalidFields.First(),
                    string.Format("Invalid metadata field(s): {0}", invalidFieldsStr));
            }
            if (annotationObj.Collection.ContainsKey("columnrole"))
            {
                StringObject roleValue = annotationObj.Collection["columnrole"] as StringObject;

                if (string.Equals(roleValue.Value, "edge", StringComparison.OrdinalIgnoreCase))
                {
                    if (!annotationObj.Collection.ContainsKey("reference"))
                    {
                        throw new SyntaxErrorException(metaStrToken.Line, "edge", "No edge reference");
                    }

                    StringObject refValue = annotationObj.Collection["reference"] as StringObject;
                    var refTableName = new WSchemaObjectName(new Identifier { Value = refValue.Value });
                    var attributeList = new List<Tuple<Identifier, WEdgeAttributeType>>();

                    if (annotationObj.Collection.ContainsKey("attributes"))
                    {
                        CollectionObject attributeCollection = annotationObj.Collection["attributes"] as CollectionObject;

                        foreach (string propName in attributeCollection.Collection.Keys)
                        {
                            StringObject propValue = attributeCollection.Collection[propName] as StringObject;

                            attributeList.Add(new Tuple<Identifier, WEdgeAttributeType>(
                                new Identifier { Value = propName },
                                (WEdgeAttributeType)Enum.Parse(typeof(WEdgeAttributeType), propValue.Value, true)
                                ));
                        }
                    }

                    result = new WGraphTableEdgeColumn
                    {
                        ColumnName = columnName,
                        DataType = dataType,
                        TableReference = new WNamedTableReference { TableObjectName = refTableName },
                        Attributes = attributeList,
                        FirstTokenIndex = firstToken,
                        LastTokenIndex = currentToken - 1,
                    };
                }
                else if (string.Equals(roleValue.Value, "property", StringComparison.OrdinalIgnoreCase))
                {
                    if (annotationObj.Collection.Count > 1)
                    {
                        var invalidName =
                            annotationObj.Collection.Where(e => e.Key.ToLower() != "columnrole")
                                .Select(e => e.Key);
                        var sb = new StringBuilder();
                        foreach (var name in invalidName)
                        {
                            sb.AppendFormat("'{0}' ",name);
                        }
                        throw new SyntaxErrorException(metaStrToken.Line, "columnrole",
                            string.Format("Invalid metadata field(s): {0}", sb));
                    }
                    result = new WGraphTablePropertyColumn
                    {
                        ColumnName = columnName,
                        DataType = dataType,
                        FirstTokenIndex = firstToken,
                        LastTokenIndex = currentToken - 1,
                    };
                }
                else if (string.Equals(roleValue.Value, "nodeid", StringComparison.OrdinalIgnoreCase))
                {
                    if (annotationObj.Collection.Count > 1)
                    {
                        var invalidName =
                            annotationObj.Collection.Where(e => e.Key.ToLower() != "columnrole")
                                .Select(e => e.Key);
                        var sb = new StringBuilder();
                        foreach (var name in invalidName)
                        {
                            sb.AppendFormat("'{0}' ", name);
                        }
                        throw new SyntaxErrorException(metaStrToken.Line, "columnrole",
                            string.Format("Invalid metadata field(s): {0}", sb));
                    }
                    result = new WGraphTableNodeIdColumn
                    {
                        ColumnName = columnName,
                        DataType = dataType,
                        FirstTokenIndex = firstToken,
                        LastTokenIndex = currentToken - 1,
                    };
                }
                else
                {
                    throw new SyntaxErrorException(metaStrToken.Line, "columnrole", "Invalid column role");
                }
            }
            else
            {
                throw new SyntaxErrorException(metaStrToken.Line, annotationTokens[1].value, "Invalid metadata Field"); ;
            }

            nextToken = currentToken;
            return true;
        }

        #region A parser for a nested dictionary
        private abstract class NestedObject { }

        private class StringObject : NestedObject
        {
            public string Value { get; set; }
        }

        private class NormalObject: NestedObject
        {
            public string Value { get; set; }
        }

        private class CollectionObject : NestedObject
        {
            public IDictionary<string, NestedObject> Collection { get; set; }
        }

        private enum AnnotationTokenType
        {
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
            Space
        };

        private struct AnnotationToken
        {
            public AnnotationTokenType type;
            public string value;
            public int position;
        }
        private static class LexicalAnalyzer 
        {
            private static Dictionary<Regex, AnnotationTokenType> tokenRules = null;

            private static void Initialize()
            {
                tokenRules = new Dictionary<Regex, AnnotationTokenType>
                {
                    {new Regex(@"\("), AnnotationTokenType.LeftParenthesis},
                    {new Regex(@"\)"), AnnotationTokenType.RightParenthesis},
                    {new Regex(@"{"), AnnotationTokenType.LeftBrace},
                    {new Regex(@"}"), AnnotationTokenType.RightBrace},
                    {new Regex(@"(\+|-|~|&|\^|\|)"), AnnotationTokenType.ArithmeticOperator1},
                    {new Regex(@"(\*|/|%)"), AnnotationTokenType.ArithmeticOperator2},
                    {new Regex(@"(\<=|\>=|=|\<\>|\<|\>|!\>|!\<|!=)"), AnnotationTokenType.ComparisonOperator},
                    {new Regex(@":="), AnnotationTokenType.BindOperator},
                    {new Regex(@":"), AnnotationTokenType.Colon},
                    {new Regex(@"(0[x|X][0-9a-fA-F]+)"), AnnotationTokenType.Binary},
                    {new Regex(@"([0-9]+\.[0-9]+)"), AnnotationTokenType.Double},
                    {new Regex(@"([0-9]+)"), AnnotationTokenType.Integer},
                    {new Regex(@"(\$[a-zA-Z_][0-9a-zA-Z_]*)"), AnnotationTokenType.NodeType},
                    {new Regex(@"(""([^""\\]|\\.)*"")"), AnnotationTokenType.DoubleQuotedString},
                    {new Regex(@"('([^'\\]|\\.)*')"), AnnotationTokenType.SingleQuotedString},
                    {new Regex(@"([a-zA-Z_][0-9a-zA-Z_]*)"), AnnotationTokenType.NameToken},
                    {new Regex(@"\."), AnnotationTokenType.DotToken},
                    {new Regex(@"\,"), AnnotationTokenType.Comma},
                    {new Regex(@"\["), AnnotationTokenType.LeftBracket},
                    {new Regex(@"\]"), AnnotationTokenType.RightBracket},
                    {new Regex(@"(\s+)"), AnnotationTokenType.Space}
                };
            }

            internal static List<AnnotationToken> Tokenize(string text, ref string errorKey)
            {
                if (tokenRules == null)
                {
                    Initialize();
                }

                List<AnnotationToken> tokens = new List<AnnotationToken>();
                int position = 0;
                int prePosition = position;
                while (position < text.Length)
                {
                    var result = tokenRules
                        .Select(p => Tuple.Create(p.Key.Match(text, position), p.Value)).FirstOrDefault(t => t.Item1.Index == position && t.Item1.Success);
                    if (result == null)
                    {
                        errorKey = text.Substring(prePosition, position - prePosition + 1);
                        return null;
                    }
                    if (result.Item2 != AnnotationTokenType.Space)
                    {
                        if (result.Item2 == AnnotationTokenType.DoubleQuotedString)
                        {
                            tokens.Add(new AnnotationToken()
                            {
                                type = result.Item2,
                                value = result.Item1.Value.Substring(1, result.Item1.Value.Length - 2),
                                position = position
                            });
                        }
                        else
                        {
                            tokens.Add(new AnnotationToken()
                            {
                                type = result.Item2,
                                value = result.Item1.Value,
                                position = position
                            });
                        }
                    }
                    position += result.Item1.Length;
                    prePosition = position;
                }

                return tokens;
            }
        }

        private static bool ReadToken(
            List<AnnotationToken> tokens,
            AnnotationTokenType type,
            ref int nextToken,
            ref string tokenValue,
            ref int fareastError)
        {
            if (tokens.Count == nextToken)
            {
                fareastError = nextToken;
                return false;
            }
            else if (tokens[nextToken].type == type)
            {
                tokenValue = tokens[nextToken].value;
                nextToken++;
                return true;
            }
            else
            {
                fareastError = Math.Max(fareastError, nextToken);
                return false;
            }
        }

        private static bool ReadToken(
            List<AnnotationToken> tokens,
            string text,
            ref int nextToken,
            ref string tokenValue,
            ref int fareastError)
        {
            if (tokens.Count == nextToken)
            {
                fareastError = nextToken;
                return false;
            }
            else if (string.Equals(tokens[nextToken].value, text, StringComparison.OrdinalIgnoreCase))
            {
                tokenValue = tokens[nextToken].value;
                nextToken++;
                return true;
            }
            else
            {
                fareastError = Math.Max(fareastError, nextToken);
                return false;
            }
        }

        private static bool ParseNestedObject(
            List<AnnotationToken> tokenList, 
            ref int nextToken, 
            ref NestedObject result,
            ref int fareastError,
            bool supportMoreFieldType = false)
        {
            int currentToken = nextToken;
            string tokenStr = null;

            if (ReadToken(tokenList, AnnotationTokenType.LeftBrace, ref currentToken, ref tokenStr, ref fareastError))
            {
                string fieldName = null;
                NestedObject fieldValue = null;

                CollectionObject collectionObj = new CollectionObject()
                {
                    Collection = new Dictionary<string, NestedObject>()
                };
                // Field Name can be a NameToken or a DoubleQuotedString 
                if ((ReadToken(tokenList, AnnotationTokenType.DoubleQuotedString, ref currentToken, ref fieldName, ref fareastError) || 
                    ReadToken(tokenList, AnnotationTokenType.NameToken, ref currentToken, ref fieldName, ref fareastError)) &&
                    ReadToken(tokenList, AnnotationTokenType.Colon, ref currentToken, ref tokenStr, ref fareastError) &&
                    ParseNestedObject(tokenList, ref currentToken, ref fieldValue, ref fareastError, supportMoreFieldType))
                {
                    collectionObj.Collection[fieldName.ToLower()] = fieldValue;
                }
                else
                {
                    return false;
                }

                while (ReadToken(tokenList, AnnotationTokenType.Comma, ref currentToken, ref tokenStr, ref fareastError) &&
                    (ReadToken(tokenList, AnnotationTokenType.DoubleQuotedString, ref currentToken, ref fieldName, ref fareastError) || 
                    ReadToken(tokenList, AnnotationTokenType.NameToken, ref currentToken, ref fieldName, ref fareastError)) &&
                    ReadToken(tokenList, AnnotationTokenType.Colon, ref currentToken, ref tokenStr, ref fareastError) &&
                    ParseNestedObject(tokenList, ref currentToken, ref fieldValue, ref fareastError, supportMoreFieldType))
                {
                    collectionObj.Collection[fieldName.ToLower()] = fieldValue;
                }

                if (!ReadToken(tokenList, AnnotationTokenType.RightBrace, ref currentToken, ref tokenStr, ref fareastError))
                {
                    return false;
                }

                result = collectionObj;
                nextToken = currentToken;
                return true;
            }
            else if (
                ReadToken(tokenList, AnnotationTokenType.DoubleQuotedString, ref currentToken, ref tokenStr,
                    ref fareastError)
                )
            {
                StringObject stringObj = new StringObject()
                {
                    Value = tokenStr
                };

                result = stringObj;
                nextToken = currentToken;
                return true;
            }
            else if (supportMoreFieldType &&
                 (ReadToken(tokenList, AnnotationTokenType.Integer, ref currentToken, ref tokenStr,
                     ref fareastError) ||
                  ReadToken(tokenList, AnnotationTokenType.Double, ref currentToken, ref tokenStr,
                      ref fareastError) ||
                  ReadToken(tokenList, AnnotationTokenType.Binary, ref currentToken, ref tokenStr,
                      ref fareastError) ||
                  ReadToken(tokenList, AnnotationTokenType.SingleQuotedString, ref currentToken, ref tokenStr,
                      ref fareastError)))
            {
                NormalObject normalObject = new NormalObject
                {
                    Value = tokenStr
                };
                result = normalObject;
                nextToken = currentToken;
                return true;
            }
            else
            {
                return false;
            }
        }

        #endregion

        /// <summary>
        /// Parses a CREATE TABLE statement. The parser first replaces column annotations with white space, 
        /// then uses T-SQL parser to parse it, and finally interprets the column annotations.
        /// </summary>
        /// <param name="queryStr">The CREATE TABLE statement creating a ndoe table</param>
        /// <param name="nodeTableColumns">A list of columns of the node table</param>
        /// <param name="errors">Parsing errors</param>
        /// <returns>The syntax tree of the CREATE TABLE statement</returns>
        public WSqlFragment ParseCreateNodeTableStatement(
            string queryStr, 
            out List<WNodeTableColumn> nodeTableColumns, 
            out IList<ParseError> errors)
        {
            // Gets token stream
            var tsqlParser = new TSql110Parser(true);
            var sr = new StringReader(queryStr);
            var tokens = new List<TSqlParserToken>(tsqlParser.GetTokenStream(sr, out errors));
            if (errors.Count > 0)
            {
                nodeTableColumns = null;
                return null;
            }

            // Retrieves node table columns
            var currentToken = 0;
            var farestError = 0;
            nodeTableColumns = new List<WNodeTableColumn>();
            while (currentToken < tokens.Count)
            {
                WNodeTableColumn column = null;
                if (ParseNodeTableColumn(tokens, ref currentToken, ref column, ref farestError))
                    nodeTableColumns.Add(column);
                else
                    currentToken++;
            }

            // Replaces column annotations with whitespace
            foreach (var t in nodeTableColumns)
            {
                tokens[t.FirstTokenIndex].TokenType = TSqlTokenType.WhiteSpace;
                tokens[t.FirstTokenIndex].Text = "";
            }

            // Parses the remaining statement using the T-SQL parser
            //IList<ParseError> errors;
            var parser = new WSqlParser();
            var fragment = parser.Parse(tokens, out errors) as WSqlScript;
            if (errors.Count > 0)
                return null;

            // In addition to columns specified in the CREATE TABLE statement,
            // adds an additional column recording the incoming degree of nodes.
            var inDegreeCol = new WColumnDefinition
            {
                ColumnIdentifier = new Identifier { Value = "InDegree" },
                Constraints = new List<WConstraintDefinition>{new WNullableConstraintDefinition { Nullable = false }},
                DataType = new WParameterizedDataTypeReference
                {
                    Name = new WSchemaObjectName(new Identifier { Value = "int" }),
                },
                DefaultConstraint = new WDefaultConstraintDefinition
                {
                    Expression = new WValueExpression
                    {
                        Value = "0"
                    }
                }
            };

            var deltaColumnDefList = new List<WColumnDefinition>();

            
            WCreateTableStatement stmt = fragment.Batches[0].Statements[0] as WCreateTableStatement;
            if (stmt == null || stmt.Definition == null || stmt.Definition.ColumnDefinitions==null)
            {
                return null;
            }
            else if (stmt.Definition.ColumnDefinitions.Count != nodeTableColumns.Count)
            {
                var error = tokens[stmt.FirstTokenIndex];
                errors.Add(new ParseError(0, error.Offset, error.Line, error.Column,
                    "Metadata should be specified for each column when creating a node table"));
            }
            

            var graphColIndex = 0;
            var rawColumnDef = stmt.Definition.ColumnDefinitions;
            for (var i = 0; i < rawColumnDef.Count && graphColIndex < nodeTableColumns.Count; ++i, ++graphColIndex)
            {
                var nextGraphColumn = nodeTableColumns[graphColIndex];
                // Skips columns without annotations
                while (i < rawColumnDef.Count && rawColumnDef[i].LastTokenIndex < nextGraphColumn.FirstTokenIndex)
                {
                    ++i;
                }

                switch (nextGraphColumn.ColumnRole)
                {
                    case WNodeTableColumnRole.Edge:
                        // For an adjacency-list column, its data type is always varbinary(max)
                        var def = rawColumnDef[i];
                        def.DataType = new WParameterizedDataTypeReference
                        {
                            Name = new WSchemaObjectName(new Identifier { Value = "varbinary" }),
                            Parameters = new List<Literal> { new MaxLiteral { Value = "max" } }
                        };
                        def.Constraints.Add(new WNullableConstraintDefinition { Nullable = false });
                        def.DefaultConstraint = new WDefaultConstraintDefinition
                        {
                            Expression = new WValueExpression
                            {
                                Value = "0x"
                            }
                        };
                        // For each adjacency-list column, adds a "delta" column to 
                        // facilitate deleting edges.
                        deltaColumnDefList.Add(new WColumnDefinition
                        {
                            ColumnIdentifier = new Identifier { Value = def.ColumnIdentifier.Value + "DeleteCol" },
                            ComputedColumnExpression = def.ComputedColumnExpression,
                            Constraints = def.Constraints,
                            DataType = def.DataType,
                            DefaultConstraint = def.DefaultConstraint,
                        });
                        // For each adjacency-list column, adds an integer column to record the list's outgoing degree
                        deltaColumnDefList.Add(new WColumnDefinition
                        {
                            ColumnIdentifier = new Identifier { Value = def.ColumnIdentifier.Value + "OutDegree" },
                            Constraints = def.Constraints,
                            DataType = new WParameterizedDataTypeReference
                            {
                                Name = new WSchemaObjectName(new Identifier { Value = "int" }),
                            },
                            DefaultConstraint = new WDefaultConstraintDefinition
                            {
                                Expression = new WValueExpression
                                {
                                    Value = "0"
                                }
                            }


                        });
                        break;
                    case WNodeTableColumnRole.NodeId:
                        // set unique key to user defined node id
                        bool containNullableConstraint = false;
                        foreach (var con in rawColumnDef[i].Constraints)
                        {
                            var nullableConstraint = con as WNullableConstraintDefinition;
                            if (nullableConstraint != null)
                            {
                                containNullableConstraint = true;
                                nullableConstraint.Nullable = false;
                                break;
                            }
                        }
                        if (!containNullableConstraint)
                        {
                            rawColumnDef[i].Constraints.Add(new WNullableConstraintDefinition { Nullable = false });
                        }
                        rawColumnDef[i].Constraints.Add(new WUniqueConstraintDefinition
                        {
                            Clustered = false,
                            IsPrimaryKey = false,
                            ConstraintIdentifier = new Identifier
                            {
                                Value = string.Format("{0}_UQ_{1}", (stmt.SchemaObjectName.SchemaIdentifier == null
                                    ? "dbo"
                                    : stmt.SchemaObjectName.SchemaIdentifier.Value) +
                                                                    stmt.SchemaObjectName.BaseIdentifier.Value,
                                    rawColumnDef[i].ColumnIdentifier.Value)
                            }
                        });
                        break;
                }
            }

            // Adds a GlobalNodeID column to the node table.
            // This column is the primary key of the node table. 
            var globalNodeIdCol = new WColumnDefinition
            {
                ColumnIdentifier = new Identifier { Value = "GlobalNodeId" },
                DataType = new WParameterizedDataTypeReference
                {
                    Name = new WSchemaObjectName(new Identifier { Value = "bigint" }),
                },
                Constraints = new List<WConstraintDefinition>
                    {
                        new WUniqueConstraintDefinition
                        {
                            Clustered = true,
                            IsPrimaryKey = true,
                            ConstraintIdentifier =
                                new Identifier
                                {
                                    Value =
                                        (stmt.SchemaObjectName.SchemaIdentifier == null
                                            ? "dbo"
                                            : stmt.SchemaObjectName.SchemaIdentifier.Value) +
                                        stmt.SchemaObjectName.BaseIdentifier.Value + "_PK_GlobalNodeId"
                                }
                        }
                    },
                IdentityOptions = new WIdentityOptions
                {
                    IdentitySeed = new WValueExpression("1", false),
                    IdentityIncrement = new WValueExpression("1", false),
                },
            };

            // Adds an identity column to the node table. 
            // This column will be used to adjust size estimation. 
            var identityCol = new WColumnDefinition
            {
                ColumnIdentifier = new Identifier { Value = "LocalNodeId" },
                DataType = new WParameterizedDataTypeReference
                {
                    Name = new WSchemaObjectName(new Identifier { Value = "int" }),
                },
                DefaultConstraint = new WDefaultConstraintDefinition
                {
                    Expression = new WFunctionCall
                    {
                        FunctionName = new Identifier { Value = "CHECKSUM" },
                        Parameters = new List<WScalarExpression>
                            {
                                new WFunctionCall
                                {
                                    FunctionName = new Identifier{Value = "NEWID"},
                                    Parameters = new List<WScalarExpression>()
                                }
                            }
                    }
                }

            };

            
            foreach (var definition in deltaColumnDefList)
            {
                stmt.Definition.ColumnDefinitions.Add(definition);
            }
            stmt.Definition.ColumnDefinitions.Add(globalNodeIdCol);
            stmt.Definition.ColumnDefinitions.Add(identityCol);
            stmt.Definition.ColumnDefinitions.Add(inDegreeCol);

            return fragment;
        }

        public WSqlFragment ParseCreateNodeEdgeViewStatement(string query, out IList<ParseError> errors)
        {
            var tsqlParser = new TSql110Parser(true);
            var sr = new StringReader(query);
            var tokens = new List<TSqlParserToken>(tsqlParser.GetTokenStream(sr, out errors));
            if (errors.Count > 0)
            {
                return null;
            }
            int currentToken = 0;
            int farestError = 0;
            while (currentToken < tokens.Count)
            {
                int nextToken = currentToken;
                if (ReadToken(tokens, "create", ref nextToken, ref farestError))
                {
                    int pos = nextToken;
                    if (ReadToken(tokens, "node", ref nextToken, ref farestError))
                    {
                        tokens[pos].TokenType = TSqlTokenType.MultilineComment;
                        tokens[pos].Text = "/*__GRAPHVIEW_CREATE_NODEVIEW*/";
                    }
                    else if (ReadToken(tokens, "edge", ref nextToken, ref farestError))
                    {
                        tokens[pos].TokenType = TSqlTokenType.MultilineComment;
                        tokens[pos].Text = "/*__GRAPHVIEW_CREATE_EDGEVIEW*/";
                    }
                    else
                    {
                        var error = tokens[farestError];
                        throw new SyntaxErrorException(error.Line, error.Text);
                        //errors.Add(new ParseError(0, error.Offset, error.Line, error.Column,
                        //    string.Format("Incorrect syntax near {0}", error.Text)));
                    }
                }
                currentToken++;
            }

            var parser = new WSqlParser();
            var fragment = parser.Parse(tokens, out errors) as WSqlScript;
            if (errors.Count > 0)
                return null;
            return fragment;
        }



        private void ExtractMatchClause()
        {
            var farestError = 0;
            var currentToken = 0;
            while (currentToken < _tokens.Count)
            {
                WMatchClause result = null;
                if (ParseMatchClause(_tokens, ref currentToken, ref result, ref farestError))
                {
                    _matchList.Add(result);
                    _matchFlag.Add(false);
                }
                else
                {
                    currentToken++;
                }
            }

            //replace MATCH clause with multiline comment
            foreach (var match in _matchList)
            {
                for (var i = match.FirstTokenIndex; i <= match.LastTokenIndex; ++i)
                {
                    _tokens[i].TokenType = TSqlTokenType.MultilineComment;
                    _tokens[i].Text = "/*__GRAPHVIEW_MATCH_CLAUSE*/";
                }
            }
        }

        /// <summary>
        /// Finds all graph modification statements (INSERT NODE, INSERT EDGE, DELETE NOTE, DELETE EDGE), 
        /// records their positions in the script as a list of annotations, and replaces them by INSERT and DELETE,
        /// so that the token list can be parsed by the T-SQL parser.
        /// </summary>
        /// <returns>A list of annotations storing the positions of graph modification statements</returns>
        private List<GraphDataModificationAnnotation> FindReplaceGraphModificationStatements(ref IList<ParseError> errors)
        {
            var ret = new List<GraphDataModificationAnnotation>();
            var currentToken = 0;
            var farestError = 0;
            while (currentToken < _tokens.Count)
            {
                var nextToken = currentToken;
                if (ReadToken(_tokens, "insert", ref nextToken, ref farestError))
                {

                    var pos = nextToken;
                    if (ReadToken(_tokens, "node", ref nextToken, ref farestError))
                    {
                        ret.Add(new InsertNodeAnnotation { Position = pos });
                        _tokens[pos].TokenType = TSqlTokenType.MultilineComment;
                        _tokens[pos].Text = "/*__GRAPHVIEW_INSERT_NODE*/";
                        currentToken = nextToken;
                    }
                    else if (ReadToken(_tokens, "edge", ref nextToken, ref farestError))
                    {
                        var identifiers = new WMultiPartIdentifier();
                        if (!ReadToken(_tokens, "into", ref nextToken, ref farestError) ||
                            !ParseMultiPartIdentifier(_tokens, ref nextToken, ref identifiers, ref farestError))
                        {
                            var error = _tokens[farestError];
                            errors.Add(new ParseError(0, error.Offset, error.Line, error.Column, "Incorrect syntax near edge"));
                            return null;
                        }

                        ret.Add(new InsertEdgeAnnotation
                        {
                            Position = pos,
                            EdgeColumn = identifiers.Identifiers.Last(),
                        });
                        var lastColumnIndex = identifiers.Identifiers.Last().LastTokenIndex;
                        _tokens[pos].TokenType = TSqlTokenType.MultilineComment;
                        _tokens[pos].Text = "/*__GRAPHVIEW_INSERT_EDGE*/";
                        if (identifiers.Identifiers.Count == 1)
                        {

                            _tokens[lastColumnIndex].TokenType = TSqlTokenType.MultilineComment;
                            _tokens[lastColumnIndex].Text = "/*__GRAPHVIEW_INSERT_EDGE*/";
                        }
                        else
                        {
                            var firstColumnIndex =
                                identifiers.Identifiers[identifiers.Identifiers.Count - 2].LastTokenIndex + 1;
                            for (var i = firstColumnIndex; i <= lastColumnIndex; ++i)
                            {
                                _tokens[i].TokenType = TSqlTokenType.MultilineComment;
                                _tokens[i].Text = "/*__GRAPHVIEW_INSERT_EDGE*/";
                            }
                        }
                    }
                    currentToken = nextToken;
                }
                else if (ReadToken(_tokens, "delete", ref nextToken, ref farestError))
                {
                    var pos = nextToken;
                    if (ReadToken(_tokens, "node", ref nextToken, ref farestError))
                    {
                        ret.Add(new DeleteNodeAnnotation
                        {
                            Position = pos,
                        });
                        _tokens[pos].TokenType = TSqlTokenType.MultilineComment;
                        _tokens[pos].Text = "/*__GRAPHVIEW_DELETE_NODE*/";
                        currentToken = nextToken;
                    }
                    else if (ReadToken(_tokens, "edge", ref nextToken, ref farestError))
                    {
                        WMatchPath path = null;
                        if (!ParseMatchPath(_tokens, ref nextToken, ref path, ref farestError))
                        {
                            var error = _tokens[farestError];
                            errors.Add(new ParseError(0, error.Offset, error.Line, error.Column, ""));
                            return null;
                        }
                        else if (path.PathEdgeList.Count != 1)
                        {
                            var error = _tokens[nextToken];
                            errors.Add(new ParseError(0, error.Offset, error.Line, error.Column, 
                                "Incorrect Syntax Near edge: 1-Height Pattern should be used in Delete Edge statement"));
                            return null;
                        }
                        ret.Add(new DeleteEdgeAnnotation
                        {
                            Position = currentToken,
                            Path = path
                        });

                        _tokens[currentToken].TokenType = TSqlTokenType.Select;
                        _tokens[currentToken].Text = "SELECT";

                        _tokens[pos].TokenType = TSqlTokenType.Identifier;
                        _tokens[pos].Text = path.PathEdgeList[0].Item1.BaseIdentifier.Value;

                        _tokens[pos + 1].TokenType = TSqlTokenType.Comma;
                        _tokens[pos + 1].Text = ",";

                        _tokens[pos + 2].TokenType = TSqlTokenType.Identifier;
                        _tokens[pos + 2].Text = path.Tail.BaseIdentifier.Value;

                        for (var i = path.FirstTokenIndex + 1; i < path.LastTokenIndex; ++i)
                        {
                            _tokens[i].TokenType = TSqlTokenType.MultilineComment;
                            _tokens[i].Text = "/*__GRAPHVIEW_DELETE_EDGE*/";
                        }
                        currentToken = nextToken;
                    }
                }
                currentToken++;
            }
            
            return ret;
        }

        /// <summary>
        /// Parses a GraphView query into a syntax tree. The parser re-uses the T-SQL parser by 
        /// masking graph-extended query constructs with comments first and then putting them back  
        /// into the syntax tree. 
        /// </summary>
        /// <param name="queryInput">The query string</param>
        /// <param name="errors">A list of parsing errors</param>
        /// <returns>The syntax tree of the input query</returns>
        public WSqlFragment Parse(TextReader queryInput, out IList<ParseError> errors)
        {
            var parser = new WSqlParser();
            //IList<ParseError> tokenParseErrors;
            _tokens = new List<TSqlParserToken>(parser.tsqlParser.GetTokenStream(queryInput, out errors));
            if (errors.Count > 0)
                return null;

            // Removes comments
            _tokens.RemoveAll(x => x.TokenType == TSqlTokenType.MultilineComment);
            _tokens.RemoveAll(x => x.TokenType == TSqlTokenType.SingleLineComment);
            var annotations = FindReplaceGraphModificationStatements(ref errors);
            ExtractMatchClause();

            // Parses the transformed script into a standard SQL syntax tree
            // using the T-SQL parser
            var script = parser.Parse(_tokens, out errors) as WSqlScript;
            if (errors.Count > 0)
                return null;
            // Converts data modification statements back to graph modification statements,
            // if they are artificial products of graph modification statements.  
            var convertStatmentVisitor = new ConvertToModificationStatementVisitor();
            convertStatmentVisitor.Invoke(script,annotations);
            if (script == null)
                return null;
            // Puts the MATCH clause(s) into the syntax tree
            var matchClauseVisitor = new MatchClauseVisitor
            {
                MatchList = _matchList,
                MatchFlag = _matchFlag,
                Tokens = _tokens
            };
            matchClauseVisitor.Invoke(script, ref errors);
            if (errors.Count > 0)
                return null;
                
            return script;
        }
    }


}