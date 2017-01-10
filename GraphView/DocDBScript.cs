using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    public class DMultiPartIdentifierParser
    {
        private WSqlParser _parser;
        public DMultiPartIdentifierParser()
        {
            _parser = new WSqlParser();
        }

        public DMultiPartIdentifier ParseMultiPartIdentifier(string input)
        {
            var sr = new StringReader(input);
            IList<ParseError> errors;
            List<TSqlParserToken> tokens = new List<TSqlParserToken>(_parser.tsqlParser.GetTokenStream(sr, out errors));
            if (errors.Count > 0)
                return null;

            var currentToken = 0;
            var farestError = 0;
            DMultiPartIdentifier result = new DMultiPartIdentifier();

            if (!ParseMultiPartIdentifier(tokens, ref currentToken, ref result, ref farestError))
                return null;

            return result;
        }

        private static bool ParseMultiPartIdentifier(
            IList<TSqlParserToken> tokens,
            ref int nextToken,
            ref DMultiPartIdentifier result,
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
            result = new DMultiPartIdentifier
            {
                Identifiers = identifiers,
            };
            nextToken = currentToken;
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
            Microsoft.SqlServer.TransactSql.ScriptDom.QuoteType qt;
            if (!ReadToken(tokens, TSqlTokenType.Identifier, ref identifierName, ref currentToken, ref farestError) &&
                !ReadToken(tokens, TSqlTokenType.QuotedIdentifier, ref identifierName, ref currentToken, ref farestError) &&
                !ReadToken(tokens, TSqlTokenType.AsciiStringOrQuotedIdentifier, ref identifierName, ref currentToken, ref farestError))
                return false;
            var decodedIdentifierName = Microsoft.SqlServer.TransactSql.ScriptDom.Identifier.DecodeIdentifier(identifierName, out qt);

            QuoteType quoteType = QuoteType.NotQuoted;
            switch (qt)
            {
                case Microsoft.SqlServer.TransactSql.ScriptDom.QuoteType.DoubleQuote:
                    quoteType = QuoteType.DoubleQuote;
                    break;
                case Microsoft.SqlServer.TransactSql.ScriptDom.QuoteType.SquareBracket:
                    quoteType = QuoteType.SquareBracket;
                    break;
                default:
                    quoteType = QuoteType.NotQuoted;
                    break;
            }

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
    }

    public class DMultiPartIdentifier : WMultiPartIdentifier
    { 
        public DMultiPartIdentifier(params Identifier[] identifiers)
        {
            Identifiers = identifiers.ToList();
        }

        public DMultiPartIdentifier(WMultiPartIdentifier identifier)
        {
            Identifiers = identifier.Identifiers;
        }

        public DMultiPartIdentifier(string identifier)
        {
            Identifiers = new List<Identifier> {new Identifier {Value = identifier}};
        }

        // DocumentDB Identifier Normalization
        public override string ToString()
        {
            var sb = new StringBuilder(16);

            for (var i = 0; i < Identifiers.Count; i++)
                sb.Append(i > 0 ? string.Format("[\"{0}\"]", Identifiers[i].Value) : Identifiers[i].Value);

            return sb.ToString();
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(16);

            for (var i = 0; i < Identifiers.Count; i++)
                sb.Append(i > 0 ? string.Format("[\"{0}\"]", Identifiers[i].Value) : Identifiers[i].Value);

            return sb.ToString();
        }

        public string ToSqlStyleString()
        {
            var sb = new StringBuilder(16);

            for (var i = 0; i < Identifiers.Count; i++)
            {
                if (i > 0)
                {
                    sb.Append('.');
                }
                sb.Append(Identifiers[i].Value);
            }

            return sb.ToString();
        }
    }

    public class DColumnReferenceExpression
    {
        public DMultiPartIdentifier MultiPartIdentifier { get; set; }
        public string ColumnName { get; set; }

        public string ToString(bool illegalCharReplace = true)
        {
            var identifier = MultiPartIdentifier.ToString();
            // Alias with dot and whitespace is illegal in documentDB, so they will be replaced by "_"
            return ColumnName != null
                ? string.Format(CultureInfo.CurrentCulture, "{0} AS {1}", identifier, illegalCharReplace ? ColumnName.Replace(".", "_").Replace(" ", "_") : ColumnName)
                : identifier;
        }

        public string ToSqlStyleString(bool illegalCharReplace = true)
        {
            var identifier = MultiPartIdentifier.ToSqlStyleString();
            // Alias with dot and whitespace is illegal in documentDB, so they will be replaced by "_"
            return ColumnName != null
                ? string.Format(CultureInfo.CurrentCulture, "{0} AS {1}", identifier, illegalCharReplace ? ColumnName.Replace(".", "_").Replace(" ", "_") : ColumnName)
                : identifier;
        }
    }

    public class DFromClause
    {
        public string TableReference { get; set; }
        public string FromClauseString { get; set; }

        public override string ToString()
        {
            return " FROM " + TableReference + " " + FromClauseString;
        }
    }

    public class DocDbScript
    {
        public string ScriptBase = "SELECT {\"id\":node.id, \"edge\":node._edge, \"reverse\":node._reverse_edge} AS NodeInfo";
        public List<DColumnReferenceExpression> SelectElements { get; set; }
        public DFromClause FromClause { get; set; }
        public WWhereClause WhereClause { get; set; }
        public WBooleanExpression OriginalSearchCondition { get; set; }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();

            sb.Append(ScriptBase);

            if (SelectElements != null)
            {
                foreach (var e in SelectElements)
                {
                    sb.Append(", ");
                    sb.Append(e.ToString());
                }
            }

            if (FromClause?.TableReference != null)
                sb.Append(FromClause.ToString());

            if (WhereClause?.SearchCondition != null)
            {
                var visitor = new DMultiPartIdentifierVisitor();
                visitor.Visit(WhereClause.SearchCondition);
                sb.Append(" " + WhereClause.ToString());
            }
                
            return sb.ToString();
        }
    }
}
