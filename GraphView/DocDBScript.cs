using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    public class DMultiPartIdentifier
    {
        public IList<Identifier> Identifiers { get; set; }

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
            Identifiers = new List<Identifier>();
            Identifiers.Add(new Identifier {Value = identifier});
        }

        public Identifier this[int index]
        {
            get { return Identifiers[index]; }
            set { Identifiers[index] = value; }
        }

        public int Count
        {
            get { return Identifiers.Count; }
        }

        // DocumentDB Identifier Normalization
        public override string ToString()
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

            sb.Append(ScriptBase.Replace("node", FromClause.TableReference));

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
                sb.Append(" " + WhereClause.ToString());

            return sb.ToString();
        }
    }
}
