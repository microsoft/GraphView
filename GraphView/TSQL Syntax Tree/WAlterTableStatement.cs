using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    public class WAlterTableStatement : WSqlStatement
    {
        private WSchemaObjectName _schemaObjectName;

        public WSchemaObjectName SchemaObjectName
        {
            get { return _schemaObjectName; }
            set { UpdateTokenInfo(value); _schemaObjectName = value; }
        }

        internal override bool OneLine()
        {
            return true;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(1024);
            sb.AppendFormat("{0}ALTER TABLE {1}\r\n", indent, SchemaObjectName);
            return sb.ToString();
        }
    }


    /// <summary>
    /// Syntax tree of a ALTER TABLE Add Table Element statement
    /// </summary>
    public class WAlterTableAddTableElementStatement : WAlterTableStatement
    {
        // Omit ExistingRowsCheckEnforcement

        private WTableDefinition _definition;
        public WTableDefinition Definition
        {
            get { return _definition; }
            set { UpdateTokenInfo(value); _definition = value; }
        }

        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(1024);
            sb.AppendFormat("{0}ALTER TABLE {1}\r\n", indent, SchemaObjectName);
            if (_definition != null)
            {
                sb.AppendFormat("{0}ADD{1}\r\n", indent, Definition.ToString(" ", indent + "    "));
            }
            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }
    }

    public class WAlterTableDropTableElement : WSqlFragment
    {
        // Omit DropClusteredConstraintOptions

        public Identifier Name { get; set; }
        public TableElementType TableElementType { get; set; }

        internal override bool OneLine()
        {
            return true;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(1024);
            sb.AppendFormat("{0}{1}{2}", indent, TableElementType == TableElementType.NotSpecified 
                                                 ? "" : TableElementType + " ", Name.Value);
            return sb.ToString();
        }
    }

    public class WAlterTableDropTableElementStatement : WAlterTableStatement
    {
        public IList<WAlterTableDropTableElement> AlterTableDropTableElements { get; set; }

        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(1024);
            sb.AppendFormat("{0}ALTER TABLE {1}\r\nDROP", indent, SchemaObjectName);
            var first = true;
            foreach (var element in AlterTableDropTableElements)
            {
                if (first)
                    first = false;
                else
                    sb.Append(",");
                sb.Append(element.ToString(" "));
            }
            sb.Append("\r\n");
            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }
    }
}
