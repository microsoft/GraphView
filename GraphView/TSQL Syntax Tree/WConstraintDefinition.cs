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
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    public abstract partial class WConstraintDefinition : WSqlFragment
    {
        private Identifier _constraintIdentifier;
        public Identifier ConstraintIdentifier
        {
            get { return _constraintIdentifier; }
            set { UpdateTokenInfo(value); _constraintIdentifier = value; }
        }

        internal override bool OneLine()
        {
            return true;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder();
            sb.Append(indent);
            if (ConstraintIdentifier != null)
                sb.AppendFormat("CONSTRAINT {0} ", ConstraintIdentifier.Value);
            return sb.ToString();
        }
    }

    public partial class WCheckConstraintDefinition : WConstraintDefinition
    {
        public WBooleanExpression CheckCondition { get; set; }
        public bool NotForReplication { get; set; }

        internal override bool OneLine()
        {
            return CheckCondition.OneLine();
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder();
            sb.Append(base.ToString(indent));
            sb.Append("CHECK ");

            if (NotForReplication)
                sb.Append(" NOT FOR REPLICATION");

            if (CheckCondition.OneLine())
            {
                sb.Append(CheckCondition);
            }
            else
            {
                sb.Append("\r\n");
                sb.AppendFormat(CultureInfo.CurrentCulture, CheckCondition.ToString(indent + " "));
            }

            return sb.ToString();
        }
    }

    public partial class WDefaultConstraintDefinition : WConstraintDefinition
    {
        private WScalarExpression _expression;
        private Identifier _column;
        public bool WithValues { get; set; }

        public WScalarExpression Expression
        {
            get { return _expression; }
            set { UpdateTokenInfo(value); _expression = value; }
        }


        public Identifier Column
        {
            get { return _column; }
            set { UpdateTokenInfo(value); _column = value; }
        }
        internal override string ToString(string indent)
        {
            var sb = new StringBuilder();
            sb.Append(base.ToString(indent));
            sb.AppendFormat("DEFAULT {0}", Expression);
            if (Column != null)
                sb.AppendFormat(" FOR {0}", Column.Value);
            if (WithValues)
                sb.Append(" WITH VALUES");
            return sb.ToString();
        }
    }

    public partial class WForeignKeyConstraintDefinition : WConstraintDefinition
    {
        private WSchemaObjectName _referenceTableName;
        public IList<Identifier> Columns { get; set; }
        public IList<Identifier> ReferencedTableColumns { get; set; }
        public DeleteUpdateAction DeleteAction { get; set; }
        public DeleteUpdateAction UpdateAction { get; set; }
        public bool NotForReplication { get; set; }
        public WSchemaObjectName ReferenceTableName
        {
            get { return _referenceTableName; }
            set { UpdateTokenInfo(value); _referenceTableName = value; }
        }
        internal override string ToString(string indent)
        {
            var sb = new StringBuilder();
            sb.Append(base.ToString(indent));
            sb.Append("FOREIGN KEY (");
            for (var i = 0; i < Columns.Count; ++i)
            {
                if (i > 0)
                    sb.Append(", ");
                sb.Append(Columns[i].Value);
            }
            sb.AppendFormat(") REFERENCES {0}", ReferenceTableName);
            if (ReferencedTableColumns != null && ReferencedTableColumns.Count != 0)
            {
                sb.Append(" (");
                for (var i = 0; i < ReferencedTableColumns.Count; ++i)
                {
                    if (i > 0)
                        sb.Append(", ");
                    sb.Append(ReferencedTableColumns[i].Value);
                }
                sb.Append(")");
            }
            sb.AppendFormat(" ON DELETE {0}", DeleteAction);
            sb.AppendFormat(" ON UPDATE {0}", UpdateAction);
            if (NotForReplication)
                sb.Append(" NOT FOR REPLICATION");
            return sb.ToString();
        }
    }

    public partial class WNullableConstraintDefinition : WConstraintDefinition
    {
        public bool Nullable { get; set; }

        internal override bool OneLine() { return true; }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder();
            sb.AppendFormat("{0}{1}", indent, Nullable ? "NULL" : "NOT NULL");
            return sb.ToString();
        }
    }

    public partial class WUniqueConstraintDefinition : WConstraintDefinition
    {
        public IList<Tuple<WColumnReferenceExpression, SortOrder>> Columns { get; set; }
        public bool? Clustered { get; set; }
        public bool IsPrimaryKey { get; set; }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder();
            sb.Append(base.ToString(indent));
            sb.Append(IsPrimaryKey ? "PRIMARY KEY" : "UNIQUE");
            if (Clustered != null)
                sb.Append(Clustered == true ? " CLUSTERED" : " NONCLUSTERED");
            if (Columns != null && Columns.Count > 0)
            {

                sb.Append(" (");
                for (var i = 0; i < Columns.Count; ++i)
                {
                    if (i > 0)
                        sb.Append(", ");
                    sb.Append(Columns[i].Item1);
                    switch (Columns[i].Item2)
                    {
                        case SortOrder.Ascending:
                            sb.Append(" ASC");
                            break;
                        case SortOrder.Descending:
                            sb.Append(" DESC");
                            break;
                    }
                }
                sb.Append(")");
            }
            return sb.ToString();
        }
    }

}
