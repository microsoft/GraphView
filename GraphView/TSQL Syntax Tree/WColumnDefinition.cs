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
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    public partial class WColumnDefinition : WSqlFragment
    {
        private Identifier _columnIdentifier;
        private WDataTypeReference _dataType;
        private Identifier _collation;

        public Identifier ColumnIdentifier
        {
            get { return _columnIdentifier; }
            set { UpdateTokenInfo(value); _columnIdentifier = value; }
        }

        public WDataTypeReference DataType
        {
            get { return _dataType; }
            set { UpdateTokenInfo(value); _dataType = value; }
        }

        public Identifier Collation
        {
            get { return _collation; }
            set { UpdateTokenInfo(value); _collation = value; } }

        public bool IsRowGuidCol { get; set; }

        public IList<WConstraintDefinition> Constraints { get; set; }
        private WScalarExpression _computedColumnExpression;
        private WDefaultConstraintDefinition _defaultConstraint;
        private WIdentityOptions _identityOptions;
        private ColumnStorageOptions _storageOptions;
        private WIndexDefinition _index;

        public WScalarExpression ComputedColumnExpression
        {
            get { return _computedColumnExpression; }
            set { UpdateTokenInfo(value); _computedColumnExpression = value; }
        }

        public WDefaultConstraintDefinition DefaultConstraint
        {
            get { return _defaultConstraint; }
            set { UpdateTokenInfo(value); _defaultConstraint = value; }
        }

        public WIdentityOptions IdentityOptions
        {
            get { return _identityOptions; }
            set { UpdateTokenInfo(value); _identityOptions = value; }
        }

        public ColumnStorageOptions StorageOptions
        {
            get { return _storageOptions; }
            set { UpdateTokenInfo(value); _storageOptions = value; }
        }

        public WIndexDefinition Index
        {
            get { return _index; }
            set { UpdateTokenInfo(value); _index = value; }
        }

        internal override bool OneLine()
        {
            return ComputedColumnExpression == null || ComputedColumnExpression.OneLine();
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder();
            sb.AppendFormat("{0}[{1}]", indent, ColumnIdentifier.Value);
            if (ComputedColumnExpression != null)
                sb.AppendFormat(" {0}", ComputedColumnExpression);
            else
                sb.AppendFormat(" {0}", DataType);

            if (StorageOptions != null)
            {
                if (StorageOptions.IsFileStream)
                    sb.Append(" FILESTREAM");
                if (StorageOptions.SparseOption == SparseColumnOption.Sparse)
                    sb.Append(" SPARSE");
            }
            if (Collation != null)
                sb.AppendFormat(" COLLATE {0}", Collation.Value);
            if (DefaultConstraint != null)
                sb.AppendFormat(" {0}", DefaultConstraint);
            if (IdentityOptions != null)
                sb.AppendFormat(" {0}", IdentityOptions);
            if (IsRowGuidCol)
                sb.Append(" ROWGUIDCOL");
            if (Constraints != null)
                foreach (var t in Constraints)
                    sb.AppendFormat(" {0}", t);
            if (Index != null)
                sb.AppendFormat(" {0}", Index);
            return sb.ToString();
        }
    }

}
