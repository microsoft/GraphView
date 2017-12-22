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
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using GraphView.GraphViewDBPortal;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    [Serializable]
    public partial class WMultiPartIdentifier : WSqlFragment, ISerializable
    {
        public IList<Identifier> Identifiers { get; set; }

        public WMultiPartIdentifier(params Identifier[] identifiers)
        {
            Identifiers = identifiers.ToList();
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

        internal override bool OneLine()
        {
            return true;
        }

        internal override string ToString(string indent)
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

        internal override string ToString(string indent, bool useSquareBracket)
        {
            var sb = new StringBuilder(16);

            for (var i = 0; i < Identifiers.Count; i++)
            {
                if (i > 0)
                {
                    sb.Append('.');
                }
                sb.Append("[" + Identifiers[i].Value + "]");
            }

            return sb.ToString();
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            GraphViewSerializer.SerializeList(info, "Identifiers", this.Identifiers.ToList());
        }

        protected WMultiPartIdentifier(SerializationInfo info, StreamingContext context)
        {
            this.Identifiers = GraphViewSerializer.DeserializeList<Identifier>(info, "Identifiers");
        }
    }

    public partial class WSchemaObjectName : WMultiPartIdentifier
    {
        private const int ServerModifier = 4;
        private const int DatabaseModifier = 3;
        private const int SchemaModifier = 2;
        private const int BaseModifier = 1;

        public WSchemaObjectName(params Identifier[] identifiers)
        {
            Identifiers = identifiers;
        }

        public virtual Identifier ServerIdentifier
        {
            get { return ChooseIdentifier(ServerModifier); }
        }

        public virtual Identifier DatabaseIdentifier
        {
            get { return ChooseIdentifier(DatabaseModifier); }
        }

        public virtual Identifier SchemaIdentifier
        {
            get { return ChooseIdentifier(SchemaModifier); }
        }

        public virtual Identifier BaseIdentifier
        {
            get { return ChooseIdentifier(BaseModifier); }
        }

        protected Identifier ChooseIdentifier(int modifier)
        {
            var index = Identifiers.Count - modifier;
            return index < 0 ? null : Identifiers[index];
        }
    }
}
