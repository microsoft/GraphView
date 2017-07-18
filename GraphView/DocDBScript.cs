using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    public class DMultiPartIdentifier : WMultiPartIdentifier
    { 
        public DMultiPartIdentifier(WMultiPartIdentifier identifier)
        {
            Identifiers = identifier.Identifiers;
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
    }
}
