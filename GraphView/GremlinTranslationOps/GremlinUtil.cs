using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps
{
    public class GremlinUtil
    {
        internal static WColumnReferenceExpression GetColumnReferenceExpression(params string[] parts)
        {
            return new WColumnReferenceExpression()
            {
                MultiPartIdentifier = ConvertListToMultiPartIdentifier(parts)
            };
        }

        internal static WMultiPartIdentifier ConvertListToMultiPartIdentifier(string[] parts)
        {
            var MultiIdentifierList = new List<Identifier>();
            foreach (var part in parts)
            {
                MultiIdentifierList.Add(new Identifier() { Value = part });
            }
            return new WMultiPartIdentifier() { Identifiers = MultiIdentifierList };
        }

        internal static void CheckIsGremlinVertexVariable(GremlinVariable GremlinVar)
        {
            if (GremlinVar.GetType() != typeof(GremlinVertexVariable))
            {
                throw new Exception("It's not a GremlinVertexVariable");
            }
        }

        internal static void CheckIsGremlinEdgeVariable(GremlinVariable GremlinVar)
        {
            if (GremlinVar.GetType() != typeof(GremlinEdgeVariable)) {
                throw new Exception("It's not a GremlinEdgeVariable");
            }
        }
    }
}
