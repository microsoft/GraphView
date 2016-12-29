using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslation
{
    internal class GremlinTVFEdgeVariable : GremlinEdgeVariable
    {
        public WUnqualifiedJoin TableReference { get; set; }

        public GremlinTVFEdgeVariable(WUnqualifiedJoin tableReference, WEdgeType edgeType) : base(edgeType)
        {
            TableReference = tableReference;
            EdgeType = edgeType;
            (TableReference.SecondTableRef as WSchemaObjectFunctionTableReference).Alias =
                GremlinUtil.GetIdentifier(VariableName);
        }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Table;
        }
    }
}
