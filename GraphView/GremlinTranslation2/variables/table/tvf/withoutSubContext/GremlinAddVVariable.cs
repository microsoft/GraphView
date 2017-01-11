using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinAddVVariable: GremlinTableVariable
    {
        public Dictionary<string, object> Properties { get; set; }
        public string VertexLabel { get; set; }
        public bool IsFirstTableReference { get; set; }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            if (VertexLabel != null)
            {
                parameters.Add(SqlUtil.GetValueExpr(GremlinKeyword.Label));
                parameters.Add(SqlUtil.GetValueExpr(VertexLabel));
            }
            foreach (var property in Properties)
            {
                parameters.Add(SqlUtil.GetValueExpr(property.Key));
                parameters.Add(SqlUtil.GetValueExpr(property.Value));
            }
            var firstTableRef = IsFirstTableReference ? SqlUtil.GetDerivedTable(SqlUtil.GetSimpleSelectQueryBlock("1"), "_") : null;
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.AddV, parameters, this, VariableName);

            return SqlUtil.GetCrossApplyTableReference(firstTableRef, secondTableRef);
        }

        public GremlinAddVVariable(string vertexLabel, bool isFirstTableReference = false)
        {
            Properties = new Dictionary<string, object>();
            VertexLabel = vertexLabel;
            IsFirstTableReference = isFirstTableReference;
        }

        public GremlinAddVVariable()
        {
            Properties = new Dictionary<string, object>();
        }

        internal override void Property(GremlinToSqlContext currentContext, Dictionary<string, object> properties)
        {
            foreach (var pair in properties)
            {
                Properties[pair.Key] = pair.Value;
            }
        }
    }
}
