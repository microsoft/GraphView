using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinDropVariable : GremlinNULLTableVariable
    {
        public GremlinVariable DroppedVariable { get; set; }

        public GremlinDropVariable(GremlinVariable droppedVariable)
        {
            this.DroppedVariable = droppedVariable;
        }

        internal override bool Populate(string property, string label = null)
        {
            // Because the traversal yields no outgoing objects.
            return false;
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(this.DroppedVariable.FetchAllVars());
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(this.DroppedVariable.DefaultProjection().ToScalarExpression());
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Drop, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
