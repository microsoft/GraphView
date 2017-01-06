using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinContextEdgeVariable : GremlinContextVariable
    {
        public bool IsFromSelect;
        public GremlinKeyword.Pop Pop;
        public string SelectKey;

        public GremlinContextEdgeVariable(GremlinVariable contextEdge):base(contextEdge) {}

        internal override GremlinVariableType GetVariableType()
        {
            return ContextVariable.GetVariableType();
        }

        internal override GremlinScalarVariable DefaultProjection()
        {
            return ContextVariable.DefaultProjection();
        }

        internal override void Populate(string property)
        {
            ContextVariable.Populate(property);
        }

        internal override void InV(GremlinToSqlContext currentContext)
        {
            ContextVariable.Populate("_sink");
            GremlinBoundVertexVariable newVariable = new GremlinBoundVertexVariable(new GremlinVariableProperty(ContextVariable, "_sink"));
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.PivotVariable = newVariable;
        }

        internal override void OutV(GremlinToSqlContext currentContext)
        {
            // A naive implementation: always introduce a new vertex variable
            ContextVariable.Populate("_sink");
            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(new GremlinVariableProperty(ContextVariable, "_sink"));
            currentContext.VariableList.Add(outVertex);
            currentContext.TableReferences.Add(outVertex);
            currentContext.PivotVariable = outVertex;
        }

        internal override void OtherV(GremlinToSqlContext currentContext)
        {
            ContextVariable.OtherV(currentContext);
        }
    }
}
