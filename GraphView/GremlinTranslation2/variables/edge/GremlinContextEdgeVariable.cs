using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinContextEdgeVariable : GremlinEdgeVariable2
    {
        public GremlinVariable2 ContextVariable;

        public bool IsFromSelect;
        public GremlinKeyword.Pop Pop;
        public string SelectKey;

        public GremlinContextEdgeVariable(GremlinVariable2 contextEdge)
        {
            this.ContextVariable = contextEdge;
            VariableName = this.ContextVariable.VariableName;
        }

        public override GremlinVariableType GetVariableType()
        {
            return ContextVariable.GetVariableType();
        }

        internal override GremlinScalarVariable DefaultProjection()
        {
            return ContextVariable.DefaultProjection();
        }

        internal override void Populate(string name, bool isAlias = false)
        {
            ContextVariable.Populate(name, isAlias);
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
    }
}
