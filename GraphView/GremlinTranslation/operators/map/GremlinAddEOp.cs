using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinAddEOp: GremlinTranslationOperator
    {
        internal string EdgeLabel { get; set; }
        public GraphTraversal FromVertexTraversal { get; set; }
        public GraphTraversal ToVertexTraversal { get; set; }
        public List<GremlinProperty> EdgeProperties { get; set; }

        public GremlinAddEOp(string label)
        {
            EdgeLabel = label;
            EdgeProperties = new List<GremlinProperty>();
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            FromVertexTraversal?.GetStartOp().InheritedVariableFromParent(inputContext);
            GremlinToSqlContext fromVertexContext = FromVertexTraversal?.GetEndOp().GetContext();

            ToVertexTraversal?.GetStartOp().InheritedVariableFromParent(inputContext);
            GremlinToSqlContext toVertexContext = ToVertexTraversal?.GetEndOp().GetContext();

            if (inputContext.PivotVariable == null)
            {
                if (fromVertexContext == null || toVertexContext == null)
                {
                    throw new SyntaxErrorException("The PivotVariable of addE()-step and fromTraversal( or toTraversal) can't be null at the same time.");
                }

                GremlinAddETableVariable newTableVariable = new GremlinAddETableVariable(null, EdgeLabel, EdgeProperties, fromVertexContext, toVertexContext, true);
                inputContext.VariableList.Add(newTableVariable);
                inputContext.TableReferencesInFromClause.Add(newTableVariable);
                inputContext.SetPivotVariable(newTableVariable);
            }
            else
            {
                inputContext.PivotVariable.AddE(inputContext, EdgeLabel, EdgeProperties, fromVertexContext, toVertexContext);
            }
            
            return inputContext;
        }
    }
}

