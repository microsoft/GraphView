using System;
using System.Collections.Generic;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal abstract class GremlinTranslationOperator
    {
        public List<string> Labels { get; set; }
        public GremlinTranslationOperator InputOperator { get; set; }

        public virtual GremlinToSqlContext GetContext()
        {
            return null;
        }

        public GremlinToSqlContext GetInputContext()
        {
            if (InputOperator != null) {
                GremlinToSqlContext context = InputOperator.GetContext();
                return context;
            } else {
                return new GremlinToSqlContext();
            }
        }

        public virtual WSqlScript ToSqlScript() {
            return GetContext().ToSqlScript();
        }

        public List<string> GetLabels()
        {
            return Labels;
        }

        public void ClearLabels()
        {
            Labels.Clear();
        }
    }
    
    internal class GremlinParentContextOp : GremlinTranslationOperator
    {
        public GremlinVariable2 InheritedPivotVariable { get; set; }
        public GremlinToSqlContext InheritedContext { get; set; }
        public Dictionary<string, List<Tuple<GremlinVariable2, GremlinToSqlContext>>> InheritedTaggedVariables { get;
            set; }

        public GremlinParentContextOp()
        {
            InheritedTaggedVariables = new Dictionary<string, List<Tuple<GremlinVariable2, GremlinToSqlContext>>>();   
        }

        public override GremlinToSqlContext GetContext()
        {
            if (InheritedContext != null) return InheritedContext;
            GremlinToSqlContext newContext = new GremlinToSqlContext();
            newContext.TaggedVariables = InheritedTaggedVariables;
            if (InheritedPivotVariable != null)
            {
                if (InheritedPivotVariable.GetVariableType() == GremlinVariableType.Vertex)
                {
                    var newVertex = new GremlinContextVertexVariable(InheritedPivotVariable);
                    newContext.VariableList.Add(newVertex);
                    newContext.PivotVariable = newVertex;
                }
                else if (InheritedPivotVariable.GetVariableType() == GremlinVariableType.Edge)
                {
                    var newEdge = new GremlinContextEdgeVariable(InheritedPivotVariable);
                    newContext.VariableList.Add(newEdge);
                    newContext.PivotVariable = newEdge;
                }
                else if (InheritedPivotVariable.GetVariableType() == GremlinVariableType.Table) 
                {
                    var newEdge = new GremlinContextTableVariable(InheritedPivotVariable);
                    newContext.VariableList.Add(newEdge);
                    newContext.PivotVariable = newEdge;
                }
            } 
            return newContext;
        }
    }
}
