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
        public bool IsInheritedEntireContext { get; set; }
        public GremlinToSqlContext InheritedContext { get; set; }
        public Dictionary<string, List<Tuple<GremlinVariable2, GremlinToSqlContext>>> InheritedTaggedVariables;

        public GremlinParentContextOp()
        {
            InheritedTaggedVariables = new Dictionary<string, List<Tuple<GremlinVariable2, GremlinToSqlContext>>>();   
        }

        public void SetContext(GremlinToSqlContext context)
        {
            IsInheritedEntireContext = true;
            InheritedContext = context;
        }

        public override GremlinToSqlContext GetContext()
        {
            if (IsInheritedEntireContext) return InheritedContext;
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
                else
                {
                    throw new NotImplementedException();
                }
            } 
            return newContext;
        }
    }
}
