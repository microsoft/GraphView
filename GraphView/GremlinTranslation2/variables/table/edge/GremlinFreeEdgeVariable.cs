using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinFreeEdgeVariable : GremlinEdgeTableVariable
    {
        public GremlinFreeEdgeVariable(WEdgeType edgeType)
        {
            EdgeType = edgeType;
        }

        internal override GremlinTableVariable CreateAdjVertex(GremlinVariableProperty propertyVariable)
        {
            return new GremlinFreeVertexVariable();
        }

        //internal override void InV(GremlinToSqlContext currentContext)
        //{
        //    switch (GetEdgeType())
        //    {
        //        case WEdgeType.BothEdge:
        //            GremlinVariableProperty sinkProperty = new GremlinVariableProperty(this, GremlinKeyword.EdgeSinkV);
        //            GremlinBoundVertexVariable inVertex = new GremlinBoundVertexVariable(GetEdgeType(), sinkProperty);
        //            currentContext.VariableList.Add(inVertex);
        //            currentContext.TableReferences.Add(inVertex);
        //            currentContext.SetPivotVariable(inVertex);
        //            break;
        //        case WEdgeType.OutEdge:
        //        case WEdgeType.InEdge:
        //            var path = currentContext.GetPathFromPathList(this);
        //            if (path != null && path.SinkVariable != null)
        //            {
        //                if (currentContext.IsVariableInCurrentContext(path.SinkVariable))
        //                {
        //                    currentContext.SetPivotVariable(path.SinkVariable);
        //                }
        //                else
        //                {
        //                    GremlinContextVariable newContextVariable = GremlinContextVariable.Create(path.SinkVariable);
        //                    currentContext.VariableList.Add(newContextVariable);
        //                    currentContext.SetPivotVariable(newContextVariable);
        //                }
        //            }
        //            else
        //            {
        //                GremlinFreeVertexVariable freeInVertex = new GremlinFreeVertexVariable();
        //                path.SetSinkVariable(freeInVertex);

        //                currentContext.VariableList.Add(freeInVertex);
        //                currentContext.TableReferences.Add(freeInVertex);
        //                currentContext.SetPivotVariable(freeInVertex);
        //            }
        //            break;
        //    }
        //}

        //internal override void OutV(GremlinToSqlContext currentContext)
        //{
        //    // A naive implementation would be: add a new bound/free vertex to the variable list. 
        //    // A better implementation should reason the status of the edge variable and only reset
        //    // the pivot variable if possible, thereby avoiding adding a new vertex variable  
        //    // and reducing one join.

        //    switch (EdgeType)
        //    {
        //        case WEdgeType.BothEdge:
        //            GremlinVariableProperty sourceProperty = new GremlinVariableProperty(this, GremlinKeyword.EdgeSourceV);
        //            GremlinBoundVertexVariable newVertex = new GremlinBoundVertexVariable(GetEdgeType(), sourceProperty);
        //            currentContext.VariableList.Add(newVertex);
        //            currentContext.TableReferences.Add(newVertex);
        //            currentContext.SetPivotVariable(newVertex);
        //            break;
        //        case WEdgeType.OutEdge:
        //        case WEdgeType.InEdge:
        //            var path = currentContext.GetPathFromPathList(this);

        //            if (path != null && path.SourceVariable != null)
        //            {
        //                if (currentContext.IsVariableInCurrentContext(path.SourceVariable))
        //                {
        //                    currentContext.SetPivotVariable(path.SourceVariable);
        //                }
        //                else
        //                {
        //                    GremlinContextVariable newContextVariable = GremlinContextVariable.Create(path.SourceVariable);
        //                    currentContext.VariableList.Add(newContextVariable);
        //                    currentContext.SetPivotVariable(newContextVariable);
        //                }
        //            }
        //            else
        //            {
        //                GremlinFreeVertexVariable freeOutVertex = new GremlinFreeVertexVariable();
        //                path.SetSourceVariable(freeOutVertex);

        //                currentContext.VariableList.Add(freeOutVertex);
        //                currentContext.TableReferences.Add(freeOutVertex);
        //                currentContext.SetPivotVariable(freeOutVertex);
        //            }
        //            break;
        //    }
        //}

        //internal override void OtherV(GremlinToSqlContext currentContext)
        //{
        //    switch (EdgeType)
        //    {
        //        case WEdgeType.BothEdge:
        //            Populate(GremlinKeyword.EdgeOtherV);
        //            GremlinVariableProperty otherProperty = new GremlinVariableProperty(this, GremlinKeyword.EdgeOtherV);
        //            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(GetEdgeType(), otherProperty);
        //            currentContext.VariableList.Add(outVertex);
        //            currentContext.TableReferences.Add(outVertex);
        //            currentContext.SetPivotVariable(outVertex);
        //            break;
        //        case WEdgeType.InEdge:
        //            OutV(currentContext);
        //            break;
        //        case WEdgeType.OutEdge:
        //            InV(currentContext);
        //            break;
        //    }
        //}

        internal override void InV(GremlinToSqlContext currentContext)
        {
            currentContext.InV(this);
        }

        internal override void OutV(GremlinToSqlContext currentContext)
        {
            currentContext.OutV(this);
        }

        internal override void OtherV(GremlinToSqlContext currentContext)
        {
            currentContext.OtherV(this);
        }
    }
}
