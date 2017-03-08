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

        //internal override void InV(GremlinToSqlContext currentContext)
        //{
        //    var path = currentContext.GetPathFromPathList(this);
        //    if (path?.SinkVariable != null)
        //    {
        //        if (currentContext.IsVariableInCurrentContext(path.SinkVariable))
        //        {
        //            currentContext.SetPivotVariable(path.SinkVariable);
        //        }
        //        else
        //        {
        //            GremlinContextVariable newContextVariable = new GremlinContextVariable(path.SinkVariable);
        //            currentContext.VariableList.Add(newContextVariable);
        //            currentContext.SetPivotVariable(newContextVariable);
        //        }
        //    }
        //    else
        //    {
        //        GremlinVariableProperty sinkProperty = GetVariableProperty(GremlinKeyword.EdgeSinkV);
        //        GremlinTableVariable inVertex = new GremlinBoundVertexVariable(sinkProperty);
        //        path?.SetSinkVariable(inVertex);

        //        currentContext.VariableList.Add(inVertex);
        //        currentContext.TableReferences.Add(inVertex);
        //        currentContext.SetPivotVariable(inVertex);
        //    }
        //}

        //internal override void OutV(GremlinToSqlContext currentContext)
        //{
        //    var path = currentContext.GetPathFromPathList(this);

        //    if (path?.SourceVariable != null)
        //    {
        //        if (currentContext.IsVariableInCurrentContext(path.SourceVariable))
        //        {
        //            currentContext.SetPivotVariable(path.SourceVariable);
        //        }
        //        else
        //        {
        //            GremlinContextVariable newContextVariable = new GremlinContextVariable(path.SourceVariable);
        //            currentContext.VariableList.Add(newContextVariable);
        //            currentContext.SetPivotVariable(newContextVariable);
        //        }
        //    }
        //    else
        //    {
        //        GremlinVariableProperty sourceProperty = GetVariableProperty(GremlinKeyword.EdgeSourceV);
        //        GremlinTableVariable outVertex = new GremlinBoundVertexVariable(sourceProperty);
        //        path?.SetSourceVariable(outVertex);

        //        currentContext.VariableList.Add(outVertex);
        //        currentContext.TableReferences.Add(outVertex);
        //        currentContext.SetPivotVariable(outVertex);
        //    }
        //}
    }
}
