using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    //internal class GremlinPath2Variable : GremlinScalarTableVariable
    //{
    //    public List<GremlinVariableProperty> PathList { get; set; }
    //    public bool IsInRepeatContext { get; set; }
    //    public List<GremlinToSqlContext> ByContexts { get; set; }

    //    public GremlinPath2Variable(List<GremlinVariableProperty> pathList, List<GremlinToSqlContext> byContexts)
    //    {
    //        this.PathList = pathList;
    //        IsInRepeatContext = false;
    //        ByContexts = byContexts;
    //    }

    //    public override WTableReference ToTableReference()
    //    {
    //        List<WScalarExpression> parameters = new List<WScalarExpression>();
    //        List<WSelectQueryBlock> queryBlocks = new List<WSelectQueryBlock>();

    //        //Must toSelectQueryBlock before toCompose1 of variableList in order to populate needed columns
    //        foreach (var byContext in ByContexts)
    //        {
    //            //TODO: select compose1
    //            queryBlocks.Add(byContext.ToSelectQueryBlock());
    //        }

    //        if (IsInRepeatContext)
    //        {
    //            //Must add as the first parameter
    //            parameters.Add(SqlUtil.GetColumnReferenceExpr("R", GremlinKeyword.Path));
    //        }
    //        foreach (var path in PathList)
    //        {
    //            if (path.VariableProperty == GremlinKeyword.Path)
    //            {
    //                parameters.Add(path.ToScalarExpression());
    //            }
    //            else
    //            {
    //                parameters.Add(path.GremlinVariable.ToCompose1());
    //            }
    //        }

    //        foreach (var block in queryBlocks)
    //        {
    //            parameters.Add(SqlUtil.GetScalarSubquery(block));
    //        }

    //        var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Path2, parameters, GetVariableName());
    //        return SqlUtil.GetCrossApplyTableReference(tableRef);
    //    }
    //}
}
