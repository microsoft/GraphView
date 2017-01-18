using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    //internal class GremlinByOp: GremlinTranslationOperator
    //{
    //    private ByType Type;
    //    public string ByKey { get; set; }
    //    public GremlinKeyword.Order ByOrder { get; set; }
    //    public GraphTraversal2 ByTraversal { get; set; }

    //    public GremlinByOp(string key)
    //    {
    //        ByKey = key;
    //        Type = ByType.ByKey;
    //    }

    //    public GremlinByOp(GraphTraversal2 byTraversal2)
    //    {
    //        ByTraversal = byTraversal2;
    //        Type = ByType.ByTraversal;
    //    }

    //    public GremlinByOp(GremlinKeyword.Order order)
    //    {
    //        ByOrder = order;
    //        Type = ByType.ByOrder;
    //    }

    //    public GremlinByOp()
    //    {
    //        Type = ByType.Default;
    //    }

    //    internal override GremlinToSqlContext GetContext()
    //    {
    //        GremlinToSqlContext inputContext = GetInputContext();

    //        switch (Type)
    //        {
    //            case ByType.ByOrder:
    //                throw new NotImplementedException();
    //            case ByType.ByKey:
    //                inputContext.PivotVariable.By(inputContext, ByKey);
    //                break;
    //            case ByType.ByTraversal:
    //                //SqlUtil.InheritedVariableFromParent(ByTraversal, inputContext);
    //                //GremlinToSqlContext byContext = ByTraversal.GetEndOp().GetContext();
    //                inputContext.PivotVariable.By(inputContext, ByTraversal);
    //                break;
    //            default:
    //                inputContext.PivotVariable.By(inputContext);
    //                break;
    //        }

    //        return inputContext;
    //    }

    //    private enum ByType
    //    {
    //        Default,
    //        ByKey,
    //        ByOrder,
    //        ByTraversal
    //    }
    //}
}
