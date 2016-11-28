using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView.GremlinTranslationOps
{
    internal class GremlinVariable
    {
        public string VariableName { get; set; }
        public long Low;
        public long High;

        public GremlinVariable()
        {
            Low = Int64.MinValue;
            High = Int64.MaxValue;
        }

        public override int GetHashCode()
        {
            return VariableName.GetHashCode();
        }
    }

    internal class GremlinVertexVariable : GremlinVariable
    {
        public GremlinVertexVariable()
        {
            //automaticlly generate the name of node
            VariableName = "N_" + _count.ToString();
            _count += 1;
        }
        private static long _count = 0;
    }
    internal class GremlinEdgeVariable : GremlinVariable
    {
        public GremlinEdgeVariable()
        {
            //automaticlly generate the name of edge
            VariableName = "E_" + _count.ToString();
            _count += 1;
        }

        public GremlinEdgeVariable(WEdgeType type)
        {
            //automaticlly generate the name of edge
            VariableName = "E_" + _count.ToString();
            _count += 1;
            EdgeType = type;
        }

        private static long _count = 0;
        public WEdgeType EdgeType { get; set; }
    }

    internal class GremlinRecursiveEdgeVariable : GremlinVariable
    {
        public WSelectQueryBlock GremlinTranslationOperatorQuery { get; set; }
        public int IterationCount;
        public WBooleanExpression UntilCondition { get; set; }
    }

    //internal class GremlinJoinVertexVariable : GremlinVertexVariable
    //{
    //    public GremlinVariable LeftVariable;
    //    public GremlinVariable RightVariable;
    //    public GremlinJoinVertexVariable(GremlinVariable leftGremlinVariable, GremlinVariable righGremlinVariable)
    //    {
    //        LeftVariable = leftGremlinVariable;
    //        RightVariable = righGremlinVariable;

    //        //automaticlly generate the name of node
    //        VariableName = "JV_" + _count.ToString();
    //        _count += 1;
    //    }
    //    private static long _count = 0;
    //}

    //internal class GremlinJoinEdgeVariable : GremlinEdgeVariable
    //{
    //    public GremlinVariable LeftVariable;
    //    public GremlinVariable RightVariable;
    //    public GremlinJoinEdgeVariable(GremlinVariable leftGremlinVariable, GremlinVariable righGremlinVariable)
    //    {
    //        LeftVariable = leftGremlinVariable;
    //        RightVariable = righGremlinVariable;

    //        //automaticlly generate the name of node
    //        VariableName = "JE_" + _count.ToString();
    //        _count += 1;
    //    }
    //    private static long _count = 0;
    //}

    internal class GremlinDerivedVariable: GremlinVariable
    {
        //public WSelectQueryBlock SelectQueryBlock;
        public WTableReferenceWithAliasAndColumns QueryDerivedTable;

        public GremlinDerivedVariable() { }

        public GremlinDerivedVariable(WTableReferenceWithAliasAndColumns queryDerivedTable)
        {
            VariableName = "D_" + _count.ToString();
            _count += 1;
            QueryDerivedTable = queryDerivedTable;
            QueryDerivedTable.Alias = GremlinUtil.GetIdentifier(VariableName);
        }
        private static long _count = 0;
    }

    internal class GremlinScalarVariable : GremlinVariable
    {
        public WScalarSubquery ScalarSubquery;

        public GremlinScalarVariable(WSqlStatement selectQueryBlock)
        {
            ScalarSubquery = new WScalarSubquery()
            {
                SubQueryExpr = selectQueryBlock as WSelectQueryBlock
            };
        }
    }

    //internal class GremlinMapVariable : GremlinDerivedVariable
    //{
    //    public GremlinMapVariable(WSqlStatement selectQueryBlock): base(selectQueryBlock) {}
    //}

    //internal class GremlinListVariable : GremlinDerivedVariable
    //{
    //    public GremlinListVariable(WSqlStatement selectQueryBlock) : base(selectQueryBlock) {}
    //}

    internal class GremlinPropertyVariable : GremlinVariable
    {
        public GremlinPropertyVariable()
        {
            VariableName = "L_" + _count.ToString();
            _count += 1;
        }
        private static long _count = 0;
    }


    //=============================================================================


    internal class GremlinAddEVariable : GremlinVariable
    {
        public GremlinVariable FromVariable;
        public GremlinVariable ToVariable;
        public Dictionary<string, List<object>> Properties;
        public string EdgeLabel;

        public GremlinAddEVariable(string edgeLabel, GremlinVertexVariable currVariable)
        {
            VariableName = "AddE_" + _count.ToString();
            _count += 1;

            Properties = new Dictionary<string, List<object>>();
            FromVariable = currVariable;
            ToVariable = currVariable;
            EdgeLabel = edgeLabel;
        }
        private static long _count = 0;
    }

    internal class GremlinAddVVariable : GremlinVariable
    {
        public Dictionary<string, List<object>> Properties;
        public string VertexLabel;

        public GremlinAddVVariable(string vertexLabel)
        {
            VariableName = "AddV_" + _count.ToString();
            _count += 1;

            Properties = new Dictionary<string, List<object>>();
            VertexLabel = vertexLabel;
        }
        private static long _count = 0;
    }


    internal class GremlinChooseVariable: GremlinVariable
    {
        public WChoose2 ChooseExpr;

        public GremlinChooseVariable(WChoose2 chooseExpr)
        {
            VariableName = "Choose_" + _count.ToString();
            _count += 1;
            ChooseExpr = chooseExpr;
            ChooseExpr.Alias = GremlinUtil.GetIdentifier(VariableName);
        }
        private static long _count = 0;
    }

    internal class GremlinCoalesceVariable : GremlinVariable
    {
        public WCoalesce2 CoalesceExpr;

        public GremlinCoalesceVariable(WCoalesce2 coalesceExpr)
        {
            VariableName = "Coalesce_" + _count.ToString();
            _count += 1;
            CoalesceExpr = coalesceExpr;
            CoalesceExpr.Alias = GremlinUtil.GetIdentifier(VariableName);
        }
        private static long _count = 0;
    }

    internal class GremlinOptionalVariable : GremlinVariable
    {
        public WOptional OptionalExpr;

        public GremlinOptionalVariable(WOptional optionalExpr)
        {
            VariableName = "Optional_" + _count.ToString();
            _count += 1;
            OptionalExpr = optionalExpr;
            OptionalExpr.Alias = GremlinUtil.GetIdentifier(VariableName);
        }
        private static long _count = 0;
    }

    internal class GremlinSideEffectVariable : GremlinVariable
    {
        public WSideEffect SideEffectExpr;

        public GremlinSideEffectVariable(WSideEffect optionalExpr)
        {
            VariableName = "SideEffect_" + _count.ToString();
            _count += 1;
            SideEffectExpr = optionalExpr;
            SideEffectExpr.Alias = GremlinUtil.GetIdentifier(VariableName);
        }
        private static long _count = 0;
    }

    //====================================================================================
    public enum Scope
    {
        local,
        global
    }

    internal class Projection
    {
        public GremlinVariable CurrVariable;
        public virtual WSelectElement ToSelectElement()
        {
            return null;
        }
    }

    internal class ValueProjection: Projection
    {
        public string Value;

        public ValueProjection(GremlinVariable gremlinVar, string value)
        {
            CurrVariable = gremlinVar;
            Value = value;
        }
    }

    internal class ColumnProjection : ValueProjection
    {

        public ColumnProjection(GremlinVariable gremlinVar, string value): base(gremlinVar, value)
        {
        }

        public override WSelectElement ToSelectElement()
        {
            return new WSelectScalarExpression()
            {
                SelectExpr = new WColumnReferenceExpression()
                { MultiPartIdentifier = GremlinUtil.GetMultiPartIdentifier(CurrVariable.VariableName, Value) }
            };
        }
    }

    internal class ConstantProjection : ValueProjection
    {
        public string Value;

        public ConstantProjection(GremlinVariable gremlinVar, string value): base(gremlinVar, value)
        {
        }
        public override WSelectElement ToSelectElement()
        {
            return new WSelectScalarExpression() { SelectExpr = GremlinUtil.GetValueExpression(Value) };
        }
    }

    internal class FunctionCallProjection : Projection
    {
        public WFunctionCall FunctionCall;

        public FunctionCallProjection(GremlinVariable gremlinVar, WFunctionCall functionCall)
        {
            CurrVariable = gremlinVar;
            FunctionCall = functionCall;
        }
        public override WSelectElement ToSelectElement()
        {
            return new WSelectScalarExpression() { SelectExpr = FunctionCall };
        }
    }

    internal class StarProjection : Projection
    {
        public StarProjection() { }

        public override WSelectElement ToSelectElement()
        {
            return new WSelectStarExpression();
        }
    }


    internal class OrderByRecord
    {
        public List<WExpressionWithSortOrder> SortOrderList;

        public OrderByRecord()
        {
            SortOrderList = new List<WExpressionWithSortOrder>();
        }
    }

    internal class GroupByRecord
    {
        public List<WGroupingSpecification> GroupingSpecList;

        public GroupByRecord()
        {
            GroupingSpecList = new List<WGroupingSpecification>();
        }
    }
}
