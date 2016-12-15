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
        public string SetVariableName { get; set; }
        public WVariableReference Variable;
        public VariableType Type;
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

    public enum VariableType
    {
        NODE,
        EGDE,
        PROPERTIES,
        VALUE
    }

    internal class GremlinVertexVariable : GremlinVariable
    {
        public GremlinVertexVariable()
        {
            //automaticlly generate the name of node
            VariableName = GetVariableName();
            Type = VariableType.NODE;
        }
        
        public static string GetVariableName()
        {
            return "N_" + _count++;
        }
        private static long _count = 0;
    }
    internal class GremlinEdgeVariable : GremlinVariable
    {
        public GremlinEdgeVariable()
        {
            //automaticlly generate the name of edge
            VariableName = GetVariableName();
            Type = VariableType.EGDE;
        }

        public GremlinEdgeVariable(WEdgeType type)
        {
            //automaticlly generate the name of edge
            VariableName = GetVariableName();
            EdgeType = type;
        }
        public static string GetVariableName()
        {
            return "E_" + _count++;
        }
        private static long _count = 0;
        public WEdgeType EdgeType { get; set; }
    }

    internal class GremlinPathVariable : GremlinVariable
    {
        public GremlinPathVariable()
        {
            //automaticlly generate the name of edge
            VariableName = GetVariableName();
            Type = VariableType.EGDE;
        }

        public GremlinPathVariable(WEdgeType type)
        {
            //automaticlly generate the name of edge
            VariableName = GetVariableName();
            EdgeType = type;
        }
        public static string GetVariableName()
        {
            return "P_" + _count++;
        }
        private static long _count = 0;
        public WEdgeType EdgeType { get; set; }

        //public WSelectQueryBlock GremlinTranslationOperatorQuery { get; set; }
        //public int IterationCount;
        //public WBooleanExpression UntilCondition { get; set; }
    }

    internal class GremlinVariableReference: GremlinVariable
    {
        public WVariableReference Variable;
        public GremlinVariable RealGremlinVariable;
        public WSetVariableStatement Statement;

        public GremlinVariableReference(WSetVariableStatement statement)
        {

            Variable = (statement as WSetVariableStatement).Variable;
            SetVariableName = Variable.Name;
            VariableName = Variable.Name.Substring(1, Variable.Name.Length-1);
            Statement = statement;
        }
    }

    internal class GremlinDerivedVariable: GremlinVariable
    {
        //public WSelectQueryBlock SelectQueryBlock;
        //public WTableReferenceWithAliasAndColumns QueryDerivedTable;
        public WSqlStatement Statement;
        public DerivedType Type;
        public GremlinDerivedVariable() { }
        
        public GremlinDerivedVariable(WSqlStatement statement, string derivedType = "")
        {

            VariableName = "D" + derivedType + "_" + getCount(GetDerivedType(derivedType));
            _count += 1;
            //QueryDerivedTable = queryDerivedTable;
            Statement = statement;
            //QueryDerivedTable.Alias = GremlinUtil.GetIdentifier(VariableName);
            Type = GetDerivedType(derivedType);

            Variable = GremlinUtil.GetVariableReference(VariableName);
        }

        public DerivedType GetDerivedType(string derivedType)
        {
            if (derivedType == "union") return DerivedType.UNION;
            if (derivedType == "fold") return DerivedType.FOLD;
            if (derivedType == "inject") return DerivedType.INJECT;
            return DerivedType.DEFAULT;
        }

        private long getCount(DerivedType type)
        {
            if (_typeToCount == null)
            {
                _typeToCount = new Dictionary<DerivedType, long>();
                _typeToCount[DerivedType.UNION] = 0;
                _typeToCount[DerivedType.FOLD] = 0;
                _typeToCount[DerivedType.INJECT] = 0;
                _typeToCount[DerivedType.DEFAULT] = 0;
            }
            return _typeToCount[type]++;
        }
        private static long _count = 0;
        private static Dictionary<DerivedType, long> _typeToCount;

        public enum DerivedType
        {
            UNION,
            FOLD,
            INJECT,
            DEFAULT
        }

    }

    //internal class GremlinScalarVariable : GremlinVariable
    //{
    //    public WScalarSubquery ScalarSubquery;

    //    public GremlinScalarVariable(WSqlStatement selectQueryBlock)
    //    {
    //        ScalarSubquery = new WScalarSubquery()
    //        {
    //            SubQueryExpr = selectQueryBlock as WSelectQueryBlock
    //        };
    //    }
    //}

    //internal class GremlinMapVariable : GremlinDerivedVariable
    //{
    //    public GremlinMapVariable(WSqlStatement selectQueryBlock): base(selectQueryBlock) {}
    //}

    //internal class GremlinListVariable : GremlinDerivedVariable
    //{
    //    public GremlinListVariable(WSqlStatement selectQueryBlock) : base(selectQueryBlock) {}
    //}

    internal class GremlinTVFVariable : GremlinVariable
    {
        public WUnqualifiedJoin TableReference;
        public GremlinTVFVariable(WUnqualifiedJoin tableReference)
        {
            VariableName = GetVariableName();
            TableReference = tableReference;
            (TableReference.SecondTableRef as WSchemaObjectFunctionTableReference).Alias =
                GremlinUtil.GetIdentifier(VariableName);
        }
        private static long _count = 0;
        public static string GetVariableName()
        {
            return "TVF_" + _count++;
        }
    }

    //=============================================================================


    internal class GremlinAddEVariable : GremlinVariable
    {
        public GremlinVariable FromVariable;
        //public bool IsNewFromVariable;
        public GremlinVariable ToVariable;
        //public bool IsNewToVariable;
        public Dictionary<string, object> Properties;
        public string EdgeLabel;
        public bool IsGenerateSql;

        public GremlinAddEVariable(string edgeLabel, GremlinVariable currVariable)
        {
            SetVariableName = GetVariableName();
            //VariableName = GremlinEdgeVariable.GetVariableName();
            VariableName = SetVariableName;

            Properties = new Dictionary<string, object>();
            FromVariable = currVariable;
            ToVariable = currVariable;
            EdgeLabel = edgeLabel;
            //IsNewFromVariable = false;
            //IsNewToVariable = false;

            Type = VariableType.EGDE;

            Variable = GremlinUtil.GetVariableReference(SetVariableName);

            IsGenerateSql = false;
        }

        public static string GetVariableName()
        {
            return "AddE_" + _count++;
        }
        private static long _count = 0;
    }

    internal class GremlinAddVVariable : GremlinVariable
    {
        public bool IsGenerateSql;
        public Dictionary<string, object> Properties;
        public string VertexLabel;

        public GremlinAddVVariable(string vertexLabel)
        {
            SetVariableName = GetVariableName();
            //VariableName = GremlinVertexVariable.GetVariableName();
            VariableName = SetVariableName;

            Properties = new Dictionary<string, object>();
            VertexLabel = vertexLabel;

            Type = VariableType.NODE;

            Variable = GremlinUtil.GetVariableReference(SetVariableName);
            IsGenerateSql = false;
        }

        private static long _count = 0;
        public static string GetVariableName()
        {
            return "AddV_" + _count++;
        }
    }


    internal class GremlinChooseVariable: GremlinVariable
    {
        public WChoose2 TableReference;

        public GremlinChooseVariable(WChoose2 tableReference)
        {
            VariableName = GetVariableName();
            TableReference = tableReference;
            TableReference.Alias = GremlinUtil.GetIdentifier(VariableName);
        }
        private static long _count = 0;
        public static string GetVariableName()
        {
            return "Choose_" + _count++;
        }
    }

    internal class GremlinCoalesceVariable : GremlinVariable
    {
        public WCoalesce2 TableReference;

        public GremlinCoalesceVariable(WCoalesce2 tableReference)
        {
            VariableName = GetVariableName();
            TableReference = tableReference;
            TableReference.Alias = GremlinUtil.GetIdentifier(VariableName);
        }
        private static long _count = 0;
        public static string GetVariableName()
        {
            return "Coalesce_" + _count++;
        }
    }

    //internal class GremlinOptionalVariable : GremlinVariable
    //{
    //    public WOptional TableReference;

    //    public GremlinOptionalVariable(WOptional tableReference)
    //    {
    //        VariableName = GetVariableName();
    //        TableReference = tableReference;
    //        TableReference.Alias = GremlinUtil.GetIdentifier(VariableName);
    //    }
    //    private static long _count = 0;
    //    public static string GetVariableName()
    //    {
    //        return "Optional_" + _count++;
    //    }
    //}

    //internal class GremlinSideEffectVariable : GremlinVariable
    //{
    //    public WSideEffect SideEffectExpr;

    //    public GremlinSideEffectVariable(WSideEffect optionalExpr)
    //    {
    //        VariableName = "SideEffect_" + _count.ToString();
    //        _count += 1;
    //        SideEffectExpr = optionalExpr;
    //        SideEffectExpr.Alias = GremlinUtil.GetIdentifier(VariableName);
    //    }
    //    private static long _count = 0;
    //}

    //internal class GremlinRepeatVariable : GremlinVariable
    //{
    //    public WRepeat RepeatExpr;

    //    public GremlinRepeatVariable(WRepeat repeatExpr)
    //    {
    //        VariableName = "Repeat" + _count.ToString();
    //        _count += 1;
    //        RepeatExpr = repeatExpr;
    //        RepeatExpr.Alias = GremlinUtil.GetIdentifier(VariableName);
    //    }
    //    private static long _count = 0;
    //}

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

    internal class ColumnProjection : Projection
    {
        public string Key;
        public ColumnProjection(GremlinVariable gremlinVar, string key)
        {
            CurrVariable = gremlinVar;
            Key = key;
        }

        public override WSelectElement ToSelectElement()
        {
            return new WSelectScalarExpression()
            {
                SelectExpr = new WColumnReferenceExpression()
                { MultiPartIdentifier = GremlinUtil.GetMultiPartIdentifier(CurrVariable.VariableName, Key) }
            };
        }
    }

    internal class FunctionCallProjection : Projection
    {
        public WFunctionCall FunctionCall;

        public FunctionCallProjection(WFunctionCall functionCall)
        {
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
