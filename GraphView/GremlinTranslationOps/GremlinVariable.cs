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
        Default,
        Vertex,
        Edge,
        Properties,
        Value
    }

    internal class GremlinVertexVariable : GremlinVariable
    {
        public GremlinVertexVariable()
        {
            //automaticlly generate the name of node
            VariableName = GetVariableName();
            Type = VariableType.Vertex;
        }
        
        public static string GetVariableName()
        {
            return "N_" + _count++;
        }
        private static long _count = 0;
    }
    internal class GremlinEdgeVariable : GremlinVariable
    {
        public GremlinVariable SourceVariable;
        public GremlinEdgeVariable(GremlinVariable sourceVariable, WEdgeType type)
        {
            //automaticlly generate the name of edge
            VariableName = GetVariableName();
            EdgeType = type;
            Type = VariableType.Edge;
            SourceVariable = sourceVariable;
        }
        public static string GetVariableName()
        {
            return "E_" + _count++;
        }
        private static long _count = 0;
        public WEdgeType EdgeType { get; set; }
    }

    internal class GremlinPathNodeVariable : GremlinVariable
    {
        public GremlinPathNodeVariable()
        {
            //automaticlly generate the name of edge
            VariableName = GetVariableName();
            Type = VariableType.Vertex;
        }
        public static string GetVariableName()
        {
            return "PN_" + _count++;
        }
        private static long _count = 0;
    }

    internal class GremlinPathEdgeVariable : GremlinVariable
    {
        public GremlinEdgeVariable EdgeVariable;
        public GremlinPathEdgeVariable(GremlinEdgeVariable edgeVariable)
        {
            //automaticlly generate the name of edge
            VariableName = GetVariableName();
            Type = VariableType.Edge;
            EdgeVariable = edgeVariable;
        }

        public static string GetVariableName()
        {
            return "PE_" + _count++;
        }
        private static long _count = 0;
    }

    internal class GremlinVariableReference: GremlinVariable
    {
        public WVariableReference Variable;
        public GremlinVariable RealGremlinVariable;
        public WSetVariableStatement Statement;

        public GremlinVariableReference(WSetVariableStatement statement)
        {

            Variable = statement.Variable;
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
        public WVariableReference Variable;
        public GremlinDerivedVariable(WSqlStatement statement, string derivedType = null)
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

    internal class GremlinScalarVariable : GremlinVariable
    {
        //public WScalarSubquery ScalarSubquery;

        //public GremlinScalarVariable(WSqlStatement selectQueryBlock)
        //{
        //    ScalarSubquery = new WScalarSubquery()
        //    {
        //        SubQueryExpr = selectQueryBlock as WSelectQueryBlock
        //    };
        //}
        public GremlinVariable FromVariable;
        public string Key;

        public GremlinScalarVariable(GremlinVariable variable, string key)
        {
            VariableName = variable.VariableName;
            FromVariable = variable;
            Key = key;
        }
    }


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

    internal class GremlinAddEVariable : GremlinVariable
    {
        public GremlinVariable FromVariable;
        //public bool IsNewFromVariable;
        public GremlinVariable ToVariable;
        //public bool IsNewToVariable;
        public Dictionary<string, object> Properties;
        public string EdgeLabel;
        public bool IsGenerateSql;
        public WVariableReference Variable;
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

            Type = VariableType.Edge;

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
        public WVariableReference Variable;
        public GremlinAddVVariable(string vertexLabel)
        {
            SetVariableName = GetVariableName();
            //VariableName = GremlinVertexVariable.GetVariableName();
            VariableName = SetVariableName;

            Properties = new Dictionary<string, object>();
            VertexLabel = vertexLabel;

            Type = VariableType.Vertex;

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

    internal class GremlinMatchPath
    {
        public GremlinVariable SourceVariable;
        public GremlinVariable EdgeVariable;
        public GremlinVariable SinkVariable;

        public GremlinMatchPath(GremlinVariable sourceVariable, GremlinVariable edgeVariable, GremlinVariable sinkVariable)
        {
            SourceVariable = sourceVariable;
            EdgeVariable = edgeVariable;
            SinkVariable = sinkVariable;
        }
    }

//====================================================================================
public enum Scope
    {
        local,
        global
    }

    internal class Projection
    {
        public string VariableAlias;
        public virtual WSelectElement ToSelectElement()
        {
            return null;
        }
    }

    internal class ColumnProjection : Projection
    {
        public string Key;
        public string ColumnAlias;
        public ColumnProjection(string variableAlias, string key, string columnAlias = null)
        {
            VariableAlias = variableAlias;
            ColumnAlias = columnAlias;
            Key = key;
        }

        public override WSelectElement ToSelectElement()
        {
            var multiPartIdentifier = (Key == "" || Key == null)
                ? GremlinUtil.GetMultiPartIdentifier(VariableAlias)
                : GremlinUtil.GetMultiPartIdentifier(VariableAlias, Key);
            return new WSelectScalarExpression()
            {
                ColumnName = ColumnAlias,
                SelectExpr = new WColumnReferenceExpression()
                { MultiPartIdentifier = multiPartIdentifier }
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
