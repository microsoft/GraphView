using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView.GremlinTranslationOps
{
    internal enum GremlinVariableType
    {
        Vertex,
        Edge,
        Scalar,
        Table,
        Undefined
    }
    internal abstract class GremlinVariable
    {

        public string VariableName { get; set; }
        public string SetVariableName { get; set; }
        public long Low { get; set; }
        public long High { get; set; }

        public virtual GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Undefined;
        }
    }

    internal class GremlinVertexVariable : GremlinVariable
    {
        private static long _count = 0;

        public static string GetVariableName()
        {
            return "N_" + _count++;
        }
        
        public GremlinVertexVariable()
        {
            VariableName = GetVariableName();
        }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Vertex;
        }

    }
    internal class GremlinEdgeVariable : GremlinVariable
    {
        private static long _count = 0;

        public static string GetVariableName()
        {
            return "E_" + _count++;
        }

        public GremlinVariable SourceVariable { get; set; }
        public WEdgeType EdgeType { get; set; }

        public GremlinEdgeVariable() { }

        public GremlinEdgeVariable(GremlinVariable sourceVariable, WEdgeType type)
        {
            VariableName = GetVariableName();
            EdgeType = type;
            SourceVariable = sourceVariable;
        }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Edge;
        }
    }

    internal class GremlinScalarVariable : GremlinVariable
    {
        public GremlinVariable FromVariable { get; set; }
        public string Key { get; set; }

        public GremlinScalarVariable(GremlinVariable variable, string key)
        {
            VariableName = variable.VariableName;
            FromVariable = variable;
            Key = key;
        }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Scalar;
        }
    }

    //internal class GremlinPathNodeVariable : GremlinVariable
    //{
    //    public GremlinPathNodeVariable()
    //    {
    //        VariableName = GetVariableName();
    //    }
    //    public static string GetVariableName()
    //    {
    //        return "PN_" + _count++;
    //    }
    //    private static long _count = 0;
    //}

    //internal class GremlinPathEdgeVariable : GremlinVariable
    //{
    //    public GremlinEdgeVariable EdgeVariable;
    //    public GremlinPathEdgeVariable(GremlinEdgeVariable edgeVariable)
    //    {
    //        VariableName = GetVariableName();
    //        Type = GremlinVariableType.Edge;
    //        EdgeVariable = edgeVariable;
    //    }

    //    public static string GetVariableName()
    //    {
    //        return "PE_" + _count++;
    //    }
    //    private static long _count = 0;
    //}

    internal class GremlinVariableReference: GremlinVariable
    {
        public WVariableReference Variable { get; set; }
        public GremlinVariable RealGremlinVariable { get; set; }
        public WSetVariableStatement Statement { get; set; }

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
        private static long _count = 0;
        private static Dictionary<DerivedType, long> _typeToCount;

        public WSqlStatement Statement { get; set; }
        public DerivedType Type { get; set; }
        public WVariableReference Variable { get; set; }

        public GremlinDerivedVariable() { }

        public GremlinDerivedVariable(WSqlStatement statement, string derivedType = null)
        {
            VariableName = "D" + derivedType + "_" + getCount(GetDerivedType(derivedType));
            _count += 1;
            Statement = statement;
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
        
        public enum DerivedType
        {
            UNION,
            FOLD,
            INJECT,
            DEFAULT
        }

    }

    internal class GremlinTVFVariable : GremlinVariable
    {
        private static long _count = 0;
        public static string GetVariableName()
        {
            return "TVF_" + _count++;
        }

        public WUnqualifiedJoin TableReference { get; set; }

        public GremlinTVFVariable(WUnqualifiedJoin tableReference)
        {
            VariableName = GetVariableName();
            TableReference = tableReference;
            (TableReference.SecondTableRef as WSchemaObjectFunctionTableReference).Alias =
                GremlinUtil.GetIdentifier(VariableName);
        }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Table;
        }
    }

    internal class GremlinAddEVariable : GremlinEdgeVariable
    {
        public static string GetVariableName()
        {
            return "AddE_" + _count++;
        }

        private static long _count = 0;

        public GremlinVariable FromVariable { get; set; }
        public GremlinVariable ToVariable { get; set; }
        public Dictionary<string, object> Properties { get; set; }
        public string EdgeLabel { get; set; }
        public bool IsGenerateSql { get; set; }
        public WVariableReference Variable { get; set; }

        public GremlinAddEVariable(string edgeLabel, GremlinVariable currVariable)
        {
            SetVariableName = GetVariableName();
            VariableName = SetVariableName;
            Properties = new Dictionary<string, object>();
            FromVariable = currVariable;
            ToVariable = currVariable;
            EdgeLabel = edgeLabel;
            Variable = GremlinUtil.GetVariableReference(SetVariableName);
            IsGenerateSql = false;
        }
    }

    internal class GremlinAddVVariable : GremlinVertexVariable
    {
        private static long _count = 0;

        public static string GetVariableName()
        {
            return "AddV_" + _count++;
        }

        public bool IsGenerateSql { get; set; }
        public Dictionary<string, object> Properties { get; set; }
        public string VertexLabel { get; set; }
        public WVariableReference Variable { get; set; }

        public GremlinAddVVariable(string vertexLabel)
        {
            SetVariableName = GetVariableName();
            VariableName = SetVariableName;

            Properties = new Dictionary<string, object>();
            VertexLabel = vertexLabel;
            Variable = GremlinUtil.GetVariableReference(SetVariableName);
            IsGenerateSql = false;
        }
    }


    //internal class GremlinChooseVariable: GremlinVariable
    //{
    //    public WChoose2 TableReference;

    //    public GremlinChooseVariable(WChoose2 tableReference)
    //    {
    //        VariableName = GetVariableName();
    //        TableReference = tableReference;
    //        TableReference.Alias = GremlinUtil.GetIdentifier(VariableName);
    //    }
    //    private static long _count = 0;
    //    public static string GetVariableName()
    //    {
    //        return "Choose_" + _count++;
    //    }
    //}

    internal class GremlinMatchPath
    {
        public GremlinVariable SourceVariable { get; set; }
        public GremlinVariable EdgeVariable { get; set; }
        public GremlinVariable SinkVariable { get; set; }

        public GremlinMatchPath(GremlinVariable sourceVariable, GremlinVariable edgeVariable, GremlinVariable sinkVariable)
        {
            SourceVariable = sourceVariable;
            EdgeVariable = edgeVariable;
            SinkVariable = sinkVariable;
        }
    }


    internal class OrderByRecord
    {
        public List<WExpressionWithSortOrder> SortOrderList { get; set; }

        public OrderByRecord()
        {
            SortOrderList = new List<WExpressionWithSortOrder>();
        }
    }

    internal class GroupByRecord
    {
        public List<WGroupingSpecification> GroupingSpecList { get; set; }

        public GroupByRecord()
        {
            GroupingSpecList = new List<WGroupingSpecification>();
        }
    }

}
