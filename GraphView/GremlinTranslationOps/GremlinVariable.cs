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

        public override int GetHashCode()
        {
            return VariableName.GetHashCode();
        }
    }

    internal enum GremlinEdgeType
    {
        InEdge,
        OutEdge,
        BothEdge
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

        public GremlinEdgeVariable(GremlinEdgeType type)
        {
            //automaticlly generate the name of edge
            VariableName = "E_" + _count.ToString();
            _count += 1;
            EdgeType = type;
        }

        private static long _count = 0;
        public GremlinEdgeType EdgeType { get; set; }
    }

    internal class GremlinRecursiveEdgeVariable : GremlinVariable
    {
        public WSelectQueryBlock GremlinTranslationOperatorQuery { get; set; }
        public int IterationCount;
        public WBooleanExpression UntilCondition { get; set; }
    }

    internal class GremlinJoinVertexVariable : GremlinVertexVariable
    {
        public GremlinVariable LeftVariable;
        public GremlinVariable RightVariable;
        public GremlinJoinVertexVariable(GremlinVariable leftGremlinVariable, GremlinVariable righGremlinVariable)
        {
            LeftVariable = leftGremlinVariable;
            RightVariable = righGremlinVariable;

            //automaticlly generate the name of node
            VariableName = "JV_" + _count.ToString();
            _count += 1;
        }
        private static long _count = 0;
    }

    internal class GremlinJoinEdgeVariable : GremlinEdgeVariable
    {
        public GremlinVariable LeftVariable;
        public GremlinVariable RightVariable;
        public GremlinJoinEdgeVariable(GremlinVariable leftGremlinVariable, GremlinVariable righGremlinVariable)
        {
            LeftVariable = leftGremlinVariable;
            RightVariable = righGremlinVariable;

            //automaticlly generate the name of node
            VariableName = "JE_" + _count.ToString();
            _count += 1;
        }
        private static long _count = 0;
    }

    internal class GremlinDerivedVariable: GremlinVariable
    {
        public WQueryDerivedTable QueryDerivedTable;
        public GremlinDerivedVariable(WSqlStatement selectQueryBlock)
        {
            QueryDerivedTable = new WQueryDerivedTable()
            {
                QueryExpr = selectQueryBlock as WSelectQueryExpression,
                Alias = GremlinUtil.GetIdentifier(VariableName)
            };
        }
    }

    internal class GremlinScalarVariable : GremlinVariable
    {
        
    }

    internal class GremlinMapVariable : GremlinDerivedVariable
    {
        public GremlinMapVariable(WSqlStatement selectQueryBlock): base(selectQueryBlock)
        {
            VariableName = "M_" + _count.ToString();
            _count += 1;
        }

        private static long _count = 0;
    }

    internal class GremlinListVariable : GremlinDerivedVariable
    {
        public GremlinListVariable(WSqlStatement selectQueryBlock) : base(selectQueryBlock)
        {
            VariableName = "L_" + _count.ToString();
            _count += 1;
        }
        private static long _count = 0;
    }

    internal class GremlinPropertyVariable : GremlinVariable
    {
        public GremlinPropertyVariable()
        {
            VariableName = "L_" + _count.ToString();
            _count += 1;
        }
        private static long _count = 0;
    }

    internal class GremlinAddEVariable : GremlinVariable
    {
        public GremlinVertexVariable FromVariable;
        public GremlinVertexVariable ToVariable;
        public Dictionary<string, object> Properties;
        public string EdgeLabel;

        public GremlinAddEVariable(string edgeLabel, GremlinVertexVariable currVariable)
        {
            Properties = new Dictionary<string, object>();
            FromVariable = currVariable;
            ToVariable = currVariable;
            EdgeLabel = edgeLabel;
        }
    }

    internal class GremlinAddVVariable : GremlinVariable
    {
        public Dictionary<string, object> Properties;

        public GremlinAddVVariable()
        {
            Properties = new Dictionary<string, object>();
        }
    }

    internal class GremlinConstantVariable : GremlinVariable
    {
        public object Constant;

        public GremlinConstantVariable(object constant)
        {
            Constant = constant;
        }
    }

    internal class GremlinRangeVariable: GremlinVariable
    {
        public long Low;
        public long High;
        public GremlinRangeVariable(long low, long high)
        {
            Low = low;
            High = high;

            VariableName = "R_" + _count.ToString();
            _count += 1;
        }
        private static long _count = 0;

    }

    public enum Scope
    {
        local,
        global
    }

    internal class Projection
    {
        public GremlinVariable CurrVariable;
        public virtual WSelectScalarExpression ToSelectScalarExpression()
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

        public override WSelectScalarExpression ToSelectScalarExpression()
        {
            return new WSelectScalarExpression()
                {
                    SelectExpr = new WColumnReferenceExpression()
                    { MultiPartIdentifier = GetProjectionIndentifiers() }
                };
        }

        public WMultiPartIdentifier GetProjectionIndentifiers()
        {
            var identifiers = new List<Identifier>();
            identifiers.Add(new Identifier() { Value = CurrVariable.VariableName });
            identifiers.Add(new Identifier() { Value = Value });
            return new WMultiPartIdentifier() { Identifiers = identifiers };
        }
    }

    internal class ConstantProjection : Projection
    {
        public string Value;

        public ConstantProjection(GremlinVariable gremlinVar, string value)
        {
            CurrVariable = gremlinVar;
            Value = value;
        }
        public override WSelectScalarExpression ToSelectScalarExpression()
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
        public override WSelectScalarExpression ToSelectScalarExpression()
        {
            return new WSelectScalarExpression() { SelectExpr = FunctionCall };
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
