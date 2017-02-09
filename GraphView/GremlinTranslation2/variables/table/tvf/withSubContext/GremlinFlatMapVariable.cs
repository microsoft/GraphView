using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinFlatMapVariable: GremlinTableVariable
    {
        public GremlinToSqlContext FlatMapContext { get; set; }

        public static GremlinFlatMapVariable Create(GremlinToSqlContext flatMapContext)
        {
            switch (flatMapContext.PivotVariable.GetVariableType())
            {
                case GremlinVariableType.Vertex:
                    return new GremlinFlatMapVertexVariable(flatMapContext);
                case GremlinVariableType.Edge:
                    return new GremlinFlatMapEdgeVariable(flatMapContext);
                case GremlinVariableType.Scalar:
                    return new GremlinFlatMapScalarVariable(flatMapContext);
            }
            return new GremlinFlatMapVariable(flatMapContext);
        }

        public GremlinFlatMapVariable(GremlinToSqlContext flatMapContext, GremlinVariableType variableType = GremlinVariableType.Table)
            : base(variableType)
        {
            FlatMapContext = flatMapContext;
        }

        internal override GremlinVariableProperty GetPath()
        {
            return new GremlinVariableProperty(this, GremlinKeyword.Path);
        }

        internal override void Populate(string property)
        {
            if (ProjectedProperties.Contains(property)) return;
            base.Populate(property);
            FlatMapContext.Populate(property);
        }

        internal override bool ContainsLabel(string label)
        {
            if (base.ContainsLabel(label)) return true;
            return false;
        }

        internal override List<GremlinVariable> FetchAllVariablesInCurrAndChildContext()
        {
            return FlatMapContext.FetchAllVariablesInCurrAndChildContext();
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetScalarSubquery(FlatMapContext.ToSelectQueryBlock(ProjectedProperties)));
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.FlatMap, parameters, this, VariableName);

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }

    internal class GremlinFlatMapVertexVariable : GremlinFlatMapVariable
    {
        public GremlinFlatMapVertexVariable(GremlinToSqlContext flatMapContext)
            : base(flatMapContext, GremlinVariableType.Vertex)
        {
        }

        internal override void Both(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.Both(this, edgeLabels);
        }

        internal override void BothE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.BothE(this, edgeLabels);
        }

        internal override void BothV(GremlinToSqlContext currentContext)
        {
            currentContext.BothV(this);
        }

        internal override void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.In(this, edgeLabels);
        }

        internal override void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.InE(this, edgeLabels);
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.Out(this, edgeLabels);
        }

        internal override void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.OutE(this, edgeLabels);
        }
    }

    internal class GremlinFlatMapEdgeVariable : GremlinFlatMapVariable
    {
        public GremlinFlatMapEdgeVariable(GremlinToSqlContext flatMapContext)
            : base(flatMapContext, GremlinVariableType.Edge)
        {
        }

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

    internal class GremlinFlatMapScalarVariable : GremlinFlatMapVariable
    {
        public GremlinFlatMapScalarVariable(GremlinToSqlContext flatMapContext)
            : base(flatMapContext, GremlinVariableType.Scalar)
        {
        }
    }
}
