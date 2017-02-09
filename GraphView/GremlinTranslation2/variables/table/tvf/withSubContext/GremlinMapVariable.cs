using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinMapVariable : GremlinTableVariable
    {
        public GremlinToSqlContext MapContext { get; set; }

        public static GremlinMapVariable Create(GremlinToSqlContext MapContext)
        {
            switch (MapContext.PivotVariable.GetVariableType())
            {
                case GremlinVariableType.Vertex:
                    return new GremlinMapVertexVariable(MapContext);
                case GremlinVariableType.Edge:
                    return new GremlinMapEdgeVariable(MapContext);
                case GremlinVariableType.Scalar:
                    return new GremlinMapScalarVariable(MapContext);
            }
            return new GremlinMapVariable(MapContext);
        }

        internal override GremlinVariableProperty GetPath()
        {
            return new GremlinVariableProperty(this, GremlinKeyword.Path);
        }

        public GremlinMapVariable(GremlinToSqlContext mapContext, GremlinVariableType variableType = GremlinVariableType.Table)
            : base(variableType)
        {
            MapContext = mapContext;
        }

        internal override void Populate(string property)
        {
            if (ProjectedProperties.Contains(property)) return;

            base.Populate(property);
            MapContext.Populate(property);
        }

        internal override bool ContainsLabel(string label)
        {
            if (base.ContainsLabel(label)) return true;
            return false;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetScalarSubquery(MapContext.ToSelectQueryBlock(ProjectedProperties)));
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Map, parameters, this, VariableName);

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }

    internal class GremlinMapVertexVariable : GremlinMapVariable
    {
        public GremlinMapVertexVariable(GremlinToSqlContext mapContext)
            : base(mapContext, GremlinVariableType.Vertex)
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

    internal class GremlinMapEdgeVariable : GremlinMapVariable
    {
        public GremlinMapEdgeVariable(GremlinToSqlContext mapContext)
            : base(mapContext, GremlinVariableType.Edge)
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

    internal class GremlinMapScalarVariable : GremlinMapVariable
    {
        public GremlinMapScalarVariable(GremlinToSqlContext mapContext)
            : base(mapContext, GremlinVariableType.Scalar)
        {
        }
    }
}
