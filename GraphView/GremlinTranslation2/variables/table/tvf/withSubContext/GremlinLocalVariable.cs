using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinLocalVariable : GremlinTableVariable
    {
        public GremlinToSqlContext LocalContext { get; set; }

        public static GremlinLocalVariable Create(GremlinToSqlContext localContext)
        {
            switch (localContext.PivotVariable.GetVariableType())
            {
                case GremlinVariableType.Vertex:
                    return new GremlinLocalVertexVariable(localContext);
                case GremlinVariableType.Edge:
                    return new GremlinLocalEdgeVariable(localContext);
                case GremlinVariableType.Scalar:
                    return new GremlinLocalEdgeScalarVariable(localContext);
            }
            return new GremlinLocalVariable(localContext);
        }

        internal override GremlinVariableProperty GetPath()
        {
            return new GremlinVariableProperty(this, GremlinKeyword.Path);
        }

        public GremlinLocalVariable(GremlinToSqlContext localContext, GremlinVariableType variableType = GremlinVariableType.Table)
            : base(variableType)
        {
            LocalContext = localContext;
        }

        internal override void Populate(string property)
        {
            if (ProjectedProperties.Contains(property)) return;
            base.Populate(property);
            LocalContext.Populate(property);
        }

        internal override void PopulateGremlinPath()
        {
            LocalContext.PopulateGremlinPath();
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetScalarSubquery(LocalContext.ToSelectQueryBlock(ProjectedProperties)));
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Local, parameters, this, VariableName);

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }

    internal class GremlinLocalVertexVariable : GremlinLocalVariable
    {
        public GremlinLocalVertexVariable(GremlinToSqlContext localContext)
            : base(localContext, GremlinVariableType.Vertex)
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

    internal class GremlinLocalEdgeVariable : GremlinLocalVariable
    {
        public GremlinLocalEdgeVariable(GremlinToSqlContext localContext)
             : base(localContext, GremlinVariableType.Edge)
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

    internal class GremlinLocalEdgeScalarVariable : GremlinLocalVariable
    {
        public GremlinLocalEdgeScalarVariable(GremlinToSqlContext localContext)
            : base(localContext, GremlinVariableType.Scalar)
        {
        }
    }
}
