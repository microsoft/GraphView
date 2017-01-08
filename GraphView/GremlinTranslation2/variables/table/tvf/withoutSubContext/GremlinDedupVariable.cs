using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinDedupVariable : GremlinSqlTableVariable
    {
        public static GremlinTableVariable Create(GremlinVariable inputVariable, List<string> dedupLabels)
        {
            switch (inputVariable.GetVariableType())
            {
                case GremlinVariableType.Vertex:
                    return new GremlinDedupVertexVariable(inputVariable, dedupLabels);
                case GremlinVariableType.Edge:
                    return new GremlinDedupEdgeVariable(inputVariable, dedupLabels);
                case GremlinVariableType.Table:
                    return new GremlinDedupTableVariable(inputVariable, dedupLabels);
                case GremlinVariableType.Scalar:
                    return new GremlinDedupScalarVariable(inputVariable, dedupLabels);
            }
            throw new QueryCompilationException();
        }

        public GremlinVariable InputVariable { get; set; }
        public List<string> DedupLabels { get; set; }

        public GremlinDedupVariable(GremlinVariable inputVariable, List<string> dedupLabels)
        {
            InputVariable = inputVariable;
            DedupLabels = new List<string>(dedupLabels);
        }

        internal override void Populate(string property)
        {
            
        }

        public override WTableReference ToTableReference(List<string> projectProperties, string tableName)
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(InputVariable.DefaultProjection().ToScalarExpression());
            foreach (var dedupLabel in DedupLabels)
            {
                //TODO:
                throw new NotImplementedException();
                parameters.Add(SqlUtil.GetColumnReferenceExpr(InputVariable.VariableName, dedupLabel));
            }
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Dedup, parameters, tableName);
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }

    internal class GremlinDedupVertexVariable : GremlinVertexTableVariable
    {
        public GremlinDedupVertexVariable(GremlinVariable inputVariable, List<string> dedupLabels)
        {
            SqlTableVariable = new GremlinDedupVariable(inputVariable, dedupLabels);
        }
    }

    internal class GremlinDedupEdgeVariable : GremlinEdgeTableVariable
    {
        public GremlinDedupEdgeVariable(GremlinVariable inputVariable, List<string> dedupLabels)
        {
            SqlTableVariable = new GremlinDedupVariable(inputVariable, dedupLabels);
        }
    }

    internal class GremlinDedupScalarVariable : GremlinScalarTableVariable
    {
        public GremlinDedupScalarVariable(GremlinVariable inputVariable, List<string> dedupLabels)
        {
            SqlTableVariable = new GremlinDedupVariable(inputVariable, dedupLabels);
        }
    }

    internal class GremlinDedupTableVariable : GremlinTableVariable
    {
        public GremlinDedupTableVariable(GremlinVariable inputVariable, List<string> dedupLabels)
        {
            SqlTableVariable = new GremlinDedupVariable(inputVariable, dedupLabels);
        }
    }
}
