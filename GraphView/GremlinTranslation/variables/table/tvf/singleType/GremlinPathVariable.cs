using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinPathVariable: GremlinScalarTableVariable
    {
        public List<GremlinVariableProperty> PathList { get; set; }
        public bool IsInRepeatContext { get; set; }

        public GremlinPathVariable(List<GremlinVariableProperty> pathList)
        {
            this.PathList = pathList;
            IsInRepeatContext = false;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            if (IsInRepeatContext)
            {
                //Must add as the first parameter
                parameters.Add(SqlUtil.GetColumnReferenceExpr("R", GremlinKeyword.Path));
            }
            foreach (var path in PathList)
            {
                parameters.Add(path.ToScalarExpression());    
            }
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Path, parameters, this, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}
