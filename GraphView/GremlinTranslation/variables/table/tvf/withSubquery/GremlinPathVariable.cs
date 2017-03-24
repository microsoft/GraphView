using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinPathVariable : GremlinTableVariable
    {
        public List<GremlinVariable> PathList { get; set; }
        public bool IsInRepeatContext { get; set; }
        public List<GremlinToSqlContext> ByContexts { get; set; }

        public GremlinPathVariable(List<GremlinVariable> pathList, List<GremlinToSqlContext> byContexts)
            :base(GremlinVariableType.Table)
        {
            PathList = pathList;
            IsInRepeatContext = false;
            ByContexts = byContexts;
        }

        //automatically generated path will use this constructor
        public GremlinPathVariable(List<GremlinVariable> pathList)
            : base(GremlinVariableType.Table)
        {
            PathList = pathList;
            IsInRepeatContext = false;
            ByContexts = new List<GremlinToSqlContext>();
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(PathList.FindAll(p=>p!=null && !(p is GremlinContextVariable)));
            foreach (var context in ByContexts)
            {
                variableList.AddRange(context.FetchAllVars());
            }
            return variableList;
        }

        internal override List<GremlinVariable> FetchAllTableVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            foreach (var context in ByContexts)
            {
                variableList.AddRange(context.FetchAllTableVars());
            }
            return variableList;
        }

        internal override void Populate(string property)
        {
            foreach (var context in ByContexts)
            {
                context.Populate(property);
            }
            base.Populate(property);
        }

        internal override void PopulateStepProperty(string property)
        {
            foreach (var step in PathList)
            {
                if (step == this) continue;
                step?.PopulateStepProperty(property);
            }
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            List<WSelectQueryBlock> queryBlocks = new List<WSelectQueryBlock>();

            //Must toSelectQueryBlock before toCompose1 of variableList in order to populate needed columns
            foreach (var byContext in ByContexts)
            {
                queryBlocks.Add(byContext.ToSelectQueryBlock(true));
            }

            foreach (var path in PathList)
            {
                if (path == null)
                {
                    if (IsInRepeatContext)
                    {
                        parameters.Add(SqlUtil.GetColumnReferenceExpr(GremlinKeyword.RepeatInitalTableName,
                            GremlinKeyword.Path));
                    }
                }
                else if (path is GremlinContextVariable)
                {
                    foreach (var label in path.Labels)
                    {
                        parameters.Add(SqlUtil.GetValueExpr(label));
                    }
                }
                else
                {
                    parameters.Add(path.ToStepScalarExpr());
                    foreach (var label in path.Labels)
                    {
                        parameters.Add(SqlUtil.GetValueExpr(label));
                    }
                }
            }

            foreach (var block in queryBlocks)
            {
                parameters.Add(SqlUtil.GetScalarSubquery(block));
            }

            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Path, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }

    internal class GremlinLocalPathVariable : GremlinPathVariable
    {
        public GremlinLocalPathVariable(List<GremlinVariable> pathList, List<GremlinToSqlContext> byContexts)
            :base(pathList, byContexts)
        {
        }

        public GremlinLocalPathVariable(List<GremlinVariable> pathList)
            : base(pathList)
        {
        }

    }

    internal class GremlinGlobalPathVariable : GremlinPathVariable
    {
        public GremlinGlobalPathVariable(List<GremlinVariable> pathList, List<GremlinToSqlContext> byContexts)
            : base(pathList, byContexts)
        {
        }

        public GremlinGlobalPathVariable(List<GremlinVariable> pathList)
            : base(pathList)
        {
        }

    }
}
