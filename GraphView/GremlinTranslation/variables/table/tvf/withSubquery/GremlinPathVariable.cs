using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinPathVariable : GremlinTableVariable
    {
        public List<GremlinPathStepVariable> PathList { get; set; }
        public bool IsInRepeatContext { get; set; }
        public List<GremlinToSqlContext> ByContexts { get; set; }

        public GremlinPathVariable(List<GremlinPathStepVariable> pathList, List<GremlinToSqlContext> byContexts)
            :base(GremlinVariableType.Table)
        {
            this.PathList = pathList;
            IsInRepeatContext = false;
            ByContexts = byContexts;
        }

        //automatic generated path will use this constructor
        public GremlinPathVariable(List<GremlinPathStepVariable> pathList)
            : base(GremlinVariableType.Table)
        {
            this.PathList = pathList;
            IsInRepeatContext = false;
            ByContexts = new List<GremlinToSqlContext>();
        }

        internal override List<GremlinVariable> FetchVarsFromCurrAndChildContext()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>();
            foreach (var context in ByContexts)
            {
                variableList.AddRange(context.FetchVarsFromCurrAndChildContext());
            }
            return variableList;
        }

        internal override void Populate(string property)
        {
            if (ByContexts.Count == 0)
            {
                foreach (var step in PathList)
                {
                    step.Populate(property);
                }
            }
            else
            {
                foreach (var context in ByContexts)
                {
                    context.Populate(property);
                }
            }
            base.Populate(property);
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            List<WSelectQueryBlock> queryBlocks = new List<WSelectQueryBlock>();

            //Must toSelectQueryBlock before toCompose1 of variableList in order to populate needed columns
            foreach (var byContext in ByContexts)
            {
                //TODO: select compose1
                WSelectQueryBlock queryBlock = byContext.ToSelectQueryBlock();
                queryBlock.SelectElements.Clear();
                queryBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(byContext.PivotVariable.ToCompose1(), GremlinKeyword.TableDefaultColumnName));
                queryBlocks.Add(queryBlock);
                
            }

            if (IsInRepeatContext)
            {
                //Must add as the first parameter
                parameters.Add(SqlUtil.GetColumnReferenceExpr(GremlinKeyword.RepeatInitalTableName, GremlinKeyword.Path));
            }
            foreach (var path in PathList)
            {
                if (path.AttachedVariable != null)
                {
                    parameters.Add(SqlUtil.GetColumnReferenceExpr(path.AttachedVariable.GetVariableName(), GremlinKeyword.Path));
                }
                else
                {
                    parameters.Add(path.StepVariable.First().ToCompose1());
                }
            }

            foreach (var block in queryBlocks)
            {
                parameters.Add(SqlUtil.GetScalarSubquery(block));
            }

            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Path2, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
