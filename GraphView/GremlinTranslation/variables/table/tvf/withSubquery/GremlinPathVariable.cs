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
            this.PathList = pathList;
            IsInRepeatContext = false;
            ByContexts = byContexts;
        }

        //automatically generated path will use this constructor
        public GremlinPathVariable(List<GremlinVariable> pathList)
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
                if (path is GremlinMultiStepVariable)
                {
                    parameters.Add(path.DefaultProjection().ToScalarExpression());
                }
                else
                {
                    parameters.Add(path.ToCompose1());
                    //var stepVar = path.StepVariable.First();
                    //parameters.Add(stepVar.ToCompose1());
                    //foreach (var label in stepVar.Labels)
                    //{
                    //    parameters.Add(SqlUtil.GetValueExpr(label));
                    //}
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
}
