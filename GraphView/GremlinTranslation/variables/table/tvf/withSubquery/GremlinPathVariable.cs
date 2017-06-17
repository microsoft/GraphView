using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinPathVariable : GremlinTableVariable
    {
        private List<GremlinVariable> StepList { get; set; }
        private List<List<string>> StepLabelsAtThatMoment { get; set; }
        public bool IsInRepeatContext { get; set; }
        public List<GremlinToSqlContext> ByContexts { get; set; }

        public GremlinPathVariable(List<GremlinVariable> stepList, List<GremlinToSqlContext> byContexts)
            :base(GremlinVariableType.Table)
        {
            this.StepList = new List<GremlinVariable>();
            this.StepLabelsAtThatMoment = new List<List<string>>();
            stepList.ForEach(this.AddStep);
            this.IsInRepeatContext = false;
            this.ByContexts = byContexts;
        }

        //automatically generated path will use this constructor
        public GremlinPathVariable(List<GremlinVariable> stepList)
            : base(GremlinVariableType.Table)
        {
            this.StepList = new List<GremlinVariable>();
            this.StepLabelsAtThatMoment = new List<List<string>>();
            stepList.ForEach(this.AddStep);
            this.IsInRepeatContext = false;
            this.ByContexts = new List<GremlinToSqlContext>();
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(this.GetStepList().FindAll(p=>p!=null && !(p is GremlinContextVariable)));
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
            foreach (var step in this.GetStepList())
            {
                if (step == this) continue;
                step?.PopulateStepProperty(property);
            }
        }

        public List<GremlinVariable> GetStepList()
        {
            return this.StepList;
        }

        public void InsertStep(int index, GremlinVariable step)
        {
            this.StepList.Insert(index, step);
            this.StepLabelsAtThatMoment.Insert(index, step?.Labels.Copy());
        }

        public void AddStep(GremlinVariable step)
        {
            this.StepList.Add(step);
            this.StepLabelsAtThatMoment.Add(step?.Labels.Copy());
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

            for (int i = 0; i < this.StepList.Count; i++)
            {
                GremlinVariable step = this.StepList[i];
                if (step == null)
                {
                    // throw new TranslationException("The step should not be null.");
                    //if (IsInRepeatContext)
                    //{
                    //    parameters.Add(SqlUtil.GetColumnReferenceExpr(GremlinKeyword.RepeatInitalTableName,
                    //        GremlinKeyword.Path));
                    //}
                }
                else if (step is GremlinContextVariable)
                {
                    if ((step is GremlinRepeatContextVariable) || (step is GremlinUntilContextVariable) || (step is GremlinEmitContextVariable))
                    {
                        parameters.Add(SqlUtil.GetColumnReferenceExpr(GremlinKeyword.RepeatInitalTableName,
                            GremlinKeyword.Path));
                    }
                    foreach (var label in this.StepLabelsAtThatMoment[i])
                    {
                        parameters.Add(SqlUtil.GetValueExpr(label));
                    }
                }
                else
                {
                    parameters.Add(step.ToStepScalarExpr());
                    foreach (var label in this.StepLabelsAtThatMoment[i])
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
        public GremlinLocalPathVariable(List<GremlinVariable> stepList, List<GremlinToSqlContext> byContexts)
            :base(stepList, byContexts)
        {
        }

        public GremlinLocalPathVariable(List<GremlinVariable> stepList)
            : base(stepList)
        {
        }

    }

    internal class GremlinGlobalPathVariable : GremlinPathVariable
    {
        public GremlinGlobalPathVariable(List<GremlinVariable> stepList, List<GremlinToSqlContext> byContexts)
            : base(stepList, byContexts)
        {
        }

        public GremlinGlobalPathVariable(List<GremlinVariable> stepList)
            : base(stepList)
        {
        }

    }
}
