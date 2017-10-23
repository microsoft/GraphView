using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinPathVariable : GremlinTableVariable
    {
        public List<GremlinVariable> StepList { get; set; }
        public List<List<string>> StepLabelsAtThatMoment { get; set; }
        public List<List<string>> StepLabelsPerhaps { get; set; }
        public List<GremlinToSqlContext> ByContexts { get; set; }
        public List<Tuple<string, string> > LabelPropertyList { get; set; }

        public GremlinPathVariable(List<GremlinVariable> stepList, List<GremlinToSqlContext> byContexts = null,
            string fromLabel = null, string toLabel = null) : base(GremlinVariableType.Path)
        {
            NormalizePathAndLabels(stepList, fromLabel, toLabel);

            if (byContexts == null)
            {
                this.ByContexts = new List<GremlinToSqlContext>();
            }
            else
            {
                this.ByContexts = byContexts;
            }
        }
        private void NormalizePathAndLabels(List<GremlinVariable> stepList, string fromLabel, string toLabel)
        {
            int fromIndex = 0, toIndex = stepList.Count - 1;
            this.StepList = new List<GremlinVariable>();
            this.LabelPropertyList = new List<Tuple<string, string>>();
            this.StepLabelsAtThatMoment = new List<List<string>>();
            this.StepLabelsPerhaps = new List<List<string>>();
            
            this.StepLabelsAtThatMoment.Add(new List<string>());

            for (int index = 0; index < stepList.Count;)
            {
                GremlinVariable step = stepList[index];
                if (step == null)
                {
                    throw new TranslationException("The step should not be null.");
                }
                else if (step is GremlinContextVariable)
                {
                    this.StepList.Add(step);
                    this.StepLabelsAtThatMoment[this.StepLabelsAtThatMoment.Count - 1].AddRange(step.Labels);
                    this.StepLabelsAtThatMoment.Add(new List<string>());
                    ++index;
                }
                else
                {
                    this.StepLabelsAtThatMoment[this.StepLabelsAtThatMoment.Count - 1].AddRange(step.Labels);
                    while (++index < stepList.Count)
                    {
                        if (stepList[index] is GremlinContextVariable &&
                            !((stepList[index] is GremlinRepeatContextVariable) ||
                              (stepList[index] is GremlinUntilContextVariable) ||
                              (stepList[index] is GremlinEmitContextVariable)))
                        {
                            if (step == ((GremlinContextVariable)stepList[index]).RealVariable)
                            {
                                this.StepLabelsAtThatMoment[this.StepLabelsAtThatMoment.Count - 1].AddRange(stepList[index].Labels);
                            }
                        }
                        else
                        {
                            break;
                        }
                    }
                    this.StepList.Add(step);
                    this.StepLabelsAtThatMoment.Add(new List<string>());
                }
            }

            for (int index = 0; index < this.StepList.Count; index++)
            {
                if (this.StepLabelsAtThatMoment[index].Contains(fromLabel))
                {
                    fromIndex = index;
                }
                if (this.StepLabelsAtThatMoment[index].Contains(toLabel))
                {
                    toIndex = index;
                }
                this.StepLabelsPerhaps.Add(new List<string>());
            }

            toIndex = toIndex < this.StepList.Count ? toIndex : this.StepList.Count - 1;
            this.StepList = this.StepList.GetRange(fromIndex, toIndex - fromIndex + 1);
            this.StepLabelsAtThatMoment = this.StepLabelsAtThatMoment.GetRange(fromIndex, toIndex - fromIndex + 1);
            this.StepLabelsPerhaps = this.StepLabelsPerhaps.GetRange(fromIndex, toIndex - fromIndex + 1);

            this.MinPathLength = 0;
            foreach (GremlinVariable step in this.StepList)
            {
                this.MinPathLength += step.MinPathLength;
            }
        }
        
        internal override WScalarExpression ToStepScalarExpr(List<string> composedProperties = null)
        {
            return this.ToCompose1(new List<string>());
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(this.StepList.FindAll(p => p != null && !(p is GremlinContextVariable)));
            foreach (var context in this.ByContexts)
            {
                variableList.AddRange(context.FetchAllVars());
            }
            return variableList;
        }

        internal override List<GremlinTableVariable> FetchAllTableVars()
        {
            List<GremlinTableVariable> variableList = new List<GremlinTableVariable> { this };
            foreach (var context in this.ByContexts)
            {
                variableList.AddRange(context.FetchAllTableVars());
            }
            return variableList;
        }

        internal override bool Populate(string property, string label = null)
        {
            if (base.Populate(property, label))
            {
                foreach (var context in this.ByContexts)
                {
                    context.Populate(property, null);
                }
                return true;
            }
            else
            {
                bool populateSuccess = false;
                foreach (var context in this.ByContexts)
                {
                    populateSuccess |= context.Populate(property, label);
                }
                if (populateSuccess)
                {
                    base.Populate(property, null);
                }
                return populateSuccess;
            }
        }

        internal override bool PopulateStepProperty(string property, string label = null)
        {
            if (this.ProjectedProperties.Contains(property))
            {
                return true;
            }
            
            if (label == null)
            {
                if (property != null)
                {
                    this.ProjectedProperties.Add(property);
                }

                foreach (var step in this.StepList)
                {
                    step.PopulateStepProperty(property, null);
                }
                
                return true;
            }
            else
            {
                Tuple<string, string> labelproperty = new Tuple<string, string>(label, property);
                if (this.LabelPropertyList.Contains(labelproperty))
                {
                    return true;
                }

                bool populateSuccess = false;
                for (int index = 0; index < this.StepList.Count; index++)
                {
                    if (this.StepLabelsAtThatMoment[index].Contains(label) ||
                        this.StepLabelsPerhaps[index].Contains(label))
                    {
                        populateSuccess |= this.StepList[index].PopulateStepProperty(property, null);
                    }
                    else if (this.StepList[index].PopulateStepProperty(property, label))
                    {
                        populateSuccess = true;
                        this.StepLabelsPerhaps[index].Add(label);
                    }
                    else
                    {
                        continue;
                    }

                    int preIndex = index;
                    while (preIndex > 0 && this.StepList[preIndex].MinPathLength == 0)
                    {
                        preIndex--;
                        if (!(this.StepLabelsAtThatMoment[preIndex].Contains(label) || this.StepLabelsPerhaps[preIndex].Contains(label)))
                        {
                            this.StepLabelsPerhaps[preIndex].Add(label);
                        }
                        this.StepList[preIndex].PopulateStepProperty(property, null);
                    }
                }

                if (populateSuccess)
                {
                    this.LabelPropertyList.Add(labelproperty);
                }
                return populateSuccess;
            }
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            List<WSelectQueryBlock> queryBlocks = new List<WSelectQueryBlock>();
            
            //Must toSelectQueryBlock before toCompose1 of variableList in order to populate needed columns
            foreach (var byContext in this.ByContexts)
            {
                queryBlocks.Add(byContext.ToSelectQueryBlock(true));
            }
            
            for (int index = 0; index < this.StepList.Count; index++)
            {
                GremlinVariable step = this.StepList[index];
                if (step is GremlinContextVariable && !((step is GremlinRepeatContextVariable) ||
                                                               (step is GremlinUntilContextVariable) ||
                                                               (step is GremlinEmitContextVariable)))
                {
                    parameters.AddRange(this.StepLabelsAtThatMoment[index].Select(SqlUtil.GetValueExpr));
                    continue;
                }
                List<string> composedProperties = this.ProjectedProperties.Copy();
                foreach (var labelproperty in this.LabelPropertyList)
                {
                    if (!composedProperties.Contains(labelproperty.Item1) &&
                        (this.StepLabelsAtThatMoment[index].Contains(labelproperty.Item1) ||
                        this.StepLabelsPerhaps[index].Contains(labelproperty.Item1)))
                    {
                        composedProperties.Add(labelproperty.Item2);
                    }
                }
                parameters.Add(step.ToStepScalarExpr(composedProperties));
                parameters.AddRange(this.StepLabelsAtThatMoment[index].Select(SqlUtil.GetValueExpr));
                
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
        public GremlinLocalPathVariable(List<GremlinVariable> stepList, List<GremlinToSqlContext> byContexts) : base(stepList, byContexts) {}

        public GremlinLocalPathVariable(List<GremlinVariable> stepList) : base(stepList) {}

        internal override bool PopulateStepProperty(string property, string label = null)
        {
            if (this.ProjectedProperties.Contains(property))
            {
                return true;
            }

            if (label == null)
            {
                this.ProjectedProperties.Add(property);
                this.StepList[0].Populate(property, null);
                for (int index = 1; index < this.StepList.Count; index++)
                {
                    this.StepList[index].PopulateStepProperty(property, null);
                }
                return true;
            }
            else
            {
                Tuple<string, string> labelproperty = new Tuple<string, string>(label, property);
                if (this.LabelPropertyList.Contains(labelproperty))
                {
                    return true;
                }

                bool populateSuccess = false;
                if (this.StepLabelsAtThatMoment[0].Contains(label) || this.StepLabelsPerhaps[0].Contains(label))
                {
                    populateSuccess |= this.StepList[0].Populate(property, null);
                }
                else
                {
                    populateSuccess |= this.StepList[0].Populate(property, label);
                }
                
                for (int index = 1; index < this.StepList.Count; index++)
                {
                    if (this.StepLabelsAtThatMoment[index].Contains(label) ||
                        this.StepLabelsPerhaps[index].Contains(label))
                    {
                        populateSuccess |= this.StepList[index].PopulateStepProperty(property, null);
                    }
                    else if (this.StepList[index].PopulateStepProperty(property, label))
                    {
                        populateSuccess = true;
                        this.StepLabelsPerhaps[index].Add(label);
                    }
                    else
                    {
                        continue;
                    }

                    int preIndex = index;
                    while (preIndex > 0 && this.StepList[preIndex].MinPathLength == 0)
                    {
                        preIndex--;
                        if (!(this.StepLabelsAtThatMoment[preIndex].Contains(label) || this.StepLabelsPerhaps[preIndex].Contains(label)))
                        {
                            this.StepLabelsPerhaps[preIndex].Add(label);
                        }
                        this.StepList[preIndex].PopulateStepProperty(property, null);
                    }
                }

                if (populateSuccess)
                {
                    this.LabelPropertyList.Add(labelproperty);
                }
                return populateSuccess;
            }
        }

    }

    internal class GremlinGlobalPathVariable : GremlinPathVariable
    {
        public GremlinGlobalPathVariable(List<GremlinVariable> stepList, List<GremlinToSqlContext> byContexts, string fromLabel, string toLabel)
            : base(stepList, byContexts, fromLabel, toLabel) {}

        public GremlinGlobalPathVariable(List<GremlinVariable> stepList)
            : base(stepList) {}
    }

    internal class GremlinSelectPathVariable : GremlinPathVariable
    {
        
        public GremlinSelectPathVariable(List<GremlinVariable> stepList)
            : base(stepList) {}

        public void PopulateStepNULL(List<string> selectKeys)
        {
            foreach (string selectKey in selectKeys)
            {
                this.PopulateStepProperty(null, selectKey);
            }
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            List<WSelectQueryBlock> queryBlocks = new List<WSelectQueryBlock>();

            //Must toSelectQueryBlock before toCompose1 of variableList in order to populate needed columns
            foreach (var byContext in this.ByContexts)
            {
                queryBlocks.Add(byContext.ToSelectQueryBlock(true));
            }

            for (int index = 0; index < this.StepList.Count; index++)
            {
                GremlinVariable step = this.StepList[index];
                if (step is GremlinContextVariable && !((step is GremlinRepeatContextVariable) ||
                                                        (step is GremlinUntilContextVariable) ||
                                                        (step is GremlinEmitContextVariable)))
                {
                    parameters.AddRange(this.StepLabelsAtThatMoment[index].Select(SqlUtil.GetValueExpr));
                    continue;
                }
                List<string> composedProperties = this.ProjectedProperties.Copy();
                foreach (var labelproperty in this.LabelPropertyList)
                {
                    if (!composedProperties.Contains(labelproperty.Item1) &&
                        (this.StepLabelsAtThatMoment[index].Contains(labelproperty.Item1) ||
                         this.StepLabelsPerhaps[index].Contains(labelproperty.Item1)))
                    {
                        composedProperties.Add(labelproperty.Item2);
                    }
                }

                if (composedProperties.Count > this.ProjectedProperties.Count)
                {
                    composedProperties.RemoveAll(s => String.IsNullOrEmpty(s));
                    parameters.Add(step.ToStepScalarExpr(composedProperties));
                    parameters.AddRange(this.StepLabelsAtThatMoment[index].Select(SqlUtil.GetValueExpr));
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
