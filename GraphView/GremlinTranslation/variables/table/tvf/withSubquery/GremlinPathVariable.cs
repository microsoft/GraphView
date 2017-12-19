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
        /// <summary>
        /// For each path step, a list of labels assigned to it through the annotation step as(). 
        /// </summary>
        public List<List<string>> AnnotatedLabels { get; set; }
        /// <summary>
        /// For each path step, a list of labels inherited from its following steps 
        /// should the following steps are annotated but produce no content and as a result 
        /// the annotation step(s) have to pass the labels to this step.   
        /// </summary>
        public List<List<string>> InheritedLabels { get; set; }
        public List<GremlinToSqlContext> ByContexts { get; set; }
        /// <summary>
        /// A list of label-property pairs such that all path steps annotated with a label 
        /// should populate the designated property. 
        /// </summary>
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
            this.AnnotatedLabels = new List<List<string>>();
            this.InheritedLabels = new List<List<string>>();
            
            this.AnnotatedLabels.Add(new List<string>());

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
                    this.AnnotatedLabels[this.AnnotatedLabels.Count - 1].AddRange(step.Labels);
                    this.AnnotatedLabels.Add(new List<string>());
                    ++index;
                }
                else
                {
                    this.AnnotatedLabels[this.AnnotatedLabels.Count - 1].AddRange(step.Labels);
                    while (++index < stepList.Count)
                    {
                        if (stepList[index] is GremlinContextVariable &&
                            !((stepList[index] is GremlinRepeatContextVariable) ||
                              (stepList[index] is GremlinUntilContextVariable) ||
                              (stepList[index] is GremlinEmitContextVariable)))
                        {
                            if (step == ((GremlinContextVariable)stepList[index]).RealVariable)
                            {
                                this.AnnotatedLabels[this.AnnotatedLabels.Count - 1].AddRange(stepList[index].Labels);
                            }
                        }
                        else
                        {
                            break;
                        }
                    }
                    this.StepList.Add(step);
                    this.AnnotatedLabels.Add(new List<string>());
                }
            }

            for (int index = 0; index < this.StepList.Count; index++)
            {
                if (this.AnnotatedLabels[index].Contains(fromLabel))
                {
                    fromIndex = index;
                }
                if (this.AnnotatedLabels[index].Contains(toLabel))
                {
                    toIndex = index;
                }
                this.InheritedLabels.Add(new List<string>());
            }

            toIndex = toIndex < this.StepList.Count ? toIndex : this.StepList.Count - 1;
            this.StepList = this.StepList.GetRange(fromIndex, toIndex - fromIndex + 1);
            this.AnnotatedLabels = this.AnnotatedLabels.GetRange(fromIndex, toIndex - fromIndex + 1);
            this.InheritedLabels = this.InheritedLabels.GetRange(fromIndex, toIndex - fromIndex + 1);

            this.LocalPathLengthLowerBound = 0;
            foreach (GremlinVariable step in this.StepList)
            {
                this.LocalPathLengthLowerBound += step.LocalPathLengthLowerBound;
            }
        }
        
        internal override WScalarExpression ToStepScalarExpr(HashSet<string> composedProperties = null)
        {
            return this.ToCompose1(new HashSet<string>());
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
            bool populateSuccessfully = false;
            if (label == null || this.Labels.Contains(label))
            {
                populateSuccessfully = true;
                foreach (GremlinToSqlContext context in this.ByContexts)
                {
                    context.Populate(property, null);
                }
            }
            else
            {
                foreach (GremlinToSqlContext context in this.ByContexts)
                {
                    populateSuccessfully |= context.Populate(property, label);
                }
            }
            return populateSuccessfully;
        }

        internal override bool PopulateStepProperty(string property, string label = null)
        {
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

                bool populateSuccessfully = false;
                for (int index = 0; index < this.StepList.Count; index++)
                {
                    if (this.AnnotatedLabels[index].Contains(label) ||
                        this.InheritedLabels[index].Contains(label))
                    {
                        populateSuccessfully |= this.StepList[index].PopulateStepProperty(property, null);
                    }
                    else if (this.StepList[index].PopulateStepProperty(property, label))
                    {
                        populateSuccessfully = true;
                        this.InheritedLabels[index].Add(label);
                    }
                    else
                    {
                        continue;
                    }

                    int preIndex = index;
                    while (preIndex > 0 && this.StepList[preIndex].LocalPathLengthLowerBound == 0)
                    {
                        preIndex--;
                        if (!(this.AnnotatedLabels[preIndex].Contains(label) || this.InheritedLabels[preIndex].Contains(label)))
                        {
                            this.InheritedLabels[preIndex].Add(label);
                        }
                        this.StepList[preIndex].PopulateStepProperty(property, null);
                    }
                }

                if (populateSuccessfully)
                {
                    this.LabelPropertyList.Add(labelproperty);
                }
                return populateSuccessfully;
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
                    parameters.AddRange(this.AnnotatedLabels[index].Select(SqlUtil.GetValueExpr));
                    continue;
                }
                HashSet<string> composedProperties = new HashSet<string>(this.ProjectedProperties);
                foreach (var labelproperty in this.LabelPropertyList)
                {
                    if (!composedProperties.Contains(labelproperty.Item1) &&
                        (this.AnnotatedLabels[index].Contains(labelproperty.Item1) ||
                        this.InheritedLabels[index].Contains(labelproperty.Item1)))
                    {
                        composedProperties.Add(labelproperty.Item2);
                    }
                }
                parameters.Add(step.ToStepScalarExpr(composedProperties));
                parameters.AddRange(this.AnnotatedLabels[index].Select(SqlUtil.GetValueExpr));
                
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
                if (this.AnnotatedLabels[0].Contains(label) || this.InheritedLabels[0].Contains(label))
                {
                    populateSuccess |= this.StepList[0].Populate(property, null);
                }
                else
                {
                    populateSuccess |= this.StepList[0].Populate(property, label);
                }
                
                for (int index = 1; index < this.StepList.Count; index++)
                {
                    if (this.AnnotatedLabels[index].Contains(label) ||
                        this.InheritedLabels[index].Contains(label))
                    {
                        populateSuccess |= this.StepList[index].PopulateStepProperty(property, null);
                    }
                    else if (this.StepList[index].PopulateStepProperty(property, label))
                    {
                        populateSuccess = true;
                        this.InheritedLabels[index].Add(label);
                    }
                    else
                    {
                        continue;
                    }

                    int preIndex = index;
                    while (preIndex > 0 && this.StepList[preIndex].LocalPathLengthLowerBound == 0)
                    {
                        preIndex--;
                        if (!(this.AnnotatedLabels[preIndex].Contains(label) || this.InheritedLabels[preIndex].Contains(label)))
                        {
                            this.InheritedLabels[preIndex].Add(label);
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
                    parameters.AddRange(this.AnnotatedLabels[index].Select(SqlUtil.GetValueExpr));
                    continue;
                }
                HashSet<string> composedProperties = new HashSet<string>(this.ProjectedProperties);
                foreach (var labelproperty in this.LabelPropertyList)
                {
                    if (!composedProperties.Contains(labelproperty.Item1) &&
                        (this.AnnotatedLabels[index].Contains(labelproperty.Item1) ||
                         this.InheritedLabels[index].Contains(labelproperty.Item1)))
                    {
                        composedProperties.Add(labelproperty.Item2);
                    }
                }

                if (composedProperties.Count > this.ProjectedProperties.Count)
                {
                    composedProperties.RemoveWhere(s => String.IsNullOrEmpty(s));
                    parameters.Add(step.ToStepScalarExpr(composedProperties));
                    parameters.AddRange(this.AnnotatedLabels[index].Select(SqlUtil.GetValueExpr));
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
