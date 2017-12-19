using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinContextVariable: GremlinVariable
    {
        public GremlinContextVariable(GremlinVariable contextVariable)
        {
            this.RealVariable = contextVariable;
        }

        public GremlinVariable RealVariable { get; set; }

        internal override GremlinVariableType GetVariableType()
        {
            return this.RealVariable.GetVariableType();
        }

        internal override string GetVariableName()
        {
            return this.RealVariable.GetVariableName();
        }
        
        internal override GremlinVariableProperty GetVariableProperty(string property)
        {
            this.ProjectedProperties.Add(property);
            return this.RealVariable.GetVariableProperty(property);
        }

        internal override void As(GremlinToSqlContext currentContext, List<string> labels)
        {
            foreach (var label in labels)
            {
                if (!this.Labels.Contains(label))
                {
                    this.Labels.Add(label);
                }
            }
        }

        internal override bool Populate(string property, string label = null)
        {
            bool populateSuccessfully = false;
            if (label == null || this.Labels.Contains(label))
            {
                populateSuccessfully = true;
                this.RealVariable.Populate(property, null);
            }
            else
            {
                populateSuccessfully |= this.RealVariable.Populate(property, label);
            }
            if (populateSuccessfully && property != null)
            {
                this.ProjectedProperties.Add(property);
            }
            return populateSuccessfully;
        }

        internal override bool PopulateStepProperty(string property, string label = null)
        {
            bool populateSuccessfully = false;
            if (label == null || this.Labels.Contains(label))
            {
                populateSuccessfully = true;
                this.RealVariable.PopulateStepProperty(property, null);
            }
            else
            {
                populateSuccessfully = this.RealVariable.PopulateStepProperty(property, label);
            }
            if (populateSuccessfully && property != null)
            {
                this.ProjectedProperties.Add(property);
            }
            return populateSuccessfully;
        }
        internal override void PopulateLocalPath()
        {
            this.LocalPathLengthLowerBound = 0;
        }
    }

    internal class GremlinRepeatContextVariable : GremlinContextVariable
    {
        public GremlinLocalPathVariable ContextLocalPath { get; set; }
        public List<Tuple<string, string>> LabelPropertyList { get; set; }

        public GremlinRepeatContextVariable(GremlinVariable contextVariable) : base(contextVariable)
        {
            LabelPropertyList = new List<Tuple<string, string>>();
        }

        public void SetContextLocalPath(GremlinLocalPathVariable contextLocalPath)
        {
            this.ContextLocalPath = contextLocalPath;
            foreach (var labelproperty in this.LabelPropertyList)
            {
                this.ContextLocalPath.PopulateStepProperty(labelproperty.Item2, labelproperty.Item1);
            }
        }

        internal override bool PopulateStepProperty(string property, string label = null)
        {
            if (this.ContextLocalPath != null)
            {
                return this.ContextLocalPath.PopulateStepProperty(property, label);
            }
            else
            {
                Tuple<string, string> labelproperty = new Tuple<string, string>(label, property);
                if (!this.LabelPropertyList.Contains(labelproperty))
                {
                    this.LabelPropertyList.Add(labelproperty);
                }
                return true;
            }
        }

        internal override WScalarExpression ToStepScalarExpr(HashSet<string> composedProperties = null)
        {
            return SqlUtil.GetColumnReferenceExpr(GremlinKeyword.RepeatInitalTableName, GremlinKeyword.Path);
        }
    }

    internal class GremlinUntilContextVariable : GremlinContextVariable
    {
        public GremlinLocalPathVariable ContextLocalPath { get; set; }
        public List<Tuple<string, string>> LabelPropertyList { get; set; }
        public GremlinUntilContextVariable(GremlinVariable contextVariable) : base(contextVariable)
        {
            LabelPropertyList = new List<Tuple<string, string>>();
        }
        public void SetContextLocalPath(GremlinLocalPathVariable contextLocalPath)
        {
            this.ContextLocalPath = contextLocalPath;
            foreach (var labelproperty in this.LabelPropertyList)
            {
                this.ContextLocalPath.PopulateStepProperty(labelproperty.Item2, labelproperty.Item1);
            }
        }

        internal override bool PopulateStepProperty(string property, string label = null)
        {
            if (this.ContextLocalPath != null)
            {
                return this.ContextLocalPath.PopulateStepProperty(property, label);
            }
            else
            {
                Tuple<string, string> labelproperty = new Tuple<string, string>(label, property);
                if (!this.LabelPropertyList.Contains(labelproperty))
                {
                    this.LabelPropertyList.Add(labelproperty);
                }
                return true;
            }
        }

        internal override WScalarExpression ToStepScalarExpr(HashSet<string> composedProperties = null)
        {
            return SqlUtil.GetColumnReferenceExpr(GremlinKeyword.RepeatInitalTableName, GremlinKeyword.Path);
        }
    }

    internal class GremlinEmitContextVariable : GremlinContextVariable
    {
        public GremlinLocalPathVariable ContextLocalPath { get; set; }
        public List<Tuple<string, string>> LabelPropertyList { get; set; }
        public GremlinEmitContextVariable(GremlinVariable contextVariable) : base(contextVariable)
        {
            LabelPropertyList = new List<Tuple<string, string>>();
        }
        public void SetContextLocalPath(GremlinLocalPathVariable contextLocalPath)
        {
            this.ContextLocalPath = contextLocalPath;
            foreach (var labelproperty in this.LabelPropertyList)
            {
                this.ContextLocalPath.PopulateStepProperty(labelproperty.Item2, labelproperty.Item1);
            }
        }

        internal override bool PopulateStepProperty(string property, string label = null)
        {
            if (this.ContextLocalPath != null)
            {
                return this.ContextLocalPath.PopulateStepProperty(property, label);
            }
            else
            {
                Tuple<string, string> labelproperty = new Tuple<string, string>(label, property);
                if (!this.LabelPropertyList.Contains(labelproperty))
                {
                    this.LabelPropertyList.Add(labelproperty);
                }
                return true;
            }
        }

        internal override WScalarExpression ToStepScalarExpr(HashSet<string> composedProperties = null)
        {
            return SqlUtil.GetColumnReferenceExpr(GremlinKeyword.RepeatInitalTableName, GremlinKeyword.Path);
        }
    }
}
