using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinAddEVariable: GremlinVariableReference
    {
        private static long _count = 0;

        internal override string GenerateTableAlias()
        {
            return "AddE_" + _count++;
        }

        public GremlinVariableReference FromVariable { get; set; }
        public GremlinVariableReference ToVariable { get; set; }
        public Dictionary<string, object> Properties { get; set; }
        public string EdgeLabel { get; set; }

        public override WTableReference ToTableReference()
        {
            return new WVariableTableReference()
            {
                Variable = GremlinUtil.GetVariableReference(VariableName),
                Alias = GremlinUtil.GetIdentifier(VariableName)
            };
        }

        public override List<WSqlStatement> ToSetVariableStatements()
        {
            List<WSqlStatement> statementList = new List<WSqlStatement>();

            var columnK = new List<WColumnReferenceExpression>();
            var selectBlock = new WSelectQueryBlock();

            selectBlock.FromClause.TableReferences.Add(FromVariable.ToTableReference());
            selectBlock.FromClause.TableReferences.Add(ToVariable.ToTableReference());

            var fromVarExpr = GremlinUtil.GetColumnReferenceExpr(FromVariable.VariableName);
            selectBlock.SelectElements.Add(GremlinUtil.GetSelectScalarExpr(fromVarExpr));

            var toVarExpr = GremlinUtil.GetColumnReferenceExpr(ToVariable.VariableName);
            selectBlock.SelectElements.Add(GremlinUtil.GetSelectScalarExpr(toVarExpr));

            //Add edge key-value
            columnK.Add(GremlinUtil.GetColumnReferenceExpr("label"));
            var valueExpr = GremlinUtil.GetValueExpr(EdgeLabel);
            selectBlock.SelectElements.Add(GremlinUtil.GetSelectScalarExpr(valueExpr));
            foreach (var property in Properties)
            {
                columnK.Add(GremlinUtil.GetColumnReferenceExpr(property.Key));
                valueExpr = GremlinUtil.GetValueExpr(property.Value.ToString());
                selectBlock.SelectElements.Add(GremlinUtil.GetSelectScalarExpr(valueExpr));
            }

            //hack
            string temp = "\"label\", \"" + EdgeLabel + "\"";
            foreach (var property in Properties)
            {
                temp += ", \"" + property.Key + "\", \"" + property.Value + "\"";
            }
            //=====

            var insertStatement = new WInsertSpecification()
            {
                Columns = columnK,
                InsertSource = new WSelectInsertSource() { Select = selectBlock },
                Target = GremlinUtil.GetNamedTableReference("Edge")
            };

            var addEStatement = new WInsertEdgeSpecification(insertStatement)
            {
                SelectInsertSource = new WSelectInsertSource() { Select = selectBlock }
            };

            var setStatement = new WSetVariableStatement()
            {
                Expression = new WScalarSubquery()
                {
                    SubQueryExpr = addEStatement
                },
                Variable = GremlinUtil.GetVariableReference(VariableName)
            };

            statementList.Add(setStatement);
            return statementList;
        }

        public GremlinAddEVariable(string edgeLabel, GremlinVariableReference currVariable)
        {
            Properties = new Dictionary<string, object>();

            VariableName = GenerateTableAlias();
            FromVariable = currVariable;
            ToVariable = currVariable;
            EdgeLabel = edgeLabel;
        }

        internal override void From(GremlinToSqlContext currentContext, string label)
        {
            //FromVariable = currentContext.SelectVariable(label);
        }

        internal override void From(GremlinToSqlContext currentContext, GremlinToSqlContext fromVertexContext)
        {
            GremlinVariableReference newVariableReference;
            var index = currentContext.SetVariables.FindIndex(p => p == this);

            if (fromVertexContext.PivotVariable is GremlinAddVVariable)
            {
                FromVariable = fromVertexContext.PivotVariable as GremlinAddVVariable;
                currentContext.SetVariables.InsertRange(index, fromVertexContext.SetVariables);
            }
            else
            {
                newVariableReference = new GremlinVariableReference(fromVertexContext);
                currentContext.VariableList.Add(newVariableReference);
                currentContext.SetVariables.Insert(index, newVariableReference);
                FromVariable = newVariableReference;
            }
        }

        internal override void InV(GremlinToSqlContext currentContext)
        {
            currentContext.PivotVariable = ToVariable;
        }

        internal override void Property(GremlinToSqlContext currentContext, Dictionary<string, object> properties)
        {
            foreach (var pair in properties)
            {
                Properties[pair.Key] = pair.Value;
            }
        }

        internal override void To(GremlinToSqlContext currentContext, string label)
        {
            throw new NotImplementedException();
        }

        internal override void To(GremlinToSqlContext currentContext, GremlinToSqlContext toVertexContext)
        {
            var index = currentContext.SetVariables.FindIndex(p => p == this);

            if (toVertexContext.PivotVariable is GremlinAddVVariable)
            {
                ToVariable = toVertexContext.PivotVariable as GremlinAddVVariable;
                currentContext.SetVariables.InsertRange(index, toVertexContext.SetVariables);
            }
            else
            {
                GremlinVariableReference newVariableReference = new GremlinVariableReference(toVertexContext);
                currentContext.VariableList.Add(newVariableReference);
                currentContext.SetVariables.Insert(index, newVariableReference);
                ToVariable = newVariableReference;
            }
        }

        internal override void OutV(GremlinToSqlContext currentContext)
        {
            currentContext.PivotVariable = FromVariable;
        }
    }
}
