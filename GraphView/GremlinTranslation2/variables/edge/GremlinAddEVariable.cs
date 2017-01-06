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

        public override List<WSqlStatement> ToSetVariableStatements()
        {
            List<WSqlStatement> statementList = new List<WSqlStatement>();

            var columnK = new List<WColumnReferenceExpression>();
            var selectBlock = new WSelectQueryBlock()
            {
                FromClause = new WFromClause()
            };

            selectBlock.FromClause.TableReferences.Add(FromVariable.ToTableReference());
            selectBlock.FromClause.TableReferences.Add(ToVariable.ToTableReference());

            var fromVarExpr = SqlUtil.GetColumnReferenceExpr(FromVariable.VariableName, "id");
            selectBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(fromVarExpr));

            var toVarExpr = SqlUtil.GetColumnReferenceExpr(ToVariable.VariableName, "id");
            selectBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(toVarExpr));


            //Add edge key-value
            WScalarExpression valueExpr;
            if (EdgeLabel != null)
            {
                columnK.Add(SqlUtil.GetColumnReferenceExpr("label"));
                valueExpr = SqlUtil.GetValueExpr(EdgeLabel);
                selectBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(valueExpr));

            }
            foreach (var property in Properties)
            {
                columnK.Add(SqlUtil.GetColumnReferenceExpr(property.Key));
                valueExpr = SqlUtil.GetValueExpr(property.Value.ToString());
                selectBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(valueExpr));
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
                Target = SqlUtil.GetNamedTableReference("Edge")
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
                Variable = SqlUtil.GetVariableReference(VariableName)
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
