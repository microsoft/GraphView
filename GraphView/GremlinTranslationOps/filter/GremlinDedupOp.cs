using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView.GremlinTranslationOps.filter
{
    internal class GremlinDedupOp: GremlinTranslationOperator
    {
        public List<string> DedupLabels;

        public GremlinDedupOp(params string[] dedupLabels)
        {
            DedupLabels = new List<string>();
            foreach (var dedupLabel in dedupLabels)
            {
                DedupLabels.Add(dedupLabel);
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            //List<object> parametersList = new List<object>();
            //foreach (var dedupLabel in DedupLabels)
            //{
            //    parametersList.Add(inputContext.AliasToGremlinVariableList[dedupLabel].Last().VariableName);
            //}
            //var functionTableReference = GremlinUtil.GetSchemaObjectFunctionTableReference("dedup", parametersList);

            //GremlinDerivedVariable newVariable = new GremlinDerivedVariable(functionTableReference, "dedup");

            //inputContext.AddNewVariable(newVariable, Labels);
            //inputContext.SetDefaultProjection(newVariable);
            //inputContext.SetCurrVariable(newVariable);

            return inputContext;
        }
    }
}
