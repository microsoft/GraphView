using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinMatchOp: GremlinTranslationOperator
    {
        public List<GraphTraversal2> MatchTraversals { get; set; }
        public Dictionary<string, List<GraphTraversal2>> MatchTraversalsDict { get; set; }

        public GremlinMatchOp(params GraphTraversal2[] matchTraversals)
        {
            ConfigureStartAndEndSteps(matchTraversals);

            MatchTraversals = new List<GraphTraversal2>();
            MatchTraversalsDict = new Dictionary<string, List<GraphTraversal2>>();
            foreach (var traversal in matchTraversals)
            {
                MatchTraversals.Add(traversal);

                List<string> startLabels = traversal.GetStartOp().GetLabels();
                if (null != startLabels && startLabels.Count > 0)
                {
                    if (MatchTraversalsDict.ContainsKey(startLabels.First()))
                    {
                        MatchTraversalsDict[startLabels.First()].Add(traversal);
                    }
                    else
                    {
                        MatchTraversalsDict[startLabels.First()] = new List<GraphTraversal2>();
                        MatchTraversalsDict[startLabels.First()].Add(traversal);
                    }
                }
            }
            
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            throw new NotImplementedException();
            return inputContext;
        }

        public string FindStartLabel(List<GraphTraversal2> matchTraversals)
        {
            List<string> sort = new List<string>();
            foreach (var matchTraversal in matchTraversals)
            {
                List<string> startLabels = matchTraversal.GetStartOp().GetLabels();
                if (null != startLabels && startLabels.Count > 1)
                {
                    throw new Exception("The start step of a match()-traversal can have at most one label");
                }
                foreach (var startLabel in startLabels)
                {
                    if (!sort.Contains(startLabel)) sort.Add(startLabel);
                }

                List<string> endLabels = matchTraversal.GetEndOp().GetLabels();
                if (null != endLabels && endLabels.Count > 1)
                {
                    throw new Exception("The end step of a match()-traversal can have at most one label");
                }
                if (null != endLabels)
                {
                    foreach (var endLabel in endLabels)
                    {
                        if (!sort.Contains(endLabel)) sort.Add(endLabel);
                    }
                }
            }

            sort.Sort((a, b) =>
            {
                foreach (var matchTraversal in matchTraversals)
                {
                    List<string> startLabels = matchTraversal.GetStartOp().GetLabels();
                    List<string> endLabels = matchTraversal.GetEndOp().GetLabels();
                    if (null != endLabels && endLabels.Count > 0)
                    {
                        if (a.Equals(endLabels.First()) && startLabels.Contains(b))
                            return 1;
                        else if (b.Equals(endLabels.First()) && startLabels.Contains(a))
                            return -1;
                    }
                }
                return 0;
            });

            return sort.First();
        }

        public void ConfigureStartAndEndSteps(params GraphTraversal2[] matchTraversals)
        {
            foreach (var matchTraversal in matchTraversals)
            {
                //List<string> startLabels = matchTraversal.GetStartOp().Labels.Copy();
                //matchTraversal.GetStartOp().ClearLabels();
                List<string> endLabels = matchTraversal.GetEndOp().Labels.Copy();
                matchTraversal.GetEndOp().ClearLabels();
                //if (startLabels.Count > 0)
                //    matchTraversal.InsertAfterOperator(0, new GremlinMatchStartOp(startLabels.First()));
                if (endLabels.Count > 0)
                    matchTraversal.AddGremlinOperator(new GremlinMatchEndOp(endLabels));
            }
        }
    }

    //internal class GremlinMatchStartOp : GremlinTranslationOperator
    //{
    //    public string SelectKey;

    //    public GremlinMatchStartOp(string selectKey)
    //    {
    //        SelectKey = selectKey;
    //    }

    //    public override GremlinToSqlContext GetContext()
    //    {
    //        GremlinToSqlContext inputContext = GetInputContext();

    //        for (var i = inputContext.AliasToGremlinVariableList.Count - 1; i >= 0; i--)
    //        {
    //            if (inputContext.AliasToGremlinVariableList[i].Item1 == SelectKey)
    //            {
    //                inputContext.CurrVariable = inputContext.AliasToGremlinVariableList[i].Item2;
    //                break;
    //            }
    //        }

    //        return inputContext;
    //    }
    //}

    internal class GremlinMatchEndOp : GremlinTranslationOperator
    {
        public GremlinMatchEndOp(List<string> matchKey)
        {
            Labels = matchKey;
        }
        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            return inputContext;
        }
    }
}
