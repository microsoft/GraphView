using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps
{
    internal class GremlinToSqlContext
    {
        public GremlinVariable RootVariable { get; set; }
        public bool FromOuter;

        public GremlinToSqlContext()
        {
            InheritedVariableList = new List<GremlinVariable>();
            NewVariableList = new List<GremlinVariable>();
            //VariablePredicates = new Dictionary<GremlinVariable, WBooleanExpression>();
            //CrossVariableConditions = new List<WBooleanExpression>();
            Predicates = null;
            ProjectionList = new List<Projection>();
            NewPathList = new List<Tuple<GremlinVariable, GremlinVariable, GremlinVariable>>();
            InheritedPathList = new List<Tuple<GremlinVariable, GremlinVariable, GremlinVariable>>();
            WithPaths = new Dictionary<string, WRepeatPath> ();
            AliasToGremlinVariableList = new Dictionary<string, List<GremlinVariable>>();
            //GroupByVariable = new Tuple<GremlinVariable, GroupByRecord>();
            //OrderByVariable = new Tuple<GremlinVariable, OrderByRecord>();
            Statements = new List<WSqlStatement>();
        }
        /// <summary>
        /// A list of Gremlin variables. The variables are expected to 
        /// follow the (vertex)-(edge|path)-(vertex)-... pattern
        /// </summary>
        //public List<GremlinVariable> RemainingVariableList { get; set; }
        public List<GremlinVariable> InheritedVariableList { get; set; }
        public List<GremlinVariable> NewVariableList;
        /// <summary>
        /// A collection of variables and their predicates
        /// </summary>
        //public Dictionary<GremlinVariable, WBooleanExpression> VariablePredicates { get; set; }

        /// <summary>
        /// A list of boolean expressions, each of which is on multiple variables
        /// </summary>
        //public List<WBooleanExpression> CrossVariableConditions { get; set; }

        public WBooleanExpression Predicates;

        /// <summary>
        /// The variable on which the new traversal operates
        /// </summary>
        public GremlinVariable CurrVariable;

        public Dictionary<string, List<GremlinVariable>> AliasToGremlinVariableList;

        /// <summary>
        /// A list of Gremlin variables and their properties the query projects. 
        /// When no property is specified, the variable projects its "ID":
        /// If the variable is a vertex variable, it projects the vertex ID;
        /// If the variable is an edge variable, it projects the (source vertex ID, sink vertex ID, offset) pair. 
        /// 
        /// The projection is updated, as it is passed through every traversal. 
        /// </summary> 
        public List<Projection> ProjectionList { get; set; }

        public List<Tuple<GremlinVariable, GremlinVariable, GremlinVariable>> NewPathList { get; set; }
        public List<Tuple<GremlinVariable, GremlinVariable, GremlinVariable>> InheritedPathList { get; set; }
        public Dictionary<string, WRepeatPath> WithPaths;
        /// <summary>
        /// The Gremlin variable and its property by which the query groups
        /// </summary>
        public Tuple<GremlinVariable, GroupByRecord> GroupByVariable { get; set; }

        /// <summary>
        /// The Gremlin variable and its property by which the query orders
        /// </summary>
        public Tuple<GremlinVariable, OrderByRecord> OrderByVariable { get; set; }

        public List<WSqlStatement> Statements;

        public void SetLabelsToCurrentVariable(List<string> labels)
        {
            foreach (var label in labels)
            {
                AddAliasToGremlinVariable(label, CurrVariable);
            }
        }

        //public void AddNewVariable(GremlinVariable newVariable, List<string> labels)
        //{
        //    VariableList.Add(newVariable);
        //    NewVariableList.Add(newVariable);
        //    if (labels.Count == 0) return;
        //    foreach (var label in labels)
        //    {
        //        AddAliasToGremlinVariable(label, newVariable);
        //    }
        //}

        public void AddNewVariable(GremlinVariable newVariable)
        {
            NewVariableList.Add(newVariable);
        }

        public GremlinTVFVariable CrossApplyToVariable(GremlinVariable oldVariable, WSchemaObjectFunctionTableReference secondTableRef, List<string> labels)
        {
            int index = -1;
            //can't use findIndex, because when we call SaveCurrentState, VariableList will be copied.
            for (var i = 0; i < NewVariableList.Count; i++)
            {
                if (NewVariableList[i].VariableName == oldVariable.VariableName)
                {
                    WUnqualifiedJoin tableReference = new WUnqualifiedJoin()
                    {
                        FirstTableRef = GremlinUtil.GetNamedTableReference(oldVariable),
                        SecondTableRef = secondTableRef,
                        UnqualifiedJoinType = UnqualifiedJoinType.CrossApply
                    };
                    GremlinTVFVariable newVariable = new GremlinTVFVariable(tableReference);

                    index = i;
                    NewVariableList.RemoveAt(index);
                    NewVariableList.Insert(index, newVariable);
                    return newVariable;
                }
            }
            for (var i = 0; i < InheritedVariableList.Count; i++)
            {
                if (InheritedVariableList[i].VariableName == oldVariable.VariableName)
                {
                    if (InheritedVariableList[i] is GremlinVertexVariable)
                    {
                        GremlinVertexVariable vertexVariable = new GremlinVertexVariable();

                        WUnqualifiedJoin tableReference = new WUnqualifiedJoin()
                        {
                            FirstTableRef = GremlinUtil.GetNamedTableReference(vertexVariable),
                            SecondTableRef = secondTableRef,
                            UnqualifiedJoinType = UnqualifiedJoinType.CrossApply
                        };
                        GremlinTVFVariable newTVFVariable = new GremlinTVFVariable(tableReference);
                        WColumnReferenceExpression key =
                            GremlinUtil.GetColumnReferenceExpression(oldVariable.VariableName, "_value");
                        WColumnReferenceExpression value =
                            GremlinUtil.GetColumnReferenceExpression(vertexVariable.VariableName, "_value");
                        WBooleanComparisonExpression booleanExpr = GremlinUtil.GetBooleanComparisonExpr(key, value);
                        AddPredicate(booleanExpr);
                        AddNewVariable(newTVFVariable);
                        return newTVFVariable;
                    }
                    else
                    {
                        throw new NotImplementedException();
                    }
                    break;
                }
            }
            throw new Exception("Can't replace any variable");
        }

        public void AddAliasToGremlinVariable(string label, GremlinVariable gremlinVar)
        {
            if (!AliasToGremlinVariableList.ContainsKey(label))
            {
                AliasToGremlinVariableList[label] = new List<GremlinVariable>();
            }
            AliasToGremlinVariableList[label].Add(gremlinVar);
        }

        public void SetCurrVariable(GremlinVariable newVar)
        {
            if (CurrVariable is GremlinAddEVariable ||
                (CurrVariable is GremlinDerivedVariable &&
                 (CurrVariable as GremlinDerivedVariable).Type == GremlinDerivedVariable.DerivedType.UNION))
            {
                WSqlStatement statement = ToSetVariableStatement();
                Statements.Add(statement);
            }
            CurrVariable = newVar;
        }

        public bool IsSpecCurrVar()
        {
            if (CurrVariable is GremlinAddEVariable) return true;
            if (CurrVariable is GremlinAddVVariable) return true;
            if (CurrVariable is GremlinDerivedVariable)
            {
                if ((CurrVariable as GremlinDerivedVariable).Type == GremlinDerivedVariable.DerivedType.UNION) return true;
            }
            return false;
        }

        //Projection
        public void SetDefaultProjection(GremlinVariable newGremlinVar)
        {
            ProjectionList.Clear();
            if (newGremlinVar is GremlinVertexVariable ||
                newGremlinVar is GremlinEdgeVariable ||
                newGremlinVar is GremlinAddEVariable ||
                newGremlinVar is GremlinAddVVariable)
            {
                ProjectionList.Add(new ColumnProjection(newGremlinVar, "id"));
            }
            else if (newGremlinVar is GremlinTVFVariable || newGremlinVar is GremlinDerivedVariable)
            {
                ProjectionList.Add(new ColumnProjection(newGremlinVar, "_value"));
            }
            else if (newGremlinVar is GremlinVariableReference)
            {
                if ((newGremlinVar as GremlinVariableReference).Type == VariableType.NODE
                    || (newGremlinVar as GremlinVariableReference).Type == VariableType.EGDE)
                {
                    ProjectionList.Add(new ColumnProjection(newGremlinVar, "id"));
                }
                else if ((newGremlinVar as GremlinVariableReference).Type == VariableType.VALUE
                    || (newGremlinVar as GremlinVariableReference).Type == VariableType.PROPERTIES)
                {
                    ProjectionList.Add(new ColumnProjection(newGremlinVar, "_value"));
                }
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void SetCurrProjection(params Projection[] projections)
        {
            ProjectionList.Clear();
            foreach (var projection in projections)
            {
                ProjectionList.Add(projection);
            }
        }

        public void SetCurrProjection(List<Projection> projections)
        {
            ProjectionList = projections;
        }

        public void SetStarProjection()
        {
            ProjectionList.Clear();
            ProjectionList.Add(new StarProjection());
        }

        public void ProcessProjectWithFunctionCall(List<string> labels, string functionName, List<WScalarExpression> parameterList)
        {
            WFunctionCall functionCall = GremlinUtil.GetFunctionCall(functionName, parameterList);

            FunctionCallProjection funcionCallProjection = new FunctionCallProjection(functionCall);
            SetCurrProjection(funcionCallProjection);

            GremlinDerivedVariable newVariable = new GremlinDerivedVariable(ToSelectQueryBlock(), functionName);
            ClearAndCreateNewContextInfo();
            AddNewVariable(newVariable);
            SetCurrVariable(newVariable);
            SetStarProjection();
        }

        //------

        public WBooleanExpression ToSqlBoolean()
        {
            if (NewVariableList.Count == 0)
            {
                return Predicates;
            }
            else
            {
                WSqlStatement subQueryExpr = ToSelectQueryBlock();
                return GremlinUtil.GetExistPredicate(subQueryExpr);
            }
        }

        public WScalarExpression ToSqlScalar()
        {
            return null;
        }

        public WSqlScript ToSqlScript()
        {
            WSqlScript script = new WSqlScript()
            {
                Batches = new List<WSqlBatch>()
            };
            List<WSqlBatch> batchList = GetBatchList();
            script.Batches = batchList;
            return script;
        }

        public List<WSqlBatch> GetBatchList()
        {
            List<WSqlBatch> batchList = new List<WSqlBatch>();
            WSqlBatch batch = new WSqlBatch()
            {
                Statements = new List<WSqlStatement>()
            };
            batch.Statements = GetStatements();
            batchList.Add(batch);
            return batchList;
        }

        public List<WSqlStatement> GetStatements()
        {
            Statements.Add(ToSqlStatement());
            List<WSqlStatement> withoutEmptyStatement = new List<WSqlStatement>();
            foreach (var statement in Statements)
            {
                if (statement != null)
                    withoutEmptyStatement.Add(statement);
            }
            return withoutEmptyStatement;
        }

        public WSetVariableStatement GetOrCreateSetVariableStatement()
        {
            if (CurrVariable is GremlinVariableReference)
            {
                return (CurrVariable as GremlinVariableReference).Statement;
            }
            else
            {
                WSetVariableStatement statement = ToSetVariableStatement();
                Statements.Add(statement);
                return statement;
            }
        }

        public WSetVariableStatement ToSetVariableStatement()
        {
            return GremlinUtil.GetSetVariableStatement(CurrVariable, ToSqlStatement());
        }

        public WSqlStatement ToSqlStatement()
        {
            if (CurrVariable is GremlinAddEVariable && (CurrVariable as GremlinAddEVariable).IsGenerateSql == false)
            {
                (CurrVariable as GremlinAddEVariable).IsGenerateSql = true;
                return ToAddESqlQuery(CurrVariable as GremlinAddEVariable);
            }
            if (CurrVariable is GremlinAddVVariable && (CurrVariable as GremlinAddVVariable).IsGenerateSql == false)
            {
                (CurrVariable as GremlinAddVVariable).IsGenerateSql = true;
                return ToAddVSqlQuery(CurrVariable as GremlinAddVVariable);
            }
            //if (CurrVariable is GremlinDerivedVariable)
            //{
            //    if ((CurrVariable as GremlinDerivedVariable).Type == GremlinDerivedVariable.DerivedType.UNION)
            //    {
            //        WSetVariableStatement statement = GremlinUtil.GetSetVariableStatement(CurrVariable.VariableName, (CurrVariable as GremlinDerivedVariable).Statement);
            //        return statement;
            //    }
            //}
            else
            {
                return ToSelectQueryBlock();
            }
        }

        public WSelectQueryBlock ToSelectQueryBlock()
        {
            // Construct the new Select Component
            var newSelectElementClause = GetSelectElement();

            //Consturct the new From Cluase;
            var newFromClause = GetFromClause();

            // Construct the new Match Cluase
            var newMatchClause = GetMatchClause();

            // Construct the Where Clause
            var newWhereClause = GetWhereClause();

            // Construct the OrderBy Clause
            var newOrderByClause = GetOrderByClause();
            
            // Construct the GroupBy Clause
            var newGroupByClause = GetGroupByClause();

            // Construct the WithPath Clause
            var newWithPathClause = GetWithPathClause();

            // Construct the SelectBlock
            return new WSelectQueryBlock()
            {
                FromClause = newFromClause,
                SelectElements = newSelectElementClause,
                WhereClause = newWhereClause,
                MatchClause = newMatchClause,
                OrderByClause = newOrderByClause,
                GroupByClause = newGroupByClause,
                WithPathClause2 = newWithPathClause
            };
        }

        public WWithPathClause2 GetWithPathClause()
        {
            return new WWithPathClause2(WithPaths); 
        }

        public WSqlStatement ToAddESqlQuery(GremlinAddEVariable currVar)
        {
            var columnK = new List<WColumnReferenceExpression>();

            var selectBlock = new WSelectQueryBlock()
            {
                SelectElements = new List<WSelectElement>(),
                FromClause = new WFromClause()
                {
                    TableReferences = new List<WTableReference>()
                }
            };
            selectBlock.FromClause.TableReferences.Add(GremlinUtil.GetTableReferenceFromVariable(currVar.FromVariable));
            selectBlock.FromClause.TableReferences.Add(GremlinUtil.GetTableReferenceFromVariable(currVar.ToVariable));

            var fromVarExpr = GremlinUtil.GetColumnReferenceExpression(currVar.FromVariable.VariableName);
            selectBlock.SelectElements.Add(GremlinUtil.GetSelectScalarExpression(fromVarExpr));

            var toVarExpr = GremlinUtil.GetColumnReferenceExpression(currVar.ToVariable.VariableName);
            selectBlock.SelectElements.Add(GremlinUtil.GetSelectScalarExpression(toVarExpr));

            //Add edge key-value
            columnK.Add(GremlinUtil.GetColumnReferenceExpression("label"));
            var valueExpr = GremlinUtil.GetValueExpression(currVar.EdgeLabel);
            selectBlock.SelectElements.Add(GremlinUtil.GetSelectScalarExpression(valueExpr));
            foreach (var property in currVar.Properties)
            {
                columnK.Add(GremlinUtil.GetColumnReferenceExpression(property.Key));
                valueExpr = GremlinUtil.GetValueExpression(property.Value.ToString());
                selectBlock.SelectElements.Add(GremlinUtil.GetSelectScalarExpression(valueExpr));
            }

            //hack
            string temp = "\"label\", \"" + currVar.EdgeLabel + "\"";
            foreach (var property in currVar.Properties)
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

            return new WInsertEdgeSpecification(insertStatement)
            {
                SelectInsertSource = new WSelectInsertSource() { Select = selectBlock }
            };
        }

        public WSqlStatement ToAddVSqlQuery(GremlinAddVVariable currVar)
        {
            var columnK = new List<WColumnReferenceExpression>();
            var columnV = new List<WScalarExpression>();

            if (currVar.VertexLabel != null)
            {
                columnK.Add(GremlinUtil.GetColumnReferenceExpression("label"));
                columnV.Add(GremlinUtil.GetValueExpression(currVar.VertexLabel));
            }

            foreach (var property in currVar.Properties)
            {
                columnK.Add(GremlinUtil.GetColumnReferenceExpression(property.Key));
                columnV.Add(GremlinUtil.GetValueExpression(property.Value));
            }

            var row = new List<WRowValue>() {new WRowValue() {ColumnValues = columnV}};
            var source = new WValuesInsertSource() {RowValues = row};

            var insertStatement = new WInsertSpecification()
            {
                Columns = columnK,
                InsertSource = source,
                Target = GremlinUtil.GetNamedTableReference("Node")
            };

            return new WInsertNodeSpecification(insertStatement);
        }

        public WFromClause GetFromClause()
        {
            var newFromClause = new WFromClause() { TableReferences = new List<WTableReference>() };
            for (var i = 0; i < NewVariableList.Count; i++)
            {
                GremlinVariable currVar = NewVariableList[i];
                var tableReference = GremlinUtil.GetTableReferenceFromVariable(currVar);
                //AddTableReference(currVar);
                if (tableReference != null)
                    newFromClause.TableReferences.Add(tableReference);
            }
            return newFromClause;
        }

        public WMatchClause GetMatchClause()
        {
            var newMatchClause = new WMatchClause() { Paths = new List<WMatchPath>() };
            foreach (var path in NewPathList)
            {
                newMatchClause.Paths.Add(GremlinUtil.GetMatchPath(path));
            }
            return newMatchClause;
        }

        public List<WSelectElement> GetSelectElement()
        {
            var newSelectElementClause = new List<WSelectElement>();
            foreach (var projection in ProjectionList)
            {
                 newSelectElementClause.Add(projection.ToSelectElement());
            }
            return newSelectElementClause;
        }

        public WWhereClause GetWhereClause()
        {
            return new WWhereClause() { SearchCondition = Predicates };
        }

        public WOrderByClause GetOrderByClause()
        {
            if (OrderByVariable == null)
            {
                return new WOrderByClause()
                {
                    OrderByElements = new List<WExpressionWithSortOrder>()
                };
            }

            OrderByRecord orderByRecord = OrderByVariable.Item2;
            WOrderByClause newOrderByClause = new WOrderByClause()
            {
                OrderByElements = orderByRecord.SortOrderList
            };
            return newOrderByClause;
        }

        public WGroupByClause GetGroupByClause()
        {
            if (GroupByVariable == null)
            {
                return new WGroupByClause()
                {
                    GroupingSpecifications = new List<WGroupingSpecification>()
                };
            }
            GroupByRecord groupByRecord = GroupByVariable.Item2;
            WGroupByClause newGroupByClause = new WGroupByClause()
            {
                GroupingSpecifications = groupByRecord.GroupingSpecList
            };

            return newGroupByClause;
        }


        public WDeleteSpecification ToSqlDelete()
        {
            if (CurrVariable is GremlinVertexVariable)
            {
                return ToSqlDeleteNode();
            }
            else if (CurrVariable is GremlinEdgeVariable)
            {
                return ToSqlDeleteEdge();
            }
            else
            {
                return null;
            }
        }

        public WDeleteNodeSpecification ToSqlDeleteNode()
        {
            // delete node
            // where node.id in (subquery)
            //SetProjection("id");
            WSelectQueryExpression selectQueryExpr = ToSelectQueryBlock() as WSelectQueryBlock;
            WInPredicate inPredicate = new WInPredicate()
            {
                Subquery = new WScalarSubquery() { SubQueryExpr = selectQueryExpr },
                Expression = GremlinUtil.GetColumnReferenceExpression("node", "id")
            };

            WWhereClause newWhereClause = new WWhereClause() { SearchCondition = inPredicate };
            WNamedTableReference newTargetClause = GremlinUtil.GetNamedTableReference("node");

            return new WDeleteNodeSpecification()
            {
                WhereClause = newWhereClause,
                Target = newTargetClause
            };
        }

        public WDeleteEdgeSpecification ToSqlDeleteEdge()
        {
            return new WDeleteEdgeSpecification(ToSelectQueryBlock() as WSelectQueryBlock);
        }

        public void AddPaths(GremlinVariable source, GremlinVariable edge, GremlinVariable target)
        {
            //if (source.GetType() == typeof(GremlinVertexVariable) )
            if (source.Type == VariableType.NODE)
            {
                NewPathList.Add(new Tuple<GremlinVariable, GremlinVariable, GremlinVariable>
                    (source, edge, target));
            }
            else
            {
                throw new Exception("Edges can't have a out step.");
            }
        }

        public GremlinVariable GetSourceNode(GremlinVariable edge)
        {
            foreach (var path in NewPathList)
            {
                if (path.Item2.VariableName == edge.VariableName) return path.Item1;
            }
            foreach (var path in InheritedPathList)
            {
                if (path.Item2.VariableName == edge.VariableName) return path.Item1;
            }
            throw new NotImplementedException();
        }

        public GremlinVariable GetSinkNode(GremlinVariable edge)
        {
            foreach (var path in NewPathList)
            {
                if (path.Item2.VariableName == edge.VariableName) return path.Item3;
            }
            foreach (var path in InheritedPathList)
            {
                if (path.Item2.VariableName == edge.VariableName) return path.Item3;
            }
            throw new NotImplementedException();
        }

        public void AddPredicate(WBooleanExpression expr)
        {
            Predicates = Predicates == null ? expr : new WBooleanBinaryExpression()
            {
                BooleanExpressionType = BooleanBinaryExpressionType.And,
                FirstExpr = Predicates,
                SecondExpr = expr
            };
        }

        public void AddLabelsPredicatesToEdge(List<string> edgeLabels, GremlinEdgeVariable edgeVar)
        {
            foreach (var edgeLabel in edgeLabels)
            {
                WValueExpression predicateValue = new WValueExpression(edgeLabel, true);
                WBooleanComparisonExpression comExpression = new WBooleanComparisonExpression()
                {
                    ComparisonType = BooleanComparisonType.Equals,
                    FirstExpr = GremlinUtil.GetColumnReferenceExpression(edgeVar.VariableName, "label"),
                    SecondExpr = predicateValue
                };
                AddPredicate(comExpression);
            }

        }

        public void ClearAndCreateNewContextInfo()
        {
            CurrVariable = null;
            InheritedVariableList = new List<GremlinVariable>();
            NewVariableList = new List<GremlinVariable>();
            NewPathList = new List<Tuple<GremlinVariable, GremlinVariable, GremlinVariable>>();
            InheritedPathList = new List<Tuple<GremlinVariable, GremlinVariable, GremlinVariable>>();
            ProjectionList = new List<Projection>();
            GroupByVariable = null;
            OrderByVariable = null;
            Predicates = null;
        }

        private Stack<GremlinVariable> _storedCurrVariableStack = new Stack<GremlinVariable>();
        private Stack<List<GremlinVariable>> _storedInheritedVariableListStack = new Stack<List<GremlinVariable>>();
        private Stack<List<GremlinVariable>> _storedNewVariableListStack = new Stack<List<GremlinVariable>>();
        private Stack<List<Projection>> _storedCurrProjectionStack = new Stack<List<Projection>>();
        private Stack<List<Tuple<GremlinVariable, GremlinVariable, GremlinVariable>>> _storedNewPathListStack = new Stack<List<Tuple<GremlinVariable, GremlinVariable, GremlinVariable>>>();
        private Stack<List<Tuple<GremlinVariable, GremlinVariable, GremlinVariable>>> _storedInheritedPathListStack = new Stack<List<Tuple<GremlinVariable, GremlinVariable, GremlinVariable>>>();
        private Stack<WBooleanExpression> _storedPredicatedStack = new Stack<WBooleanExpression>();
        private Stack<Tuple<GremlinVariable, GroupByRecord>> _storedGroupByVariableStack = new Stack<Tuple<GremlinVariable, GroupByRecord>>();
        private Stack<Tuple<GremlinVariable, OrderByRecord>> _storedOrderByVariableStack = new Stack<Tuple<GremlinVariable, OrderByRecord>>();
        private bool _isSubTraversal = false;
        public void SaveCurrentState()
        {
            _isSubTraversal = true;
            _storedCurrVariableStack.Push(CurrVariable);
            _storedInheritedVariableListStack.Push(InheritedVariableList.Copy());
            _storedNewVariableListStack.Push(NewVariableList.Copy());
            _storedCurrProjectionStack.Push(ProjectionList.Copy());
            _storedNewPathListStack.Push(NewPathList.Copy());
            _storedInheritedPathListStack.Push(InheritedPathList.Copy());
            _storedPredicatedStack.Push(Predicates);
            _storedOrderByVariableStack.Push(OrderByVariable);
            _storedGroupByVariableStack.Push(GroupByVariable);
        }

        public void ResetSavedState()
        {
            CurrVariable = _storedCurrVariableStack.Pop();
            InheritedVariableList = _storedInheritedVariableListStack.Pop();
            NewVariableList = _storedNewVariableListStack.Pop();
            ProjectionList = _storedCurrProjectionStack.Pop();
            NewPathList = _storedNewPathListStack.Pop();
            InheritedPathList = _storedInheritedPathListStack.Pop();
            Predicates = _storedPredicatedStack.Pop();
            OrderByVariable = _storedOrderByVariableStack.Pop();
            GroupByVariable = _storedGroupByVariableStack.Pop();
            if (_storedCurrVariableStack.Count == 0) _isSubTraversal = false;
        }
    }
}
