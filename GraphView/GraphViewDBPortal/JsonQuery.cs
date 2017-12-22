using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView.GraphViewDBPortal
{
    [Serializable]
    internal class JsonQuery : ISerializable
    {
        public List<string> NodeProperties { get; set; }
        public List<string> EdgeProperties { get; set; }

        public WBooleanExpression RawWhereClause;

        public string NodeAlias;
        public string EdgeAlias;

        // Note: this Dict is used to contruct select clause.
        private Dictionary<string, List<WPrimaryExpression>> selectDictionary;
        public HashSet<string> FlatProperties;
        public Dictionary<string, string> JoinDictionary;

        // Only JsonServer needs it, given by JsonServerDbPortal
        public string JsonServerCollectionName;

        private string dummyQueryString;

        public JsonQuery()
        {
            this.FlatProperties = new HashSet<string>();
            this.JoinDictionary = new Dictionary<string, string>();
            this.selectDictionary = new Dictionary<string, List<WPrimaryExpression>>();
        }


        public JsonQuery(JsonQuery rhs)
        {
            this.NodeProperties = rhs.NodeProperties;
            this.EdgeProperties = rhs.EdgeProperties;
            this.RawWhereClause = rhs.RawWhereClause;
            this.NodeAlias = rhs.NodeAlias;
            this.EdgeAlias = rhs.EdgeAlias;
            this.selectDictionary = rhs.selectDictionary;
            this.FlatProperties = new HashSet<string>(rhs.FlatProperties);
            this.JoinDictionary = new Dictionary<string, string>(rhs.JoinDictionary);
        }


        public void AddSelectElement(string selectName, List<WPrimaryExpression> asJsonStrList = null)
        {
            this.selectDictionary.Add(selectName, asJsonStrList);
        }


        public void WhereConjunction(WBooleanExpression condition, BooleanBinaryExpressionType conjunction)
        {
            Debug.Assert(condition != null);
            if (this.RawWhereClause == null)
            {
                this.RawWhereClause = condition;
                return;
            }

            this.RawWhereClause = new WBooleanBinaryExpression
            {
                FirstExpr = new WBooleanParenthesisExpression
                {
                    Expression = this.RawWhereClause
                },
                SecondExpr = new WBooleanParenthesisExpression
                {
                    Expression = condition
                },
                BooleanExpressionType = conjunction
            };
        }

        public string ToDocDbString()
        {
            // construct select clause
            Debug.Assert(this.selectDictionary.Any(), "There is nothing to be selected!");
            List<string> elements = new List<string>();
            foreach (KeyValuePair<string, List<WPrimaryExpression>> kvp in this.selectDictionary)
            {
                Debug.Assert(kvp.Key != "*" || kvp.Value == null, "`*` can't be used with `AS`");
                if (kvp.Value != null && kvp.Value.Any())
                {
                    var sb = new StringBuilder();
                    foreach (WPrimaryExpression expression in kvp.Value)
                    {
                        var valueExp = expression as WValueExpression;
                        if (valueExp != null)
                        {
                            sb.Append(valueExp);
                            continue;
                        }

                        var columnExp = expression as WColumnReferenceExpression;
                        if (columnExp != null)
                        {
                            if (columnExp.ColumnName == "*")
                            {
                                sb.Append($"{columnExp.TableReference}");
                            }
                            else if (columnExp.ColumnName[0] == '[')
                            {
                                // TODO: Refactor, case like doc["partionKey"], try to use AddIdentifier() function of WColumnRefExp.
                                sb.Append($"{columnExp.TableReference}{columnExp.ColumnName}");
                            }
                            else
                            {
                                sb.Append($"{columnExp.TableReference}.{columnExp.ColumnName}");
                            }
                            continue;
                        }

                        throw new QueryExecutionException("Un-supported type of SELECT clause expression");
                    }
                    elements.Add($"{sb} AS {kvp.Key}");
                }
                else
                {
                    elements.Add($"{kvp.Key}");
                }
            }
            string selectClauseString = $"SELECT {string.Join(", ", elements)}";


            // cpmstruct FROM clause with the first element of SelectAlias
            var fromStrBuilder = new StringBuilder();
            fromStrBuilder.AppendFormat("FROM {0}", this.NodeAlias ?? this.EdgeAlias); // TODO: double check here
            string fromClauseString = fromStrBuilder.ToString();


            // construct JOIN clause, because the order of replacement is not matter,
            // so use Dictinaty to store it(JoinDictionary).
            WBooleanExpression whereClauseCopy = this.RawWhereClause.Copy();
            // True --> true
            var booleanWValueExpressionVisitor = new BooleanWValueExpressionVisitor();
            booleanWValueExpressionVisitor.Invoke(whereClauseCopy);

            var normalizeNodePredicatesColumnReferenceExpressionVisitor =
                new NormalizeNodePredicatesWColumnReferenceExpressionVisitor(null);
            normalizeNodePredicatesColumnReferenceExpressionVisitor.AddFlatProperties(this.FlatProperties);
            normalizeNodePredicatesColumnReferenceExpressionVisitor.AddSkipTableName(this.EdgeAlias);

            Dictionary<string, string> referencedProperties =
                normalizeNodePredicatesColumnReferenceExpressionVisitor.Invoke(whereClauseCopy);
            var joinStrBuilder = new StringBuilder();
            foreach (KeyValuePair<string, string> referencedProperty in referencedProperties)
            {
                joinStrBuilder.AppendFormat(" JOIN {0} IN {1}['{2}'] ", referencedProperty.Key,
                    this.NodeAlias, referencedProperty.Value);
            }

            foreach (KeyValuePair<string, string> pair in JoinDictionary)
            {
                joinStrBuilder.AppendFormat(" JOIN {0} IN {1} ", pair.Key, pair.Value);
            }
            string joinClauseString = joinStrBuilder.ToString();


            // WHERE clause
            // convert some E_6.label --> E_6["label"] if needed(Add 'E_6' to visitor.NeedsConvertion before invoke the visitor).

            if (this.EdgeAlias != null)
            {
                var normalizeEdgePredicatesColumnReferenceExpressionVisitor = new DMultiPartIdentifierVisitor();
                normalizeEdgePredicatesColumnReferenceExpressionVisitor.NeedsConvertion.Add(this.EdgeAlias);
                normalizeEdgePredicatesColumnReferenceExpressionVisitor.Invoke(whereClauseCopy);
            }

            // construct where clause string.
            var docDbStringVisitor = new ToDocDbStringVisitor();
            docDbStringVisitor.Invoke(whereClauseCopy);
            string whereClauseString = $"WHERE ({docDbStringVisitor.GetString()})";

            return $"{selectClauseString}\n" +
                   $"{fromClauseString} {joinClauseString}\n" +
                   $"{whereClauseString}";
        }


        public string ToJsonServerString()
        {
            // SELECT clause
            Debug.Assert(this.selectDictionary.Any(), "There is nothing to be selected!");
            // TODO: using StringConcat() function when it is available
            string selectClauseString;
            if (this.selectDictionary.Count == 1 && this.selectDictionary.ContainsKey("*"))
            {
                Debug.Assert(this.selectDictionary["*"] == null, "`*` can't be used with `AS`");
                selectClauseString = $"Doc({this.NodeAlias ?? this.EdgeAlias})";
            }
            else
            {
                List<string> elements = new List<string>();
                foreach (KeyValuePair<string, List<WPrimaryExpression>> kvp in this.selectDictionary)
                {
                    if (kvp.Value != null && kvp.Value.Any())
                    {
                        var attributes = new List<string>();
                        foreach (WPrimaryExpression expression in kvp.Value)
                        {
                            var valueExp = expression as WValueExpression;
                            if (valueExp != null)
                            {
                                attributes.Add($"'{valueExp}'");
                                continue;
                            }

                            var columnExp = expression as WColumnReferenceExpression;
                            if (columnExp != null)
                            {
                                if (columnExp.ColumnName == "*")
                                {
                                    attributes.Add($"Doc({columnExp.TableReference})");
                                }
                                else if (columnExp.ColumnName[0] == '[')
                                {
                                    // NOTE: JsonServer accepts path like: aaa.["bbb"].["ccc"]
                                    // TODO: Deal with ["aaa"]["bbb"]["ccc"] --> .["aaa"].["bbb"].["ccc"]
                                    attributes.Add($"{columnExp.TableReference}{columnExp.ColumnName.Replace("[", ".[")}");
                                }
                                else
                                {
                                    attributes.Add($"{columnExp.TableReference}.{columnExp.ColumnName}");
                                }
                                continue;
                            }

                            throw new QueryExecutionException("Un-supported type of SELECT clause expression");
                        }
                        elements.Add($"StringConcatenate({string.Join(", ", attributes)}) AS {kvp.Key}");
                    }
                    else if (kvp.Key == "*")
                    {
                        throw new QueryExecutionException("`*` can only be used with no one in SELECT clause.");
                    }
                    else
                    {
                        elements.Add($"Doc({kvp.Key}) AS {kvp.Key}");
                    }
                }
                selectClauseString = $"{string.Join(", ", elements)}";
            }

            // Join clause ( in JsonServer, it is called `FOR`, but you know, `JOIN` is what it really do)
            //   if you get a better name, please refactor it.
            var joinStrBuilder = new StringBuilder();
            foreach (KeyValuePair<string, string> pair in JoinDictionary)
            {
                joinStrBuilder.AppendFormat("FOR {0} IN {1}.*\n", pair.Key, pair.Value);
            }
            string joinClauseString = joinStrBuilder.ToString();


            // Where clause
            WBooleanExpression whereClauseCopy = this.RawWhereClause.Copy();
            // N_18.age --> N_18.age.*._value
            var jsonServerStringArrayUnfoldVisitor = new JsonServerStringArrayUnfoldVisitor(this.FlatProperties);
            jsonServerStringArrayUnfoldVisitor.AddSkipTableName(this.EdgeAlias);
            jsonServerStringArrayUnfoldVisitor.Invoke(whereClauseCopy);

            ToJsonServerStringVisitor whereVisitor = new ToJsonServerStringVisitor();
            whereVisitor.Invoke(whereClauseCopy);
            string whereClauseString = whereVisitor.GetString().Replace("[", ".["); // TODO: refactor!

            Debug.Assert(this.JsonServerCollectionName != null, "ToJsonServerString needs the collection name.");
            return $"FOR {this.NodeAlias ?? this.EdgeAlias} IN ('{this.JsonServerCollectionName}')\n" +
                   $"{joinClauseString}" +
                   $"WHERE {whereClauseString}\n" +
                   $"SELECT {selectClauseString}";
        }

        public string ToString(DatabaseType dbType)
        {
            switch (dbType)
            {
                case DatabaseType.DocumentDB:
                    return this.ToDocDbString();
                case DatabaseType.JsonServer:
                    return this.ToJsonServerString();
                default:
                    throw new NotImplementedException();
            }
        }

        void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context)
        {
            GraphViewSerializer.SerializeList(info, "NodeProperties", this.NodeProperties);
            GraphViewSerializer.SerializeList(info, "EdgeProperties", this.EdgeProperties);
            info.AddValue("NodeAlias", this.NodeAlias, typeof(string));
            info.AddValue("EdgeAlias", this.EdgeAlias, typeof(string));
            GraphViewSerializer.SerializeDictionaryList(info, "selectDictionary", this.selectDictionary);
            GraphViewSerializer.SerializeHashSet(info, "FlatProperties", this.FlatProperties);
            GraphViewSerializer.SerializeDictionary(info, "JoinDictionary", this.JoinDictionary);
            info.AddValue("JsonServerCollectionName", this.JsonServerCollectionName, typeof(string));

            this.dummyQueryString = "SELECT *\nFROM T\n" + $"WHERE {this.RawWhereClause.ToString("", true)}";
            info.AddValue("dummyQuery", this.dummyQueryString, typeof(string));
        }

        protected JsonQuery(SerializationInfo info, StreamingContext context)
        {
            this.NodeProperties = GraphViewSerializer.DeserializeList<string>(info, "NodeProperties");
            this.EdgeProperties = GraphViewSerializer.DeserializeList<string>(info, "EdgeProperties");
            this.NodeAlias = info.GetString("NodeAlias");
            this.EdgeAlias = info.GetString("EdgeAlias");
            this.selectDictionary = GraphViewSerializer.DeserializeDictionaryList<string, WPrimaryExpression>(info, "selectDictionary", true);
            this.FlatProperties = GraphViewSerializer.DeserializeHashSet<string>(info, "FlatProperties");
            this.JoinDictionary = GraphViewSerializer.DeserializeDictionary<string, string>(info, "JoinDictionary");
            this.JsonServerCollectionName = info.GetString("JsonServerCollectionName");
            this.dummyQueryString = info.GetString("dummyQuery");
            this.RawWhereClause = new WSqlParser().ParseWhereClauseFromSelect(this.dummyQueryString);
        }

    }

}
