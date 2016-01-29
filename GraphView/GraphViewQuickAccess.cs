// GraphView
// 
// Copyright (c) 2015 Microsoft Corporation
// 
// All rights reserved. 
// 
// MIT License
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
// 
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Globalization;
using System.IO;
using System.Linq;
using Microsoft.SqlServer.TransactSql.ScriptDom;

// For debugging
using System.Diagnostics;
using System.Security.Authentication.ExtendedProtection;
using System.Text;
using Microsoft.Win32.SafeHandles;
using IsolationLevel = Microsoft.SqlServer.TransactSql.ScriptDom.IsolationLevel;
using System.Reflection;

/* Following codes are not available now

namespace GraphView
{
    public class InsEdgeVisitor : WSqlFragmentVisitor
    {
        internal static BindingFlags propertyBindingFlag = BindingFlags.Public | BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Instance;
        /// <summary>
        /// Visitor to cache the insert edge query
        /// </summary>
        /// 
        public override void Visit(WBooleanComparisonExpression node)
        {
            base.Visit(node);
            if (node.SecondExpr is WValueExpression)
            {
                if (node.FirstExpr.ToString().StartsWith("[x]."))
                {
                    string name = node.FirstExpr.ToString().Substring(4);
                    name = name.Substring(1, name.Length - 2);
                    var value = From.GetType().GetField(name, propertyBindingFlag);
                    node.SecondExpr = new WValueExpression(value.GetValue(From).ToString(), value.GetValue(From) is string);
                }
                else if (node.FirstExpr.ToString().StartsWith("[y]."))
                {
                    string name = node.FirstExpr.ToString().Substring(4);
                    name = name.Substring(1, name.Length - 2);
                    var value = From.GetType().GetField(name, propertyBindingFlag);
                    node.SecondExpr = new WValueExpression(value.GetValue(To).ToString(), value.GetValue(To) is string);
                }
                else
                {
                    throw new GraphViewException("Error with unknow format");
                }
            }
        }
        object From, To;
        public void Invoke(WSqlScript root, object _from, object _to)
        {
            From = _from;
            To = _to;
            root.Accept(this);
        }
    }

    /// <summary>
    /// QuickAccess provides some method doing simple graph-like operation
    /// Those methods will generate a T-SQL statement and execute it by SqlConnector.
    /// But it is faster than calling GraphView.ExecuteNonQuery() since it avoid the parsing overhead by caching the similar syntax
    /// We provided following method so far:
    ///     1. Insert a given node
    ///     2. Insert an edge
    /// 
    /// User can pass a instance of a user-defined class as parameters into the function InsNode() or InsEdge()
    /// These method will generate a string with the content related to the instance's fields
    /// Each non-null field will be translate into corresponding T-SQL statement
    /// If you're going to call InsNode(), you should guarantee the table has been existed before calling the method.
    /// If you're going to call InsEdge(), you should guarantee the Node has been existed and your predicate is precise enough to fetch a unique result.
    /// The field in the class could be null. If it is null, It won't be translated into the statement
    /// 
    /// For example, if you have a class [People] in C#:
    ///     class People
    ///     {
    ///         int id;
    ///         string name;
    ///     }
    /// The execution:
    ///     GraphViewConnection.InsNode( new People(1,"Alice") );
    /// Or:
    ///     GraphViewConnection.InsNode( new People(1,null) );
    ///     
    /// Is equivalent to following:
    ///     GraphViewConnection.ExecuteNonQuery("Insert INTO People(id,name) VALUES (1,'ALICE')" );
    /// Or:
    ///     GraphViewConnection.ExecuteNonQuery("Insert INTO People(id) VALUES (1)");
    /// 
    /// Execution:
    ///     GraphViewConnection.InsEdge( new People(1,"Alice"), new People(2,null), "Knows")
    /// 
    /// Is equivalent to :
    ///     GraphViewConnection.ExecuteNonQuery("Insert EDGE INTO People.Knows SELECT x,y FROM People x , People y WHERE x.id=1 AND x.name='Alice' AND y.id=2 ")
    /// 
    /// 
    /// For the types of field, it supports only [int] and [string] so far.
    /// </summary>
    public partial class GraphViewConnection : IDisposable
    {
        internal static BindingFlags propertyBindingFlag = BindingFlags.Public | BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Instance;
        private static SortedDictionary<Tuple<string, string, int, int>, WSqlScript> InsEdgeCache = new SortedDictionary<Tuple<string, string, int, int>, WSqlScript>();
        private List<Tuple<string, string>> GetNameAndValueAsString(FieldInfo[] fields, object instance)
        {
            List<Tuple<string, string>> res = new List<Tuple<string, string>>();
            foreach (var field in fields)
            {
                string varName = field.Name;
                var varValue = field.GetValue(instance);

                if (varValue == null)
                    res.Add(new Tuple<string, string>(varName, "null"));
                else
                {
                    if (varValue is int)
                    {
                        res.Add(new Tuple<string, string>(varName, varValue.ToString()));
                    }
                    else if (varValue is string)
                    {
                        res.Add(new Tuple<string, string>(varName, "\'" + varValue.ToString() + "\'"));
                    }
                    else
                        throw new GraphViewException("unsupported type!");
                }
            }
            return res;
        }

        /// <summary>
        ///  GraphView will insert a node with your instance of a class
        ///  Node's properties are the same as your instance's fields
        ///  It will be inserted into a table whose name is the same as your name of class
        ///  The table must be existed before run this method
        /// </summary>
        /// <param name="Node">A instance of a class. GraphView will insert its content into table which has the same name as this class</param>
        /// <returns>A boolean indicates successful or not</returns>
        public bool InsNode(object Node)
        {

            string header = "";
            string schema = "";
            string values = "";

            string tableName = Node.GetType().Name;

            header = string.Format("INSERT INTO [{0}]", tableName);

            var fields = Node.GetType().GetFields(propertyBindingFlag);

            var elements = GetNameAndValueAsString(fields, Node);

            foreach (var item in elements)
            {
                if (item.Item2 != "null")
                {
                    if (schema.Length > 0)
                    {
                        schema += ",";
                        values += ",";
                    }
                    schema += item.Item1.ToString();
                    values += item.Item2.ToString();
                }
            }

            if (elements.Count == 0)
                throw new GraphViewException("The fileds can't be empty!");
            else
            {
                var cmd = this.Conn.CreateCommand();
                cmd.CommandText = string.Format("{0}({1}) values ({2})", header, schema, values);

                try
                {
                    return cmd.ExecuteNonQuery() == 1;
                }
                catch (SqlException e)
                {
                    throw new SqlExecutionException("Please check your table name and schema", e);
                }
            }
        }

        /// <summary>
        ///  GraphView will insert a edge between two provided nodes
        ///  The nodes must be existed before your run this method
        ///  The properties of node must be exact the same as the node's properties in graph databse
        ///  If multiple nodes could be fetched with those provided properties, multiple edges will be inserted
        /// </summary>
        /// <param name="From">A instance of a class. Indicates start node</param>
        /// <param name="To">A instance of a class. Indicates end node</param>
        /// <param name="EdgeName">The edge column name in the table of @From</param>
        /// <returns>A boolean indicates successful or not</returns>
        public bool InsEdge(object From, object To, string EdgeName)
        {
            string header = string.Format("INSERT EDGE INTO {0}.{1} ", From.GetType().Name, EdgeName);
            string select = string.Format("SELECT x,y FROM {0} x , {1} y ", From.GetType().Name, To.GetType().Name);
            string where = "WHERE ";

            int binary1 = 0;    //  this is a binary indicates each field of FROM is null or not
            int binary2 = 0;    //  this is a binary indicates each field of To is null or not

            var P1 = GetPredicatesAndBinary(From, "x");
            var P2 = GetPredicatesAndBinary(To, "y");

            binary1 = P1.Item2;
            binary2 = P2.Item2;

            var key = new Tuple<string, string, int, int>(P1.Item1, P2.Item1, binary1, binary2);

            if (!InsEdgeCache.ContainsKey(key))
            {
                var cmdT = this.CreateCommand();
                cmdT.CommandText = header + select + where + P1.Item1 + " AND " + P2.Item1;

                WSqlScript Tscript = cmdT.GetScript();
                InsEdgeCache.Add(key, Tscript);
            }
            WSqlScript script;
            bool res = InsEdgeCache.TryGetValue(key, out script);

            var vis = new GraphView.InsEdgeVisitor();

            vis.Invoke(script, From, To);

            var cmd = this.Conn.CreateCommand();
            cmd.CommandText = script.ToString();
            int rows = cmd.ExecuteNonQuery();
            if (rows == 1)
                return true;
            else if (rows == 0)
                return false;
            else
                return true;    //  multi edges inserted!
        }

        /// <summary>
        ///  This method will generate a predicate to help locate the node
        /// </summary>
        /// <param name="instance">The instance provided by user</param>
        /// <returns>A string like "property1 = value1 AND property2 = value2 ..."</returns>
        private Tuple<string, int> GetPredicatesAndBinary(object instance, string pron)
        {
            var fields = instance.GetType().GetFields(propertyBindingFlag);
            string predicate = "";

            int binary = 0;
            var elements = GetNameAndValueAsString(fields, instance);

            foreach (var item in elements)
            {
                binary *= 2;
                if (item.Item2 != "null")
                {
                    if (predicate.Length > 0)
                        predicate += " AND ";
                    predicate += pron + "." + item.Item1 + " = " + item.Item2;
                    binary++;
                }
            }
            if (predicate.Length == 0)
                throw new GraphViewException("The fileds can't be empty!");
            return new Tuple<string, int>(predicate, binary);
        }

    }
}

*/