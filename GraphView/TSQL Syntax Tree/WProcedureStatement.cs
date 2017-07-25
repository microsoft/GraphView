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
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using GraphView.TSQL_Syntax_Tree;

// using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    public partial class WDeclareVariableElement : WSqlFragment
    {
        public WDataTypeReference DataType { get; set; }
        public WScalarExpression Value { get; set; }
        public Identifier VariableName { get; set; }

        internal override bool OneLine()
        {
            return Value.OneLine();
        }

        internal override string ToString(string indent)
        {
            if (OneLine())
            {
                return string.Format("{0}{1} = {2}", indent, VariableName.ToString(""), Value.ToString(""));
            }
            else
            {
                return string.Format("{0}{1} = \r\n{2}",
                    indent, 
                    VariableName.ToString(""), 
                    Value.ToString(indent + "  "));
            }
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (Value != null)
            {
                Value.Accept(visitor);
            }
            base.AcceptChildren(visitor);
        }
    }

    public enum ParameterModifier
    {
        None = 0,
        Output = 1,
        ReadOnly = 2
    }

    public partial class WProcedureParameter : WDeclareVariableElement
    {
        public bool IsVarying { get; set; }
        public ParameterModifier Modifier { get; set; }

        
    }

    public abstract partial class WProcedureStatement : WSqlStatement
    {
        internal IList<WProcedureParameter> Parameters { get; set; }
        internal IList<WSqlStatement> StatementList { get; set; }

        internal override bool OneLine()
        {
            return false;
        }

        protected string ParameterListToString(string indent)
        {
            if (Parameters == null || Parameters.Count == 0)
            {
                return "";
            }

            var sb = new StringBuilder(1024);

            var i = 0;
            while (Parameters != null && i < Parameters.Count)
            {
                sb.AppendFormat("{0}{1} {2}", indent, Parameters[i].VariableName.Value,
                    TsqlFragmentToString.DataType(Parameters[i].DataType));

                if (i < Parameters.Count - 1)
                {
                    sb.Append(",\r\n");
                }

                i++;
            }

            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (StatementList != null)
            {
                var index = 0;
                for (var count = StatementList.Count; index < count; ++index)
                    StatementList[index].Accept(visitor);
            }
            base.AcceptChildren(visitor);
        }
    }

    public partial class WProcedureReference : WSqlFragment
    {
        public WSchemaObjectName Name { get; set; }
        // Changed from Literal to WValueExpression
        public WValueExpression Number { get; set; }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }
    }
    public enum ProcedureOptionKind
    {
        Encryption = 0,
        Recompile = 1,
        ExecuteAs = 2,
        NativeCompilation = 3,
        SchemaBinding = 4
    }

    public partial class WProcedureOption : WSqlFragment
    {
        public ProcedureOptionKind OptionKind { get; set; }
    }

    public partial class WCreateProcedureStatement : WProcedureStatement
    {
        internal bool IsForReplication { get; set; }
        internal IList<WProcedureOption> Options { get; set; }
        internal WProcedureReference ProcedureReference { get; set; }

        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(1024);

            sb.AppendFormat("{0}CREATE PROCEDURE {1} \r\n", indent,
                ProcedureReference.Name);

            if (ProcedureReference.Number != null)
                sb.AppendFormat(";{0}", ProcedureReference.Number.Value);

            sb.Append(ParameterListToString(indent + " "));

            if (Options != null && Options.Any())
            {
                sb.Append("\r\n WITH (");
                var index = 0;
                for (var count = Options.Count; index < count; ++index)
                {
                    if (index > 0)
                        sb.Append(", ");
                    sb.Append(Options[index]);
                }
                sb.Append(')');
            }

            if (IsForReplication)
                sb.Append("\r\n FOR REPLICATION ");

            sb.Append("\r\n AS \r\n");

            sb.AppendFormat("{0}\r\n", indent);

            sb.Append(StatementListToString(StatementList, indent));

            return sb.ToString();
        }
    }

    public abstract class WFunctionReturnType : WSqlFragment { }

    public partial class WScalarFunctionReturnType : WFunctionReturnType
    {
        public WDataTypeReference DataType { get; set; }
    }

    public partial class WTableValuedFunctionReturnType : WFunctionReturnType
    {
        public WSelectStatement SelectStatement { get; set; }
    }

    public partial class WSelectFunctionReturnType : WFunctionReturnType
    {

    }


    public abstract partial class WFunctionStatement : WProcedureStatement
    {
        internal WSchemaObjectName Name;
        internal WFunctionReturnType ReturnType;

        protected string FunctionBodyToString(string indent)
        {
            var sb = new StringBuilder();

            switch (ReturnType.GetType().Name)
            {
                case "WScalarFunctionReturnType":
                    {
                        var sftype = ReturnType as WScalarFunctionReturnType;

                        sb.AppendFormat("{0}RETURNS {1}\r\n", indent, TsqlFragmentToString.DataType(sftype.DataType));
                        sb.AppendFormat("{0}AS\r\n", indent);
                        sb.Append(StatementListToString(StatementList, indent));
                        
                        break;
                    }
                case "WSelectFunctionReturnType":
                    {
                        sb.AppendFormat("{0}RETURNS TABLE\r\n", indent);
                        sb.AppendFormat("{0}RETURN (\r\n", indent);
                        sb.AppendFormat(CultureInfo.CurrentCulture, StatementListToString(StatementList, indent));
                        sb.Append("\r\n");
                        sb.AppendFormat("{0});", indent);

                        break;
                    }
                case "WTableValuedFunctionReturnType":
                    {
                        break;
                    }
                default:
                    break;
            }

            return sb.ToString();
        }
    }

    public partial class WCreateFunctionStatement : WFunctionStatement
    {
        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(1024);

            sb.AppendFormat("{0}CREATE FUNCTION {1} (\r\n", indent, 
                Name);

            sb.Append(ParameterListToString(indent + " "));
            sb.Append("\r\n");

            sb.AppendFormat("{0})\r\n", indent);

            sb.Append(FunctionBodyToString(indent));

            return sb.ToString();
        }
    }
}
