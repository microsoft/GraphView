using System;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;
using System.Collections;
using System.IO;
using System.Text;

// Add DocumentDB references
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GraphView
{
    public class GraphViewJsonCommand
    {
        //insert reader into s1 according reader's type
        public static void insert_reader(ref StringBuilder s1, JsonTextReader reader, ref JsonWriter writer)
        {

            switch (reader.TokenType)
            {
                case JsonToken.StartArray:
                    writer.WriteStartArray();
                    break;
                case JsonToken.EndArray:
                    writer.WriteEnd();
                    break;
                case JsonToken.PropertyName:
                    writer.WritePropertyName(reader.Value.ToString());
                    break;
                case JsonToken.String:
                    writer.WriteValue(reader.Value);
                    break;
                case JsonToken.Integer:
                    writer.WriteValue(reader.Value);
                    break;
                case JsonToken.Comment:
                    writer.WriteComment(reader.Value.ToString());
                    break;
                case JsonToken.StartObject:
                    writer.WriteStartObject();
                    break;
                case JsonToken.EndObject:
                    writer.WriteEndObject();
                    break;
                case JsonToken.Null:
                    writer.WriteNull();
                    break;
                case JsonToken.Float:
                    writer.WriteValue(reader.Value);
                    break;
            }
        }
        //insert s2 into s1 's end
        public static void insert_string(ref StringBuilder s1, string s2, ref JsonWriter writer, bool isObject)
        {
            JsonTextReader reader = new JsonTextReader(new StringReader(s2));

            while (reader.Read())
            {
                switch (reader.TokenType)
                {
                    case JsonToken.StartObject:
                        if (isObject) insert_reader(ref s1, reader, ref writer);
                        break;
                    case JsonToken.EndObject:
                        if (isObject) insert_reader(ref s1, reader, ref writer);
                        break;
                    default:
                        insert_reader(ref s1, reader, ref writer);
                        break;
                }
            }
        }
        //insert json_str_s2 into json_str_s1 's  s3
        //and return the ans
        public static StringBuilder insert_array_element(string s1, string s2, string s3)
        {
            bool find = false;
            Stack sta = new Stack();


            StringBuilder sb = new StringBuilder();
            StringWriter sw = new StringWriter(sb);
            JsonWriter writer = new JsonTextWriter(sw);


            JsonTextReader reader1 = new JsonTextReader(new StringReader(s1));
            while (reader1.Read())
            {
                switch (reader1.TokenType)
                {
                    case JsonToken.StartArray:
                        writer.WriteStartArray();
                        if (find)
                            sta.Push(1);
                        break;

                    case JsonToken.EndArray:
                        if (find)
                            sta.Pop();
                        if (find && sta.Count == 0)
                        {
                            insert_string(ref sb, s2, ref writer, false);
                            find = false;
                        }
                        writer.WriteEnd();
                        break;

                    case JsonToken.PropertyName:
                        if (reader1.Value.ToString() == s3)
                            find = true;
                        //Console.WriteLine(reader1.Value.ToString());
                        insert_reader(ref sb, reader1, ref writer);
                        break;


                    default:
                        insert_reader(ref sb, reader1, ref writer);
                        break;
                }
            }

            return sb;
        }
        //use json_str_s2 replace json_str_s1 's property s3 
        //if there is no property s3 , create one
        public static StringBuilder insert_property(string s1, string s2, string s3)
        {
            bool find = false;
            bool flag = false;
            Stack sta = new Stack();


            StringBuilder sb = new StringBuilder();
            StringWriter sw = new StringWriter(sb);
            JsonWriter writer = new JsonTextWriter(sw);


            JsonTextReader reader1 = new JsonTextReader(new StringReader(s1));
            while (reader1.Read())
            {
                switch (reader1.TokenType)
                {
                    case JsonToken.PropertyName:
                        if (reader1.Value.ToString() == s3)
                            find = flag = true;
                        insert_reader(ref sb, reader1, ref writer);
                        if (find)
                        {
                            find = false;
                            insert_string(ref sb, s2, ref writer, false);
                            reader1.Read();
                        }
                        break;
                    case JsonToken.EndObject:
                        sta.Pop();
                        if (!flag && sta.Count == 0)
                        {
                            writer.WritePropertyName(s3);
                            insert_string(ref sb, s2, ref writer, false);
                        }
                        insert_reader(ref sb, reader1, ref writer);
                        break;
                    case JsonToken.StartObject:
                        sta.Push(1);
                        insert_reader(ref sb, reader1, ref writer);
                        break;
                    default:
                        insert_reader(ref sb, reader1, ref writer);
                        break;
                }
            }

            return sb;
        }

        public static int get_reverse_edge_num(string s1)
        {
            bool flag = false;
            int now = 1;


            JsonTextReader reader1 = new JsonTextReader(new StringReader(s1));
            while (reader1.Read())
            {
                switch (reader1.TokenType)
                {
                    case JsonToken.PropertyName:
                        if (reader1.Value.ToString() == "_reverse_edge")
                            flag = true;
                        break;
                    case JsonToken.StartArray:
                        if (flag)
                        {
                            reader1.Read();
                            if (reader1.TokenType == JsonToken.EndArray)
                                return 1;
                            reader1.Read();
                            reader1.Read();
                            if ((long)reader1.Value == now)
                                now++;
                            else
                                return now;
                        }
                        break;
                    case JsonToken.EndArray:
                        if (flag)
                            return now;
                        break;
                    case JsonToken.StartObject:
                        if (flag)
                        {
                            reader1.Read();
                            reader1.Read();
                            if ((long)reader1.Value == now)
                                now++;
                            else
                                return now;
                        }
                        break;
                }
            }
            return 1;
        }
        public static int get_edge_num(string s1)
        {
            bool flag = false;
            int now = 1;


            JsonTextReader reader1 = new JsonTextReader(new StringReader(s1));
            while (reader1.Read())
            {
                switch (reader1.TokenType)
                {
                    case JsonToken.PropertyName:
                        if (reader1.Value.ToString() == "_edge")
                            flag = true;
                        break;
                    case JsonToken.StartArray:
                        if (flag)
                        {
                            reader1.Read();
                            if (reader1.TokenType == JsonToken.EndArray)
                                return 1;
                            reader1.Read();
                            reader1.Read();
                            if ((long)reader1.Value == now)
                                now++;
                            else
                                return now;
                        }
                        break;
                    case JsonToken.EndArray:
                        if (flag)
                            return now;
                        break;
                    case JsonToken.StartObject:
                        if (flag)
                        {
                            reader1.Read();
                            reader1.Read();
                            if ((long)reader1.Value == now)
                                now++;
                            else
                                return now;
                        }
                        break;
                }
            }
            return 1;
        }
        public static StringBuilder insert_reverse_edge(string s1, string s2, int num)
        {
            bool find = false;
            bool Write = false;

            StringBuilder sb = new StringBuilder();
            StringWriter sw = new StringWriter(sb);
            JsonWriter writer = new JsonTextWriter(sw);

            JsonTextReader reader1 = new JsonTextReader(new StringReader(s1));
            while (reader1.Read())
            {
                switch (reader1.TokenType)
                {
                    case JsonToken.PropertyName:
                        if (reader1.Value.ToString() == "_reverse_edge")
                            find = true;
                        insert_reader(ref sb, reader1, ref writer);
                        //if (find)
                        //{
                        //    find = false;
                        //    insert_string(ref sb, s2, ref writer);
                        //    reader1.Read();
                        //}
                        break;
                    case JsonToken.StartArray:
                        insert_reader(ref sb, reader1, ref writer);
                        if (find)
                        {
                            if (num == 1)
                            {
                                insert_string(ref sb, s2, ref writer, true);
                                Write = true;
                            }
                        }
                        break;
                    case JsonToken.StartObject:
                        if (!find || Write)
                            insert_reader(ref sb, reader1, ref writer);
                        else
                        {
                            reader1.Read();
                            reader1.Read();
                            if ((long)reader1.Value > num)
                            {
                                insert_string(ref sb, s2, ref writer, true);
                                Write = true;
                            }
                            writer.WriteStartObject();
                            writer.WritePropertyName("_ID");
                            insert_reader(ref sb, reader1, ref writer);
                        }
                        break;
                    case JsonToken.EndArray:
                        if (find)
                        {
                            find = false;
                            if (!Write)
                            {
                                insert_string(ref sb, s2, ref writer, true);
                                Write = true;
                            }
                        }
                        insert_reader(ref sb, reader1, ref writer);
                        break;
                    default:
                        insert_reader(ref sb, reader1, ref writer);
                        break;
                }
            }

            return sb;
        }
        public static StringBuilder insert_edge(string s1, string s2, int num)
        {
            bool find = false;
            bool Write = false;

            StringBuilder sb = new StringBuilder();
            StringWriter sw = new StringWriter(sb);
            JsonWriter writer = new JsonTextWriter(sw);

            JsonTextReader reader1 = new JsonTextReader(new StringReader(s1));
            while (reader1.Read())
            {
                switch (reader1.TokenType)
                {
                    case JsonToken.PropertyName:
                        if (reader1.Value.ToString() == "_edge")
                            find = true;
                        insert_reader(ref sb, reader1, ref writer);
                        //if (find)
                        //{
                        //    find = false;
                        //    insert_string(ref sb, s2, ref writer);
                        //    reader1.Read();
                        //}
                        break;
                    case JsonToken.StartArray:
                        insert_reader(ref sb, reader1, ref writer);
                        if (find)
                        {
                            if (num == 1)
                            {
                                insert_string(ref sb, s2, ref writer, true);
                                Write = true;
                            }
                        }
                        break;
                    case JsonToken.StartObject:
                        if (!find || Write)
                            insert_reader(ref sb, reader1, ref writer);
                        else
                        {
                            reader1.Read();
                            reader1.Read();
                            if ((long)reader1.Value > num)
                            {
                                insert_string(ref sb, s2, ref writer, true);
                                Write = true;
                            }
                            writer.WriteStartObject();
                            writer.WritePropertyName("_ID");
                            insert_reader(ref sb, reader1, ref writer);
                        }
                        break;
                    case JsonToken.EndArray:
                        if (find)
                        {
                            find = false;
                            if (!Write)
                            {
                                insert_string(ref sb, s2, ref writer, true);
                                Write = true;
                            }
                        }
                        insert_reader(ref sb, reader1, ref writer);
                        break;
                    default:
                        insert_reader(ref sb, reader1, ref writer);
                        break;
                }
            }

            return sb;
        }
        public static string Delete_edge(string s1, int DeleteID)
        {
            bool find = false;
            bool Deleted = false;

            StringBuilder sb = new StringBuilder();
            StringWriter sw = new StringWriter(sb);
            JsonWriter writer = new JsonTextWriter(sw);

            JsonTextReader reader1 = new JsonTextReader(new StringReader(s1));
            while (reader1.Read())
            {
                switch (reader1.TokenType)
                {
                    case JsonToken.PropertyName:
                        if (reader1.Value.ToString() == "_edge")
                            find = true;
                        insert_reader(ref sb, reader1, ref writer);
                        break;
                    case JsonToken.StartArray:
                        insert_reader(ref sb, reader1, ref writer);
                        break;
                    case JsonToken.StartObject:
                        if (!find || Deleted)
                            insert_reader(ref sb, reader1, ref writer);
                        else
                        {
                            reader1.Read();
                            reader1.Read();
                            if ((long)reader1.Value == DeleteID)
                            {
                                while (reader1.TokenType != JsonToken.EndObject)
                                    reader1.Read();
                                Deleted = true;
                            }
                            else
                            {
                                writer.WriteStartObject();
                                writer.WritePropertyName("_ID");
                                insert_reader(ref sb, reader1, ref writer);
                            }
                        }
                        break;
                    case JsonToken.EndArray:
                        if (find)
                            find = false;
                        insert_reader(ref sb, reader1, ref writer);
                        break;
                    default:
                        insert_reader(ref sb, reader1, ref writer);
                        break;
                }
            }

            return sb.ToString();
        }
        public static string Delete_reverse_edge(string s1, int DeleteID)
        {
            bool find = false;
            bool Deleted = false;

            StringBuilder sb = new StringBuilder();
            StringWriter sw = new StringWriter(sb);
            JsonWriter writer = new JsonTextWriter(sw);

            JsonTextReader reader1 = new JsonTextReader(new StringReader(s1));
            while (reader1.Read())
            {
                switch (reader1.TokenType)
                {
                    case JsonToken.PropertyName:
                        if (reader1.Value.ToString() == "_reverse_edge")
                            find = true;
                        insert_reader(ref sb, reader1, ref writer);
                        break;
                    case JsonToken.StartArray:
                        insert_reader(ref sb, reader1, ref writer);
                        break;
                    case JsonToken.StartObject:
                        if (!find || Deleted)
                            insert_reader(ref sb, reader1, ref writer);
                        else
                        {
                            reader1.Read();
                            reader1.Read();
                            if ((long)reader1.Value == DeleteID)
                            {
                                while (reader1.TokenType != JsonToken.EndObject)
                                    reader1.Read();
                                Deleted = true;
                            }
                            else
                            {
                                writer.WriteStartObject();
                                writer.WritePropertyName("_ID");
                                insert_reader(ref sb, reader1, ref writer);
                            }
                        }
                        break;
                    case JsonToken.EndArray:
                        if (find)
                            find = false;
                        insert_reader(ref sb, reader1, ref writer);
                        break;
                    default:
                        insert_reader(ref sb, reader1, ref writer);
                        break;
                }
            }

            return sb.ToString();
        }
    }
}