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
            }
        }
        //insert s2 into s1 's end
        public static void insert_string(ref StringBuilder s1, string s2, ref JsonWriter writer)
        {
            JsonTextReader reader = new JsonTextReader(new StringReader(s2));

            while (reader.Read())
            {
                switch (reader.TokenType)
                {
                    case JsonToken.StartObject:
                        break;
                    case JsonToken.EndObject:
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
                            insert_string(ref sb, s2, ref writer);
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
                        Console.WriteLine(reader1.Value.ToString());
                        insert_reader(ref sb, reader1, ref writer);
                        if (find)
                        {
                            find = false;
                            insert_string(ref sb, s2, ref writer);
                            reader1.Read();
                        }
                        break;
                    case JsonToken.EndObject:
                        if (!flag)
                        {
                            writer.WritePropertyName(s3);
                            insert_string(ref sb, s2, ref writer);
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
    }
}