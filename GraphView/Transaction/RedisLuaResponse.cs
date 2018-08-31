
using System.Diagnostics; // Debug.Assert
using System.Text;
using System;
using System.Linq;

namespace GraphView.Transaction
{
    /// <summary>
    /// Result returned by `ServiceStack.Redis` of lua-script call
    /// ServiceStack does NOT allow lua to return non-string, non-array value (like interger type)
    /// 
    /// Strings(bytes) is returned as a byte[][] with length = 2, where [0] = null, [1] = value
    /// 
    /// Otherwise, byte[][] should be interpreted as array of string (lua table),
    /// all numbers returned in the array is transformed to string ({123} => {"123"})
    /// 
    /// Last, we use {""} to represent a 'success' call, {} to represent a 'fail' call
    /// </summary>
    public static class RedisLuaResponse
    {
        static public bool IsArray(this byte[][] data)
        {
            return !data.IsSingleValue();
        }

        static public bool IsSingleValue(this byte[][] data)
        {
            return data.Length == 2 && data[0] == null;
        }

        static private String BytesToString(byte[] bs)
        {
            return Encoding.UTF8.GetString(bs);
        }

        static public byte[] ValueBytes(this byte[][] data)
        {
            Debug.Assert(data.IsSingleValue());
            return data[1];
        }

        static public byte[][] ByteArray(this byte[][] data)
        {
            Debug.Assert(data.IsArray());
            return data;
        }

        static public bool IsSuccess(this byte[][] data)
        {
            return data.Length == 1 && data[0].Length == 0;
        }

        static public bool IsFailure(this byte[][] data)
        {
            return data.Length == 0;
        }
    }
}