namespace GraphView.Transaction
{
    using System;
    using System.IO;
    using System.Runtime.Serialization;
    using System.Runtime.Serialization.Formatters.Binary;

    internal class BytesSerializer
    {
        public static byte[] Serialize(Object obj)
        {
            byte[] bytes;
            IFormatter formatter = new BinaryFormatter();

            using (MemoryStream stream = new MemoryStream())
            {
                formatter.Serialize(stream, obj);
                bytes = stream.ToArray();
            }

            return bytes;
        }

        public static Object Deserialize(byte[] bytes)
        {
            IFormatter formatter = new BinaryFormatter();
            using (MemoryStream stream = new MemoryStream(bytes))
            {
                return formatter.Deserialize(stream);
            }
        }
    }
}
