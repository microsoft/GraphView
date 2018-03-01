namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Runtime.Serialization.Formatters.Binary;
    using System.Text;
    using System.Threading.Tasks;

    internal static class VersionEntrySerializer
    {
        public static byte[] SerializeToBytes(VersionEntry versionEntry)
        {
            byte[] bytes;
            IFormatter formatter = new BinaryFormatter();

            using (MemoryStream stream = new MemoryStream())
            {
                formatter.Serialize(stream, versionEntry);
                bytes = stream.ToArray();
            }

            return bytes;
        }

        public static VersionEntry DeserializeFromBytes(byte[] bytes)
        {
            IFormatter formatter = new BinaryFormatter();
            using (MemoryStream stream = new MemoryStream(bytes))
            {

                object obj = formatter.Deserialize(stream);
                VersionEntry versionEntry = obj as VersionEntry;
                return versionEntry;
            }
        }
    }
}
