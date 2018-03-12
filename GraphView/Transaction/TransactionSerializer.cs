namespace GraphView.Transaction
{
    using System.IO;
    using System.Runtime.Serialization;
    using System.Runtime.Serialization.Formatters.Binary;
    class TransactionSerializer
    {
        public static byte[] SerializeToBytes(Transaction tx)
        {
            byte[] bytes;
            IFormatter formatter = new BinaryFormatter();

            using (MemoryStream stream = new MemoryStream())
            {
                formatter.Serialize(stream, tx);
                bytes = stream.ToArray();
            }

            return bytes;
        }

        public static Transaction DeserializeFromBytes(byte[] bytes)
        {
            IFormatter formatter = new BinaryFormatter();
            using (MemoryStream stream = new MemoryStream(bytes))
            {
                object obj = formatter.Deserialize(stream);
                Transaction transaction = obj as Transaction;
                return transaction;
            }
        }
    }
}
