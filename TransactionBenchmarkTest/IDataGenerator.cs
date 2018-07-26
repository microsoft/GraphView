namespace TransactionBenchmarkTest
{
    public interface IDataGenerator
    {
        int NextIntKey();

        string NextStringKey();

        string NextOperation();

    }
}
