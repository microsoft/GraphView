namespace TransactionBenchmarkTest
{
    using System;

    internal class TxTask
    {
        private Func<object, object> runMethod;

        private object param;

        public TxTask(Func<object, object> run, object param)
        {
            this.runMethod = run;
            this.param = param;
        }

        public object Run()
        {
            return this.runMethod(this.param);
        }
    }
}
