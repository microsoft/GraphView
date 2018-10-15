using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using ServiceStack.Redis;
using GraphView.Transaction;

namespace TransactionBenchmarkTest.TPCC
{
    internal class WorkloadParam
    {
        public TPCCWorkloadOutput Execute(SyncExecution txExec)
        {
            return this.storedProcedure.Run(txExec, this);
        }
        public SyncStoredProcedure storedProcedure;
    }
    class NewOrderInParameters : WorkloadParam
    {
        public string timestamp;
        public uint W_ID;
        public uint D_ID;
        public uint C_ID;
        public uint[] OL_I_IDs;
        public uint[] OL_SUPPLY_W_IDs;
        public uint[] OL_QUANTITYs;
        public string O_ENTRY_D;
    }
    class NewOrderOutput
    {
        public class ItemOutput
        {
            string I_NAME;
            char brand;
            double I_PRICE;
            double OL_AMOUNT;

            public void Set(
                string I_NAME, char brand, double I_PRICE, double OL_AMOUNT)
            {
                this.I_NAME = I_NAME;
                this.brand = brand;
                this.I_PRICE = I_PRICE;
                this.OL_AMOUNT = OL_AMOUNT;
            }
        }
        public uint W_ID;
        public string C_LAST;
        public string C_CREDIT;
        public double C_DISCOUNT;
        public double W_TAX;
        public double D_TAX;
        public uint O_OL_CNT;
        public uint O_ID;
        public string O_ENTRY_D;
        public double totalAmount;

        public void Set(
            uint W_ID, string C_LAST, string C_CREDIT, double C_DISCOUNT,
            double W_TAX, double D_TAX, uint O_OL_CNT, uint O_ID,
            string O_ENTRY_D, double totalAmount)
        {
            this.W_ID = W_ID;
            this.C_LAST = C_LAST;
            this.C_CREDIT = C_CREDIT;
            this.C_DISCOUNT = C_DISCOUNT;
            this.W_TAX = W_TAX;
            this.D_TAX = D_TAX;
            this.O_OL_CNT = O_OL_CNT;
            this.O_ID = O_ID;
            this.O_ENTRY_D = O_ENTRY_D;
            this.totalAmount = totalAmount;
        }

        public TxObjPoolList<ItemOutput> itemOutputs =
            new TxObjPoolList<ItemOutput>(15);

        public Tuple<string, int, char, double, double>[] itemsData;
        public Tuple<double, double, uint, double> other;
        public CustomerPayload cpl;
    }

    class PaymentInParameters : WorkloadParam
    {
        public string timestamp;
        public uint W_ID;
        public uint D_ID;
        public uint C_ID;
        public string C_LAST;
        public uint C_D_ID;
        public uint C_W_ID;
        public string H_DATE;
        public double H_AMOUNT;
    }
    class PaymentOutput
    {
        //public TxFinalStatus txFinalStatus;
        public WarehousePayload wpl;
        public DistrictPayload dpl;
        public CustomerPayload cpl;
    }

    class TPCCWorkloadOutput
    {
        public TxFinalStatus txFinalStatus;
        public object data;
    }

    abstract class SyncStoredProcedure
    {
        private TPCCWorkloadOutput output = new TPCCWorkloadOutput();
        public
        TPCCWorkloadOutput Run(SyncExecution exec, WorkloadParam param)
        {
            this.output.txFinalStatus = TxFinalStatus.UNKNOWN;
            exec.Start();
            this.output.data = this.ExecuteStoredProcedure(exec, param);
            if (!exec.IsAborted())
            {
                this.output.txFinalStatus = TxFinalStatus.COMMITTED;
            }
            else
            {
                this.output.txFinalStatus = TxFinalStatus.ABORTED;
            }
            return this.output;
        }

        protected abstract object ExecuteStoredProcedure(
            SyncExecution exec, WorkloadParam param);
    }

    abstract class WorkloadBuilder
    {
        public abstract string Name();

        protected abstract SyncStoredProcedure MakeStoredProcedure();
        protected abstract WorkloadParam ParseColumns(string[] columns);

        public void NewStoredProcedure()
        {
            this.storedProcedure = this.MakeStoredProcedure();
        }
        public void NewStoredProcedureIfNon()
        {
            if (this.storedProcedure == null)
                NewStoredProcedure();
        }
        public void ResetStoredProcedure()
        {
            this.storedProcedure = null;
        }

        public WorkloadParam BuildWorkload(string[] columns)
        {
            WorkloadParam workload = this.ParseColumns(columns);
            if (this.storedProcedure == null)
            {
                this.NewStoredProcedure();
            }
            workload.storedProcedure = this.storedProcedure;
            return workload;
        }

        private SyncStoredProcedure storedProcedure;
    }

    class NewOrderWorkloadBuilder : WorkloadBuilder
    {
        protected override SyncStoredProcedure MakeStoredProcedure()
        {
            return new NewOrderSyncSP();
        }

        protected override WorkloadParam ParseColumns(string[] columns)
        {
            return new NewOrderInParameters
            {
                timestamp = columns[0],
                W_ID = Convert.ToUInt32(columns[5]),
                D_ID = Convert.ToUInt32(columns[3]),
                C_ID = Convert.ToUInt32(columns[1]),
                OL_I_IDs = JsonConvert.DeserializeObject<uint[]>(columns[6]),
                OL_SUPPLY_W_IDs = JsonConvert.DeserializeObject<uint[]>(columns[4]),
                OL_QUANTITYs = JsonConvert.DeserializeObject<uint[]>(columns[2]),
                O_ENTRY_D = columns[7]
            };
        }

        public override string Name()
        {
            return "NEW_ORDER";
        }
    }
    class PaymentWorkloadBuilder : WorkloadBuilder
    {
        protected override SyncStoredProcedure MakeStoredProcedure()
        {
            return new PaymentSyncSP();
        }

        protected override WorkloadParam ParseColumns(string[] columns)
        {
            return new PaymentInParameters
            {
                timestamp = columns[0],
                C_ID = (columns[1] == "" ? 0 : Convert.ToUInt32(columns[1])),
                C_LAST = columns[2],    // may be ""
                H_DATE = columns[3],
                C_D_ID = Convert.ToUInt32(columns[4]),
                D_ID = Convert.ToUInt32(columns[5]),
                W_ID = Convert.ToUInt32(columns[6]),
                C_W_ID = Convert.ToUInt32(columns[7]),
                H_AMOUNT = Convert.ToDouble(columns[8])
            };
        }

        public override string Name()
        {
            return "PAYMENT";
        }
    }



    class NewOrderSyncSP : SyncStoredProcedure
    {
        private const int MAX_ITEM_NUM = 15;

        private WarehousePkey wpk = new WarehousePkey();
        private DistrictPkey dpk = new DistrictPkey();
        private CustomerPkey cpk = new CustomerPkey();
        private TxObjPoolList<ItemPkey> ipks =
            new TxObjPoolList<ItemPkey>(MAX_ITEM_NUM);
        private ItemPayload[] items =
            new ItemPayload[NewOrderSyncSP.MAX_ITEM_NUM];
        private TxObjPoolList<StockPkey> spks =
            new TxObjPoolList<StockPkey>(MAX_ITEM_NUM);

        private NewOrderOutput noOutput = new NewOrderOutput();

        protected override
        object ExecuteStoredProcedure(SyncExecution exec, WorkloadParam param)
        {
            NewOrderInParameters input = (NewOrderInParameters)param;

            // Transaction tx = new Transaction(null, vdb);

            // check input
            if (input.OL_I_IDs.Length <= 0
                || input.OL_I_IDs.Length != input.OL_QUANTITYs.Length
                || input.OL_I_IDs.Length != input.OL_SUPPLY_W_IDs.Length)
            {
                throw new Exception("invalid input");
            }

            // read Warehouse,District, Customer
            this.wpk.Set(W_ID: input.W_ID);
            // WarehousePayload wpl = JsonConvert.DeserializeObject<WarehousePayload>((string)tx.Read(Constants.DefaultTbl, wpk.ToString()));
            WarehousePayload wpl;
            if (exec.ReadCopy(this.wpk, out wpl).IsAborted())
            {
                return null;
            }
            double W_TAX = wpl.W_TAX;

            this.dpk.Set(D_ID: input.D_ID, D_W_ID: input.W_ID);
            // DistrictPayload dpl = JsonConvert.DeserializeObject<DistrictPayload>((string)tx.Read(Constants.DefaultTbl, dpk.ToString()));
            DistrictPayload dpl;
            if (exec.Read(this.dpk, out dpl).IsAborted())
            {
                return null;
            }
            double D_TAX = dpl.D_TAX;
            uint D_NEXT_O_ID = dpl.D_NEXT_O_ID;

            dpl.D_NEXT_O_ID = D_NEXT_O_ID + 1;
            // tx.Update(Constants.DefaultTbl, dpk.ToString(), JsonConvert.SerializeObject(dpl));
            if (exec.Update(dpk, dpl).IsAborted())
            {
                return null;
            }

            this.cpk.Set(C_ID: input.C_ID, C_D_ID: input.D_ID, C_W_ID: input.W_ID);
            // CustomerPayload cpl = JsonConvert.DeserializeObject<CustomerPayload>((string)tx.Read(Constants.DefaultTbl, cpk.ToString()));
            CustomerPayload cpl;
            if (exec.Read(this.cpk, out cpl).IsAborted())
            {
                return null;
            }
            double C_DISCOUNT = cpl.C_DISCOUNT;

            // all local or not
            bool allLocal = true;
            for (int i = 0; i < input.OL_I_IDs.Length; i++)
            {
                allLocal = allLocal & input.OL_I_IDs[i] == input.W_ID;
            }

            /*
            // insert order/new-order, update next-order-id
            OrderPkey opk = new OrderPkey
            {
                O_ID = D_NEXT_O_ID,
                O_D_ID = input.D_ID,
                O_W_ID = input.W_ID
            };
            OrderPayload opl = new OrderPayload
            {
                O_C_ID = input.C_ID,
                O_ENTRY_D = input.O_ENTRY_D,
                O_CARRIER_ID = Constants.NullCarrierID,
                O_OL_CNT = (uint)input.OL_I_IDs.Length,
                O_ALL_LOCAL = Convert.ToUInt32(allLocal)
            };
            // tx.ReadAndInitialize(Constants.DefaultTbl, opk.ToString());
            // tx.Insert(Constants.DefaultTbl, opk.ToString(), JsonConvert.SerializeObject(opl));
            if (false && exec.Insert(opk, opl).IsAborted())
            {
                return null;
            }
            */
            /*
            NewOrderPkey nopk = new NewOrderPkey
            {
                NO_O_ID = D_NEXT_O_ID,
                NO_D_ID = input.D_ID,
                NO_W_ID = input.W_ID
            };
            // tx.ReadAndInitialize(Constants.DefaultTbl, nopk.ToString());
            // tx.Insert(Constants.DefaultTbl, nopk.ToString(), Constants.PlaceHolder);
            if (false && exec.Insert(nopk, NewOrderPayload.Placeholder()).IsAborted())
            {
                return null;
            }
            */

            // whether all items exist without a wrong item number
            ipks.Clear();
            for (int i = 0; i < input.OL_I_IDs.Length; i++)
            {
                ItemPkey ipk = ipks.AllocateNew();
                ipk.Set(I_ID: input.OL_I_IDs[i]);
                // var str = (string)tx.Read(Constants.DefaultTbl, ipk.ToString());
                // items[i] = JsonConvert.DeserializeObject<ItemPayload>(str);
                TpccTablePayload payload;
                if (exec.Read(ipk, out payload).IsAborted())
                {
                    return null;
                }
                this.items[i] = payload as ItemPayload;
            }

            // insert order lines
            this.spks.Clear();
            this.noOutput.itemOutputs.Clear();
            var total = 0.0;
            for (int i = 0; i < input.OL_I_IDs.Length; i++)
            {
                var OL_NUMBER = (uint)i + 1;
                var OL_SUPPLY_W_ID = input.OL_SUPPLY_W_IDs[i];
                var OL_I_ID = input.OL_I_IDs[i];
                var OL_QUANTITY = input.OL_QUANTITYs[i];

                var I_NAME = this.items[i].I_NAME;
                var I_DATA = this.items[i].I_DATA;
                var I_PRICE = this.items[i].I_PRICE;

                // read & update stock info
                StockPkey spk = this.spks.AllocateNew();
                spk.Set(S_I_ID: OL_I_ID, S_W_ID: OL_SUPPLY_W_ID);
                // StockPayload spl = JsonConvert.DeserializeObject<StockPayload>((string)tx.Read(Constants.DefaultTbl, spk.ToString()));
                StockPayload spl;
                if (exec.ReadCopy(spk, out spl).IsAborted())
                {
                    return null;
                }
                spl.S_YTD += OL_QUANTITY;
                if (spl.S_QUANTITY >= OL_QUANTITY + 10)
                    spl.S_QUANTITY -= (int)OL_QUANTITY;
                else
                    spl.S_QUANTITY += 91 - (int)OL_QUANTITY;
                spl.S_ORDER_CNT += 1;
                if (input.OL_SUPPLY_W_IDs[i] != input.W_ID) spl.S_REMOTE_CNT += 1;
                // tx.Update(Constants.DefaultTbl, spk.ToString(), JsonConvert.SerializeObject(spl));
                if (exec.Update(spk, spl).IsAborted())
                {
                    return null;
                }

                var OL_AMOUNT = OL_QUANTITY * I_PRICE;
                total += OL_AMOUNT;

                /*
                // insert order line
                OrderLinePkey olpk = new OrderLinePkey
                {
                    OL_O_ID = D_NEXT_O_ID,
                    OL_D_ID = input.D_ID,
                    OL_W_ID = input.W_ID,
                    OL_NUMBER = OL_NUMBER
                };

                OrderLinePayload olpl = new OrderLinePayload
                {
                    OL_I_ID = OL_I_ID,
                    OL_SUPPLY_W_ID = OL_SUPPLY_W_ID,
                    OL_DELIVERY_D = null,
                    OL_QUANTITY = OL_QUANTITY,
                    OL_AMOUNT = OL_AMOUNT,
                    OL_DIST_INFO = spl.S_DIST_01        // TODO, assign to S_DIST_XX, where XX equals to D_ID
                };
                // tx.ReadAndInitialize(Constants.DefaultTbl, olpk.ToString());
                // tx.Insert(Constants.DefaultTbl, olpk.ToString(), JsonConvert.SerializeObject(olpl));
                if (false && exec.Insert(olpk, olpl).IsAborted())
                {
                    return null;
                }

                // add to return
                var brand = (I_DATA.Contains("ORIGINAL") && spl.S_DATA.Contains("ORIGINAL")) ? 'B' : 'G';
                this.noOutput.itemOutputs.AllocateNew().Set(
                    this.items[i].I_NAME, brand,
                    this.items[i].I_PRICE, olpl.OL_AMOUNT);
                */
            }

            // tx.Commit();
            if (exec.Commit().IsAborted())
            {
                return null;
            }

            // to return
            total *= (1 - C_DISCOUNT) * (1 + W_TAX + D_TAX);
            this.noOutput.Set(
                wpk.W_ID, cpl.C_LAST, cpl.C_CREDIT, cpl.C_DISCOUNT,
                wpl.W_TAX, dpl.D_TAX, (uint)input.OL_I_IDs.Length, D_NEXT_O_ID,
                input.O_ENTRY_D, total);

            return this.noOutput;
        }
    }


    class PaymentSyncSP : SyncStoredProcedure
    {
        private WarehousePkey wpk = new WarehousePkey();
        private DistrictPkey dpk = new DistrictPkey();
        private CustomerPkey cpk = new CustomerPkey();
        private CustomerLastNameIndexKey lastNameKey =
            new CustomerLastNameIndexKey();

        private PaymentOutput pmOutput = new PaymentOutput();

        protected override
        object ExecuteStoredProcedure(SyncExecution exec, WorkloadParam param)
        {
            PaymentInParameters input = (PaymentInParameters)param;
            // Transaction tx = new Transaction(null, vdb);

            // warehouse, district
            this.wpk.Set(W_ID: input.W_ID);
            // var wpl = JsonConvert.DeserializeObject<WarehousePayload>((string)tx.Read(Constants.DefaultTbl, wpk.ToString()));
            WarehousePayload wpl;
            if (exec.ReadCopy(this.wpk, out wpl).IsAborted())
            {
                return null;
            }
            wpl.W_YTD += input.H_AMOUNT;
            // tx.Update(Constants.DefaultTbl, wpk.ToString(), JsonConvert.SerializeObject(wpl));
            if (exec.Update(this.wpk, wpl).IsAborted())
            {
                return null;
            }

            this.dpk.Set(D_ID: input.D_ID, D_W_ID: input.W_ID);
            // var dpl = JsonConvert.DeserializeObject<DistrictPayload>((string)tx.Read(Constants.DefaultTbl, dpk.ToString()));
            DistrictPayload dpl;
            if (exec.ReadCopy(this.dpk, out dpl).IsAborted())
            {
                return null;
            }
            dpl.D_YTD += input.H_AMOUNT;
            // tx.Update(Constants.DefaultTbl, dpk.ToString(), JsonConvert.SerializeObject(dpl));
            if (exec.Update(this.dpk, dpl).IsAborted())
            {
                return null;
            }

            // determine c_id
            var C_ID = input.C_ID;
            // if (C_ID == 0)  // by c_last
            // {
            //     var k = CustomerPayload.GetLastNameIndexKey(input.C_W_ID, input.C_D_ID, input.C_LAST);
            //     var ids = redisClient.GetAllItemsFromList(k);
            //     C_ID = Convert.ToUInt32(ids[ids.Count / 2]);    // TODO order by c_first?
            // }
            if (C_ID == 0)
            {
                // var lastNameKey = new LastNameIndexKey(
                //     input.C_W_ID, input.C_D_ID, input.C_LAST);
                this.lastNameKey.Set(
                    C_W_ID: input.C_W_ID,
                    C_D_ID: input.C_D_ID,
                    C_LAST: input.C_LAST);
                CustomerLastNamePayloads lastNamePayload;
                if (exec.Read(this.lastNameKey, out lastNamePayload).IsAborted())
                {
                    return null;
                }
                C_ID = lastNamePayload.GetRequiredId();
            }

            this.cpk.Set(
                C_ID: C_ID,
                C_D_ID: input.C_D_ID,
                C_W_ID: input.C_W_ID);
            // var cpl = JsonConvert.DeserializeObject<CustomerPayload>((string)tx.Read(Constants.DefaultTbl, cpk.ToString()));
            CustomerPayload cpl;
            if (exec.ReadCopy(this.cpk, out cpl).IsAborted())
            {
                return null;
            }
            cpl.C_BALANCE -= input.H_AMOUNT;
            cpl.C_YTD_PAYMENT += input.H_AMOUNT;
            cpl.C_PAYMENT_CNT += 1;
            //var C_DATA = cpl.C_DATA;

            // credit info
            if (cpl.C_CREDIT == Constants.BadCredit)
            {
                uint[] tmp = { C_ID, input.C_D_ID, input.C_W_ID, input.D_ID, input.W_ID };
                var newData = string.Join(" ", tmp) + " " + input.H_AMOUNT + "|" + cpl.C_DATA;
                if (newData.Length > Constants.Max_C_DATA)
                {
                    newData = newData.Substring(0, Constants.Max_C_DATA);
                }
                cpl.C_DATA = newData;
            }
            // tx.Update(Constants.DefaultTbl, cpk.ToString(), JsonConvert.SerializeObject(cpl));
            if (exec.Update(this.cpk, cpl).IsAborted())
            {
                return null;
            }

            /*
            // history
            var hpl = new HistoryPayload
            {
                H_C_ID = C_ID,
                H_C_D_ID = this.cpk.C_D_ID,
                H_C_W_ID = this.cpk.C_W_ID,
                H_D_ID = input.D_ID,
                H_W_ID = input.W_ID,
                H_DATA = wpl.W_NAME + "    " + dpl.D_NAME,
                H_AMOUNT = input.H_AMOUNT,
                H_DATE = input.timestamp
            };
            var hpk = HistoryPKey.New();
            // tx.ReadAndInitialize(Constants.DefaultTbl, hpk);
            // tx.Insert(Constants.DefaultTbl, hpk, JsonConvert.SerializeObject(hpl));
            if (false && exec.Insert(hpk, hpl).IsAborted())
            {
                return null;
            }
            */
            // tx.Commit();
            if (exec.Commit().IsAborted())
            {
                return null;
            }

            // to return
            this.pmOutput.wpl = wpl;
            this.pmOutput.dpl = dpl;
            this.pmOutput.cpl = cpl;  // TODO C_ID may be null in input

            return this.pmOutput;
        }
    }
}
