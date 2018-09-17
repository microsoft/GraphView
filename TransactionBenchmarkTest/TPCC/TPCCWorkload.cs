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
    public class WorkloadParam { }
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
        //public TxFinalStatus txFinalStatus;
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

    abstract class TPCCWorkload
    {
        public abstract
        TPCCWorkloadOutput Run(TransactionExecution exec, WorkloadParam param);
    }

    abstract class WorkloadFactory
    {
        public abstract TPCCWorkload NewWorkload();
        public abstract WorkloadParam ColumnsToParam(string[] columns);
    }

    class NewOrderWorkloadFactory : WorkloadFactory
    {
        public override TPCCWorkload NewWorkload()
        {
            return new TPCCNewOrderWorkload();
        }

        public override WorkloadParam ColumnsToParam(string[] columns)
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
    }
    class PaymentWorkloadFactory : WorkloadFactory
    {
        public override TPCCWorkload NewWorkload()
        {
            return new TPCCPaymentWorkload();
        }

        public override WorkloadParam ColumnsToParam(string[] columns)
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
    }



    class TPCCNewOrderWorkload : TPCCWorkload
    {
        public override TPCCWorkloadOutput Run(TransactionExecution exec, WorkloadParam param)
        {
            NewOrderInParameters input = (NewOrderInParameters)param;
            TPCCWorkloadOutput ret = new TPCCWorkloadOutput();
            ret.txFinalStatus = TxFinalStatus.UNKNOWN;
            exec.Reset();
            // Transaction tx = new Transaction(null, vdb);
            try
            {
                // check input
                if (input.OL_I_IDs.Length <= 0
                    || input.OL_I_IDs.Length != input.OL_QUANTITYs.Length
                    || input.OL_I_IDs.Length != input.OL_SUPPLY_W_IDs.Length)
                {
                    throw new Exception("invalid input");
                }

                // all local or not
                bool allLocal = true;
                for (int i = 0; i < input.OL_I_IDs.Length; i++)
                {
                    allLocal = allLocal & input.OL_I_IDs[i] == input.W_ID;
                }

                // whether all items exist without a wrong item number
                ItemPayload[] items = new ItemPayload[input.OL_I_IDs.Length];
                for (int i = 0; i < input.OL_I_IDs.Length; i++)
                {
                    ItemPkey ipk = new ItemPkey { I_ID = input.OL_I_IDs[i] };
                    // var str = (string)tx.Read(Constants.DefaultTbl, ipk.ToString());
                    var ipl = exec.TpccSyncRead(ipk) as ItemPayload;
                    // items[i] = JsonConvert.DeserializeObject<ItemPayload>(str);
                    items[i] = ipl;
                }

                // read Warehouse,District, Customer
                WarehousePkey wpk = new WarehousePkey { W_ID = input.W_ID };
                // WarehousePayload wpl = JsonConvert.DeserializeObject<WarehousePayload>((string)tx.Read(Constants.DefaultTbl, wpk.ToString()));
                WarehousePayload wpl = exec.TpccSyncRead(wpk) as WarehousePayload;
                double W_TAX = wpl.W_TAX;

                DistrictPkey dpk = new DistrictPkey { D_ID = input.D_ID, D_W_ID = input.W_ID };
                // DistrictPayload dpl = JsonConvert.DeserializeObject<DistrictPayload>((string)tx.Read(Constants.DefaultTbl, dpk.ToString()));
                DistrictPayload dpl = exec.TpccSyncRead(dpk) as DistrictPayload;
                double D_TAX = dpl.D_TAX;
                uint D_NEXT_O_ID = dpl.D_NEXT_O_ID;

                CustomerPkey cpk = new CustomerPkey { C_ID = input.C_ID, C_D_ID = input.D_ID, C_W_ID = input.W_ID };
                // CustomerPayload cpl = JsonConvert.DeserializeObject<CustomerPayload>((string)tx.Read(Constants.DefaultTbl, cpk.ToString()));
                CustomerPayload cpl = exec.TpccSyncRead(cpk) as CustomerPayload;
                double C_DISCOUNT = cpl.C_DISCOUNT;

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
                exec.TpccSyncInsert(opk, opl);

                dpl.D_NEXT_O_ID = D_NEXT_O_ID + 1;
                // tx.Update(Constants.DefaultTbl, dpk.ToString(), JsonConvert.SerializeObject(dpl));
                exec.TpccSyncUpdate(dpk, dpl);

                NewOrderPkey nopk = new NewOrderPkey
                {
                    NO_O_ID = D_NEXT_O_ID,
                    NO_D_ID = input.D_ID,
                    NO_W_ID = input.W_ID
                };
                // tx.ReadAndInitialize(Constants.DefaultTbl, nopk.ToString());
                // tx.Insert(Constants.DefaultTbl, nopk.ToString(), Constants.PlaceHolder);
                exec.TpccSyncInsert(nopk, NewOrderPayload.Placeholder());

                // insert order lines
                Tuple<string, int, char, double, double>[] itemsData = new Tuple<string, int, char, double, double>[input.OL_I_IDs.Length];
                var total = 0.0;
                for (int i = 0; i < input.OL_I_IDs.Length; i++)
                {
                    var OL_NUMBER = (uint)i + 1;
                    var OL_SUPPLY_W_ID = input.OL_SUPPLY_W_IDs[i];
                    var OL_I_ID = input.OL_I_IDs[i];
                    var OL_QUANTITY = input.OL_QUANTITYs[i];

                    var I_NAME = items[i].I_NAME;
                    var I_DATA = items[i].I_DATA;
                    var I_PRICE = items[i].I_PRICE;

                    // read & update stock info
                    var spk = new StockPkey { S_I_ID = OL_I_ID, S_W_ID = OL_SUPPLY_W_ID };
                    // StockPayload spl = JsonConvert.DeserializeObject<StockPayload>((string)tx.Read(Constants.DefaultTbl, spk.ToString()));
                    StockPayload spl = exec.TpccSyncRead(spk) as StockPayload;
                    spl.S_YTD += OL_QUANTITY;
                    if (spl.S_QUANTITY >= OL_QUANTITY + 10)
                        spl.S_QUANTITY -= (int)OL_QUANTITY;
                    else
                        spl.S_QUANTITY += 91 - (int)OL_QUANTITY;
                    spl.S_ORDER_CNT += 1;
                    if (input.OL_SUPPLY_W_IDs[i] != input.W_ID) spl.S_REMOTE_CNT += 1;
                    // tx.Update(Constants.DefaultTbl, spk.ToString(), JsonConvert.SerializeObject(spl));
                    exec.TpccSyncUpdate(spk, spl);

                    var OL_AMOUNT = OL_QUANTITY * I_PRICE;
                    total += OL_AMOUNT;

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
                    exec.TpccSyncInsert(olpk, olpl);

                    // add to return
                    var brand = (I_DATA.Contains("ORIGINAL") && spl.S_DATA.Contains("ORIGINAL")) ? 'B' : 'G';
                    itemsData[i] = new Tuple<string, int, char, double, double>(I_NAME, spl.S_QUANTITY, brand, I_PRICE, OL_AMOUNT);
                }

                // to return
                total *= (1 - C_DISCOUNT) * (1 + W_TAX + D_TAX);
                NewOrderOutput noOutput = new NewOrderOutput();
                noOutput.other = new Tuple<double, double, uint, double>(W_TAX, D_TAX, D_NEXT_O_ID, total);
                noOutput.itemsData = itemsData;
                noOutput.cpl = cpl;
                ret.data = noOutput;

                // tx.Commit();
                exec.Commit();
                ret.txFinalStatus = TxFinalStatus.COMMITTED;
            }
            catch (AbortException e)
            {
                // tx.Abort();     // TODO is it right? if e is a TransactionException ?
                ret.txFinalStatus = TxFinalStatus.ABORTED;
            }

            return ret;
        }
    }


    class TPCCPaymentWorkload : TPCCWorkload
    {
        public override TPCCWorkloadOutput Run(TransactionExecution exec, WorkloadParam param)
        {
            PaymentInParameters input = (PaymentInParameters)param;
            TPCCWorkloadOutput ret = new TPCCWorkloadOutput();
            ret.txFinalStatus = TxFinalStatus.UNKNOWN;
            // Transaction tx = new Transaction(null, vdb);
            exec.Reset();
            try
            {
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
                    var lastNameKey = new CustomerLastNameIndexKey
                    {
                        C_W_ID = input.C_W_ID,
                        C_D_ID = input.C_D_ID,
                        C_LAST = input.C_LAST
                    };
                    var lastNamePayload = exec.TpccSyncRead(lastNameKey)
                        as CustomerLastNamePayloads;
                    C_ID = lastNamePayload.GetRequiredId();
                }

                var cpk = new CustomerPkey
                {
                    C_ID = C_ID,
                    C_D_ID = input.C_D_ID,
                    C_W_ID = input.C_W_ID
                };
                // var cpl = JsonConvert.DeserializeObject<CustomerPayload>((string)tx.Read(Constants.DefaultTbl, cpk.ToString()));
                var cpl = exec.TpccSyncRead(cpk) as CustomerPayload;
                cpl.C_BALANCE -= input.H_AMOUNT;
                cpl.C_YTD_PAYMENT += input.H_AMOUNT;
                cpl.C_PAYMENT_CNT += 1;
                //var C_DATA = cpl.C_DATA;

                // warehouse, district
                var wpk = new WarehousePkey { W_ID = input.W_ID };
                // var wpl = JsonConvert.DeserializeObject<WarehousePayload>((string)tx.Read(Constants.DefaultTbl, wpk.ToString()));
                var wpl = exec.TpccSyncRead(wpk) as WarehousePayload;
                wpl.W_YTD += input.H_AMOUNT;
                // tx.Update(Constants.DefaultTbl, wpk.ToString(), JsonConvert.SerializeObject(wpl));
                exec.TpccSyncUpdate(wpk, wpl);

                var dpk = new DistrictPkey { D_ID = input.D_ID, D_W_ID = input.W_ID };
                // var dpl = JsonConvert.DeserializeObject<DistrictPayload>((string)tx.Read(Constants.DefaultTbl, dpk.ToString()));
                var dpl = exec.TpccSyncRead(dpk) as DistrictPayload;
                dpl.D_YTD += input.H_AMOUNT;
                // tx.Update(Constants.DefaultTbl, dpk.ToString(), JsonConvert.SerializeObject(dpl));
                exec.TpccSyncUpdate(dpk, dpl);

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
                exec.TpccSyncUpdate(cpk, cpl);

                // history
                var hpl = new HistoryPayload
                {
                    H_C_ID = C_ID,
                    H_C_D_ID = cpk.C_D_ID,
                    H_C_W_ID = cpk.C_W_ID,
                    H_D_ID = input.D_ID,
                    H_W_ID = input.W_ID,
                    H_DATA = wpl.W_NAME + "    " + dpl.D_NAME,
                    H_AMOUNT = input.H_AMOUNT,
                    H_DATE = input.timestamp
                };
                var hpk = HistoryPKey.New();
                // tx.ReadAndInitialize(Constants.DefaultTbl, hpk);
                // tx.Insert(Constants.DefaultTbl, hpk, JsonConvert.SerializeObject(hpl));
                exec.TpccSyncInsert(hpk, hpl);

                // to return
                PaymentOutput pmOutput = new PaymentOutput();
                pmOutput.wpl = wpl;
                pmOutput.dpl = dpl;
                pmOutput.cpl = cpl;  // TODO C_ID may be null in input
                ret.data = pmOutput;

                // tx.Commit();
                exec.Commit();
                ret.txFinalStatus = TxFinalStatus.COMMITTED;
            }
            catch (AbortException)
            {
                // tx.Abort();
                ret.txFinalStatus = TxFinalStatus.ABORTED;
            }

            return ret;
        }
    }
}
