using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView.Transaction;
using Newtonsoft.Json;
using ServiceStack.Redis;

namespace TransactionBenchmarkTest.TPCC
{
    class TPCCStateTracer
    {
        public static NewOrderState[] nostates;  // = new NewOrderState[10];
        public static PaymentState[] pmstates;
    }

    // the state here means the loop is blocking in the state.
    public enum NewOrderState
    {
        ToStart,    // init state
        ReadItems,  // loop
        ReadW,
        ReadD,
        ReadC,
        ReadInitO,
        InsertO,
        UpdateD,
        ReadInitNO,
        InsertNO,
        InsertOLsReadS, // loop
        InsertOLsUpdateS,
        InsertOLsReadInitOL,
        InsertOLsInsertOL,
    };

    // NewOrder
    class TPCCNewOrderStoredProcedure : StoredProcedure
    {
        private string sessionId;
        private NewOrderInParameters input;

        private int olCount;                        // order line count
        private ItemPayload[] items;                // order line items
        private double W_TAX;
        private DistrictPayload dpl;
        private string dpkStr;
        private uint D_NEXT_O_ID;
        private CustomerPayload cpl;
        private double C_DISCOUNT;
        private string opkStr;
        private string nopkStr;
        private string spkStr;
        private StockPayload spl;
        private string olpkStr;

        private double totalFee;
        private Tuple<string, int, char, double, double>[] itemsData;


        private int i;                              // loop i
        private NewOrderState currentState;         // current state

        public TPCCWorkloadOutput output;
        public string errMsg;

        public TPCCNewOrderStoredProcedure(string sessionId, NewOrderInParameters inParams)
        {
            this.pid = int.Parse(sessionId);
            this.sessionId = sessionId;
            this.input = inParams;
            this.currentState = NewOrderState.ToStart;
            this.errMsg = "";
        }

        private void ReadItem()
        {
            ItemPkey ipk = new ItemPkey { I_ID = input.OL_I_IDs[i] };
            this.AddReq(ipk.ToString());
        }

        private void AddReq(string key, string value = null, OperationType otype = OperationType.Read)
        {
            TransactionRequest req = new TransactionRequest(this.sessionId,
                    Constants.DefaultTbl, key, value, otype);
            this.RequestQueue.Enqueue(req);
        }

        public override void Start()
        {
            // init 
            this.output = new TPCCWorkloadOutput();
            output.txFinalStatus = TxFinalStatus.UNKNOWN;
            this.olCount = this.input.OL_I_IDs.Length;
            this.items = new ItemPayload[olCount];
            this.totalFee = 0.0;
            this.itemsData = new Tuple<string, int, char, double, double>[olCount];
            this.i = 0;

            // the first step
            this.currentState = NewOrderState.ReadItems;
            ReadItem();
        }

        public void StoreCurrentState()
        {
            //TPCCStateTracer.nostates[pid] = this.currentState;
        }

        public override void ReadCallback(string tableId, object recordKey, object payload)
        {
            switch (this.currentState)
            {
                case NewOrderState.ReadItems:
                    if (payload == null)
                    {
                        this.Close("read item failed");
                        break;
                    }
                    items[i++] = JsonConvert.DeserializeObject<ItemPayload>(payload as string);                    
                    if (this.i < this.olCount) ReadItem();
                    else
                    {
                        this.currentState = NewOrderState.ReadW;
                        WarehousePkey wpk = new WarehousePkey { W_ID = input.W_ID };
                        this.AddReq(wpk.ToString());
                    }
                    break;
                case NewOrderState.ReadW:
                    if (payload == null)
                    {
                        this.Close("read W fail");
                        break;
                    }
                    WarehousePayload wpl = JsonConvert.DeserializeObject<WarehousePayload>(payload as string);
                    this.W_TAX = wpl.W_TAX;
                    this.currentState = NewOrderState.ReadD;
                    DistrictPkey dpk = new DistrictPkey { D_ID = input.D_ID, D_W_ID = input.W_ID };
                    this.dpkStr = dpk.ToString();
                    this.AddReq(this.dpkStr);
                    break;
                case NewOrderState.ReadD:
                    if (payload == null)
                    {
                        this.Close("read D fail");
                        break;
                    }
                    this.dpl = JsonConvert.DeserializeObject<DistrictPayload>(payload as string);
                    this.D_NEXT_O_ID = dpl.D_NEXT_O_ID;
                    this.currentState = NewOrderState.ReadC;
                    CustomerPkey cpk = new CustomerPkey { C_ID = input.C_ID, C_D_ID = input.D_ID, C_W_ID = input.W_ID };
                    this.AddReq(cpk.ToString());
                    break;
                case NewOrderState.ReadC:
                    if (payload == null)
                    {
                        this.Close("read C fail");
                        break;
                    }
                    this.cpl = JsonConvert.DeserializeObject<CustomerPayload>(payload as string);
                    this.C_DISCOUNT = cpl.C_DISCOUNT;
                    //
                    this.currentState = NewOrderState.ReadInitO;
                    OrderPkey opk = new OrderPkey
                    {
                        O_ID = D_NEXT_O_ID,
                        O_D_ID = input.D_ID,
                        O_W_ID = input.W_ID
                    };
                    this.opkStr = opk.ToString();
                    this.AddReq(this.opkStr, null, OperationType.InitiRead);
                    break;
                case NewOrderState.ReadInitO:
                    if (payload != null)
                    {
                        this.Close("read init O fail");
                        break;
                    }
                    this.currentState = NewOrderState.InsertO;
                    // all local or not
                    bool allLocal = true;
                    for (i = 0; i < this.olCount; i++)
                    {
                        allLocal = allLocal & input.OL_I_IDs[i] == input.W_ID;
                    }
                    OrderPayload opl = new OrderPayload
                    {
                        O_C_ID = input.C_ID,
                        O_ENTRY_D = input.O_ENTRY_D,
                        O_CARRIER_ID = Constants.NullCarrierID,
                        O_OL_CNT = (uint)input.OL_I_IDs.Length,
                        O_ALL_LOCAL = Convert.ToUInt32(allLocal)
                    };
                    AddReq(this.opkStr, JsonConvert.SerializeObject(opl), OperationType.Insert);
                    break;
                case NewOrderState.ReadInitNO:
                    if (payload != null)
                    {
                        this.Close("read init NO fail");
                        break;
                    }
                    this.currentState = NewOrderState.InsertNO;
                    this.AddReq(this.nopkStr, Constants.PlaceHolders, OperationType.Insert);
                    break;
                case NewOrderState.InsertOLsReadS:
                    if (payload == null)
                    {
                        this.Close("insert OL read S fail");
                        break;
                    }
                    this.currentState = NewOrderState.InsertOLsUpdateS;
                    this.spl = JsonConvert.DeserializeObject<StockPayload>(payload as string);
                    uint OL_QUANTITY = input.OL_QUANTITYs[i];
                    this.spl.S_YTD += OL_QUANTITY;
                    if (spl.S_QUANTITY >= OL_QUANTITY + 10)
                        spl.S_QUANTITY -= (int)OL_QUANTITY;
                    else
                        spl.S_QUANTITY += 91 - (int)OL_QUANTITY;
                    spl.S_ORDER_CNT += 1;
                    if (input.OL_SUPPLY_W_IDs[i] != input.W_ID) spl.S_REMOTE_CNT += 1;
                    this.AddReq(this.spkStr, JsonConvert.SerializeObject(spl), OperationType.Update);
                    break;
                case NewOrderState.InsertOLsReadInitOL:
                    if (payload != null)
                    {
                        this.Close("insert OL read init OL fail");
                        break;
                    }
                    this.currentState = NewOrderState.InsertOLsInsertOL;
                    double OL_AMOUNT = input.OL_QUANTITYs[i] * items[i].I_PRICE;
                    this.totalFee += OL_AMOUNT;
                    OrderLinePayload olpl = new OrderLinePayload
                    {
                        OL_I_ID = input.OL_I_IDs[i],
                        OL_SUPPLY_W_ID = input.OL_SUPPLY_W_IDs[i],
                        OL_DELIVERY_D = null,
                        OL_QUANTITY = input.OL_QUANTITYs[i],
                        OL_AMOUNT = OL_AMOUNT,
                        OL_DIST_INFO = spl.S_DIST_01        // TODO, assign to S_DIST_XX, where XX equals to D_ID
                    };
                    this.AddReq(this.olpkStr, JsonConvert.SerializeObject(olpl), OperationType.Insert);
                    break;
                default:
                    this.Close("exception read");
                    break;

            }

            this.StoreCurrentState();            
        }

        public override void UpdateCallBack(string tableId, object recordKey, object newPayload)
        {
            switch (this.currentState)
            {
                case NewOrderState.UpdateD:
                    this.currentState = NewOrderState.ReadInitNO;
                    NewOrderPkey nopk = new NewOrderPkey
                    {
                        NO_O_ID = D_NEXT_O_ID,
                        NO_D_ID = input.D_ID,
                        NO_W_ID = input.W_ID
                    };
                    this.nopkStr = nopk.ToString();
                    this.AddReq(this.nopkStr, null, OperationType.InitiRead);
                    break;
                case NewOrderState.InsertOLsUpdateS:
                    this.currentState = NewOrderState.InsertOLsReadInitOL;
                    OrderLinePkey olpk = new OrderLinePkey
                    {
                        OL_O_ID = D_NEXT_O_ID,
                        OL_D_ID = input.D_ID,
                        OL_W_ID = input.W_ID,
                        OL_NUMBER = (uint)i + 1
                    };
                    this.olpkStr = olpk.ToString();
                    this.AddReq(this.olpkStr, null, OperationType.InitiRead);
                    break;
                default:
                    this.Close("exception update");
                    break;
            }

            this.StoreCurrentState();
        }

        public override void DeleteCallBack(string tableId, object recordKey, object payload)
        {
            this.Close();

            this.StoreCurrentState();
        }

        private void ReadStock()
        {
            var spk = new StockPkey { S_I_ID = input.OL_I_IDs[i], S_W_ID = input.OL_SUPPLY_W_IDs[i] };
            this.spkStr = spk.ToString();
            this.AddReq(this.spkStr);            
        }

        private void buildOutput()
        {
            this.totalFee *= (1 - C_DISCOUNT) * (1 + W_TAX + this.dpl.D_TAX);
            NewOrderOutput noOutput = new NewOrderOutput();
            noOutput.other = new Tuple<double, double, uint, double>(W_TAX, this.dpl.D_TAX, D_NEXT_O_ID, this.totalFee);
            noOutput.itemsData = itemsData;
            noOutput.cpl = cpl;

            output.data = noOutput;
        }

        public override void InsertCallBack(string tableId, object recordKey, object payload)
        {
            switch (this.currentState)
            {
                case NewOrderState.InsertO:
                    this.currentState = NewOrderState.UpdateD;
                    this.dpl.D_NEXT_O_ID = this.D_NEXT_O_ID + 1;
                    this.AddReq(this.dpkStr, JsonConvert.SerializeObject(this.dpl), OperationType.Update);
                    break;
                case NewOrderState.InsertNO:
                    this.i = 0;
                    this.currentState = NewOrderState.InsertOLsReadS;
                    ReadStock();
                    break;
                case NewOrderState.InsertOLsInsertOL:
                    // add to output
                    var brand = (items[i].I_DATA.Contains("ORIGINAL") && spl.S_DATA.Contains("ORIGINAL")) ? 'B' : 'G';
                    itemsData[i] = new Tuple<string, int, char, double, double>(items[i].I_NAME, spl.S_QUANTITY, brand, items[i].I_PRICE, input.OL_QUANTITYs[i] * items[i].I_PRICE);

                    this.i += 1;
                    if (this.i < this.olCount)
                    {   
                        this.currentState = NewOrderState.InsertOLsReadS;
                        ReadStock();
                    } else
                    {
                        this.buildOutput();
                        this.Close("normal close");
                    }
                    break;
                default:
                    this.Close("exception insert");
                    break;
            }

            this.StoreCurrentState();
        }

        private void Close(string closeMsg = "")
        {
            //Console.WriteLine("[pid={0}] Close Msg:{1}", this.pid, closeMsg);
            this.errMsg = closeMsg;
            TransactionRequest closeReq = new TransactionRequest(this.sessionId,
                            null, null, null, OperationType.Close);
            this.RequestQueue.Enqueue(closeReq);
        }
    }


    public enum PaymentState
    {
        ToStart,
        ReadC,
        ReadW,
        UpdateW,
        ReadD,
        UpdateD,
        UpdateC,
        ReadInitH,
        InsertH,

    };

    // Payment
    class TPCCPaymentStoredProcedure : StoredProcedure
    {
        private string sessionId;
        private PaymentInParameters input;
        private RedisClient redisClient;

        private uint C_ID;
        private CustomerPayload cpl;
        private string cpkStr;
        private WarehousePayload wpl;
        private string wpkStr;
        private DistrictPayload dpl;
        private string dpkStr;
        private string hpkStr;

        private PaymentState currentState;         // current state

        public TPCCWorkloadOutput output;
        
        public TPCCPaymentStoredProcedure(string sessionId, PaymentInParameters inParams, RedisClient redisClient)
        {
            this.pid = int.Parse(sessionId);
            this.sessionId = sessionId;
            this.input = inParams;
            this.currentState = PaymentState.ToStart;
            this.redisClient = redisClient;
        }

        private void AddReq(string key, string value = null, OperationType otype = OperationType.Read)
        {
            TransactionRequest req = new TransactionRequest(this.sessionId,
                    Constants.DefaultTbl, key, value, otype);
            this.RequestQueue.Enqueue(req);
        }

        public override void Start()
        {
            // init 
            this.output = new TPCCWorkloadOutput();
            output.txFinalStatus = TxFinalStatus.UNKNOWN;

            // the first step
            // determine c_id
            this.C_ID = input.C_ID;
            if (C_ID == 0)  // by c_last
            {
                var k = CustomerPayload.GetLastNameIndexKey(input.C_W_ID, input.C_D_ID, input.C_LAST);
                var ids = redisClient.GetAllItemsFromList(k);
                C_ID = Convert.ToUInt32(ids[ids.Count / 2]);    // TODO order by c_first?
            }

            this.currentState = PaymentState.ReadC;
            var cpk = new CustomerPkey
            {
                C_ID = C_ID,
                C_D_ID = input.C_D_ID,
                C_W_ID = input.C_W_ID
            };
            this.cpkStr = cpk.ToString();
            this.AddReq(this.cpkStr);
        }

        public void StoreCurrentState()
        {
            //TPCCStateTracer.pmstates[pid] = this.currentState;
        }


        public override void ReadCallback(string tableId, object recordKey, object payload)
        {
           switch (this.currentState)
           {
                case PaymentState.ReadC:
                    if (payload == null)
                    {
                        this.Close("readC payload null");
                        break;
                    }
                    this.cpl = JsonConvert.DeserializeObject<CustomerPayload>(payload as string);
                    cpl.C_BALANCE -= input.H_AMOUNT;
                    cpl.C_YTD_PAYMENT += input.H_AMOUNT;
                    cpl.C_PAYMENT_CNT += 1;

                    this.currentState = PaymentState.ReadW;                    
                    var wpk = new WarehousePkey { W_ID = input.W_ID };
                    this.wpkStr = wpk.ToString();
                    this.AddReq(this.wpkStr);
                    break;
                case PaymentState.ReadW:
                    if (payload == null)
                    {
                        this.Close("readW payload null");
                        break;
                    }
                    this.currentState = PaymentState.UpdateW;
                    wpl = JsonConvert.DeserializeObject<WarehousePayload>(payload as string);
                    wpl.W_YTD += input.H_AMOUNT;
                    this.AddReq(this.wpkStr, JsonConvert.SerializeObject(wpl), OperationType.Update);
                    break;
                case PaymentState.ReadD:
                    if (payload == null)
                    {
                        this.Close("readD payload null");
                        break;
                    }
                    this.currentState = PaymentState.UpdateD;
                    dpl = JsonConvert.DeserializeObject<DistrictPayload>(payload as string);
                    dpl.D_YTD += input.H_AMOUNT;
                    this.AddReq(this.dpkStr, JsonConvert.SerializeObject(dpl), OperationType.Update);
                    break;
                case PaymentState.ReadInitH:
                    this.currentState = PaymentState.InsertH;
                    var hpl = new HistoryPayload
                    {
                        H_C_ID = C_ID,
                        H_C_D_ID = input.C_D_ID,
                        H_C_W_ID = input.C_W_ID,
                        H_D_ID = input.D_ID,
                        H_W_ID = input.W_ID,
                        H_DATA = wpl.W_NAME + "    " + dpl.D_NAME,
                        H_AMOUNT = input.H_AMOUNT,
                        H_DATE = input.timestamp
                    };
                    this.AddReq(this.hpkStr, JsonConvert.SerializeObject(hpl), OperationType.Insert);
                    break;
                default:
                    this.Close("unexcepted read");
                    break;
           }

           this.StoreCurrentState();
        }

        public override void UpdateCallBack(string tableId, object recordKey, object newPayload)
        {
            switch (this.currentState)
            {
                case PaymentState.UpdateW:
                    this.currentState = PaymentState.ReadD;
                    var dpk = new DistrictPkey { D_ID = input.D_ID, D_W_ID = input.W_ID };
                    this.dpkStr = dpk.ToString();
                    this.AddReq(this.dpkStr);
                    break;
                case PaymentState.UpdateD:
                    // credit info
                    this.currentState = PaymentState.UpdateC;
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
                    this.AddReq(this.cpkStr, JsonConvert.SerializeObject(cpl), OperationType.Update);
                    break;
                case PaymentState.UpdateC:
                    this.currentState = PaymentState.ReadInitH;
                    var hpk = HistoryPayload.GetHPkey();
                    this.hpkStr = hpk.ToString();
                    this.AddReq(this.hpkStr, null, OperationType.InitiRead);
                    break;                    
                default:
                    this.Close("unexcepted update");
                    break;
            }
            this.StoreCurrentState();
        }

        public override void DeleteCallBack(string tableId, object recordKey, object payload)
        {
            this.StoreCurrentState();
            this.Close("delete close");
        }


        private void buildOutput()
        {
            PaymentOutput pmOutput = new PaymentOutput();
            pmOutput.wpl = wpl;
            pmOutput.dpl = dpl;
            pmOutput.cpl = cpl;
            output.data = pmOutput;
        }

        public override void InsertCallBack(string tableId, object recordKey, object payload)
        {
            switch (this.currentState)
            {
                case PaymentState.InsertH:
                    this.buildOutput();
                    this.Close("normal close: insert");
                    break;
                default:
                    this.Close("unexcepted insert");
                    break;
            }
            this.StoreCurrentState();
        }

        private void Close(string closeMsg = "")
        {
            //Console.WriteLine("[pid={0}] Close Msg:{1}", this.pid, closeMsg);
            this.AddReq(null, null, OperationType.Close);
        }
    }
}
