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
    public static class TableUtil
    {
        static public string Name(this TableType t)
        {
            return TableTypeNames[(int)t];
        }
        static private string[] TableTypeNames =
            typeof(TableType).GetEnumNames();

        static public string ToFilename(this TableType v, string dir = "")
        {
            return $"{dir}\\{v.Name()}.csv";
        }
        static public int HashFields(params object[] objs)
        {
            int hash = 17;
            foreach (var obj in objs)
            {
                hash = hash * 23 + obj.GetHashCode();
            }
            return hash;
        }
    }
    public enum TableType
    {
        CUSTOMER, WAREHOUSE, DISTRICT,
        HISTORY, ITEM, NEW_ORDER,
        ORDER_LINE, ORDERS, STOCK
    }


    public abstract class TpccTable
    {
        static private TpccTable[] instances =
        {
            new Customer(), new Warehouse(), new District(),
            new History(), new Item(), new NewOrder(),
            new OrderLine(), new Order(), new Stock()
        };
        static public TableType[] allTypes =
            Enum.GetValues(typeof(TableType)) as TableType[];

        static public TpccTable Instance(TableType v)
        {
            return instances[(int)v];
        }

        struct PayloadPlaceholder { };

        /// <summary>
        /// Turn csv string columns to (XXPKey, XXPayload) objects
        /// </summary>
        public abstract Tuple<TpccTableKey, TpccTablePayload> ParseColumns(string[] columns);

        public abstract TableType Type();

        public string Name()
        {
            return this.Type().Name();
        }

        class Customer : TpccTable
        {
            public override
            Tuple<TpccTableKey, TpccTablePayload> ParseColumns(string[] columns)
            {
                var cpk = new CustomerPkey
                {
                    C_ID = Convert.ToUInt32(columns[0]),
                    C_D_ID = Convert.ToUInt32(columns[1]),
                    C_W_ID = Convert.ToUInt32(columns[2])
                };
                var cpl = new CustomerPayload
                {
                    C_FIRST = columns[3],
                    C_MIDDLE = columns[4],
                    C_LAST = columns[5],
                    C_STREET_1 = columns[6],
                    C_STREET_2 = columns[7],
                    C_CITY = columns[8],
                    C_STATE = columns[9],
                    C_ZIP = columns[10],
                    C_PHONE = columns[11],
                    C_SINCE = columns[12],
                    C_CREDIT = columns[13],
                    C_CREDIT_LIM = Convert.ToDouble(columns[14]),
                    C_DISCOUNT = Convert.ToDouble(columns[15]),
                    C_BALANCE = Convert.ToDouble(columns[16]),
                    C_YTD_PAYMENT = Convert.ToDouble(columns[17]),
                    C_PAYMENT_CNT = Convert.ToUInt32(columns[18]),
                    C_DELIVERY_CNT = Convert.ToUInt32(columns[19]),
                    C_DATA = columns[20]
                };
                return new Tuple<TpccTableKey, TpccTablePayload>(cpk, cpl);
            }
            public override TableType Type()
            {
                return TableType.CUSTOMER;
            }
        }

        class Warehouse : TpccTable
        {
            public override Tuple<TpccTableKey, TpccTablePayload> ParseColumns(string[] columns)
            {
                var wpk = new WarehousePkey
                {
                    W_ID = Convert.ToUInt32(columns[0])
                };
                var wpl = new WarehousePayload
                {
                    W_NAME = columns[1],
                    W_STREET_1 = columns[2],
                    W_STREET_2 = columns[3],
                    W_CITY = columns[4],
                    W_STATE = columns[5],
                    W_ZIP = columns[6],
                    W_TAX = Convert.ToDouble(columns[7]),
                    W_YTD = Convert.ToDouble(columns[8])
                };
                return new Tuple<TpccTableKey, TpccTablePayload>(wpk, wpl);
            }

            public override TableType Type()
            {
                return TableType.WAREHOUSE;
            }
        }

        class District : TpccTable
        {
            public override Tuple<TpccTableKey, TpccTablePayload> ParseColumns(string[] columns)
            {
                var dpk = new DistrictPkey
                {
                    D_ID = Convert.ToUInt32(columns[0]),
                    D_W_ID = Convert.ToUInt32(columns[1])
                };
                var dpl = new DistrictPayload
                {
                    D_NAME = columns[2],
                    D_STREET_1 = columns[3],
                    D_STREET_2 = columns[4],
                    D_CITY = columns[5],
                    D_STATE = columns[6],
                    D_ZIP = columns[7],
                    D_TAX = Convert.ToDouble(columns[8]),
                    D_YTD = Convert.ToDouble(columns[9]),
                    D_NEXT_O_ID = Convert.ToUInt32(columns[10])
                };
                return new Tuple<TpccTableKey, TpccTablePayload>(dpk, dpl);
            }

            public override TableType Type()
            {
                return TableType.DISTRICT;
            }
        }

        class Item : TpccTable
        {
            public override Tuple<TpccTableKey, TpccTablePayload> ParseColumns(string[] columns)
            {
                var ipk = new ItemPkey
                {
                    I_ID = Convert.ToUInt32(columns[0])
                };
                var ipl = new ItemPayload
                {
                    I_IM_ID = Convert.ToUInt32(columns[1]),
                    I_NAME = columns[2],
                    I_PRICE = Convert.ToDouble(columns[3]),
                    I_DATA = columns[4]
                };
                return new Tuple<TpccTableKey, TpccTablePayload>(ipk, ipl);
            }
            public override TableType Type()
            {
                return TableType.ITEM;
            }
        }

        class Stock : TpccTable
        {
            public override Tuple<TpccTableKey, TpccTablePayload> ParseColumns(string[] columns)
            {
                var spk = new StockPkey
                {
                    S_I_ID = Convert.ToUInt32(columns[0]),
                    S_W_ID = Convert.ToUInt32(columns[1])
                };
                var spl = new StockPayload
                {
                    S_QUANTITY = Convert.ToInt32(columns[2]),
                    S_DIST_01 = columns[3],
                    S_DIST_02 = columns[4],
                    S_DIST_03 = columns[5],
                    S_DIST_04 = columns[6],
                    S_DIST_05 = columns[7],
                    S_DIST_06 = columns[8],
                    S_DIST_07 = columns[9],
                    S_DIST_08 = columns[10],
                    S_DIST_09 = columns[11],
                    S_DIST_10 = columns[12],
                    S_YTD = Convert.ToUInt32(columns[13]),
                    S_ORDER_CNT = Convert.ToUInt32(columns[14]),
                    S_REMOTE_CNT = Convert.ToUInt32(columns[15]),
                    S_DATA = columns[16]
                };
                return new Tuple<TpccTableKey, TpccTablePayload>(spk, spl);
            }
            public override TableType Type()
            {
                return TableType.STOCK;
            }
        }

        class Order : TpccTable
        {
            public override Tuple<TpccTableKey, TpccTablePayload> ParseColumns(string[] columns)
            {
                var opk = new OrderPkey
                {
                    O_ID = Convert.ToUInt32(columns[0]),
                    O_D_ID = Convert.ToUInt32(columns[1]),
                    O_W_ID = Convert.ToUInt32(columns[2])
                };
                var opl = new OrderPayload
                {
                    O_C_ID = Convert.ToUInt32(columns[3]),
                    O_ENTRY_D = columns[4],
                    O_CARRIER_ID = Convert.ToUInt32(columns[5]),
                    O_OL_CNT = Convert.ToUInt32(columns[6]),
                    O_ALL_LOCAL = Convert.ToUInt32(columns[7])
                };
                return new Tuple<TpccTableKey, TpccTablePayload>(opk, opl);
            }
            public override TableType Type()
            {
                return TableType.ORDERS;
            }
        }

        class OrderLine : TpccTable
        {
            public override Tuple<TpccTableKey, TpccTablePayload> ParseColumns(string[] columns)
            {
                var olpk = new OrderLinePkey
                {
                    OL_O_ID = Convert.ToUInt32(columns[0]),
                    OL_D_ID = Convert.ToUInt32(columns[1]),
                    OL_W_ID = Convert.ToUInt32(columns[2]),
                    OL_NUMBER = Convert.ToUInt32(columns[3])
                };
                var olpl = new OrderLinePayload
                {
                    OL_I_ID = Convert.ToUInt32(columns[4]),
                    OL_SUPPLY_W_ID = Convert.ToUInt32(columns[5]),
                    OL_DELIVERY_D = columns[6],
                    OL_QUANTITY = Convert.ToUInt32(columns[7]),
                    OL_AMOUNT = Convert.ToDouble(columns[8]),
                    OL_DIST_INFO = columns[9]
                };
                return new Tuple<TpccTableKey, TpccTablePayload>(olpk, olpl);
            }
            public override TableType Type()
            {
                return TableType.ORDER_LINE;
            }
        }

        class NewOrder : TpccTable
        {
            public override Tuple<TpccTableKey, TpccTablePayload> ParseColumns(string[] columns)
            {
                var nopk = new NewOrderPkey
                {
                    NO_O_ID = Convert.ToUInt32(columns[0]),
                    NO_D_ID = Convert.ToUInt32(columns[1]),
                    NO_W_ID = Convert.ToUInt32(columns[2])
                };
                return new Tuple<TpccTableKey, TpccTablePayload>(nopk, new NewOrderPayload());
            }
            public override TableType Type()
            {
                return TableType.NEW_ORDER;
            }
        }

        class History : TpccTable
        {
            public override Tuple<TpccTableKey, TpccTablePayload> ParseColumns(string[] columns)
            {
                var hpl = new HistoryPayload
                {
                    H_C_ID = Convert.ToUInt32(columns[0]),
                    H_C_D_ID = Convert.ToUInt32(columns[1]),
                    H_C_W_ID = Convert.ToUInt32(columns[2]),
                    H_D_ID = Convert.ToUInt32(columns[3]),
                    H_W_ID = Convert.ToUInt32(columns[4]),
                    H_DATE = columns[5],
                    H_AMOUNT = Convert.ToDouble(columns[6]),
                    H_DATA = columns[7]
                };
                HistoryPKey hpk = HistoryPKey.New();
                return new Tuple<TpccTableKey, TpccTablePayload>(hpk, hpl);
            }
            public override TableType Type()
            {
                return TableType.HISTORY;
            }
        }
    }
    public class TpccTableKV
    {
        protected TpccTableKV(TableType type)
        {
            this.Table = TpccTable.Instance(type);
        }
        public TpccTable Table { get; private set; }
    }
    public class TpccTableKey : TpccTableKV
    {
        protected TpccTableKey(TableType t) : base(t) { }
    }
    public class TpccTablePayload : TpccTableKV
    {
        protected TpccTablePayload(TableType t) : base(t) { }
    }
    // Warehouse
    public class WarehousePkey : TpccTableKey
    {
        public WarehousePkey() : base(TableType.WAREHOUSE) { }

        public uint W_ID;
        public override string ToString()
        {
            return "W-" + W_ID.ToString();
        }

        public override int GetHashCode()
        {
            return W_ID.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            WarehousePkey that = obj as WarehousePkey;
            return that != null && this.W_ID == that.W_ID;
        }
    }
    public class WarehousePayload : TpccTablePayload
    {
        public WarehousePayload() : base(TableType.WAREHOUSE) { }

        public string W_NAME;
        public string W_STREET_1;
        public string W_STREET_2;
        public string W_CITY;
        public string W_STATE;
        public string W_ZIP;
        public double W_TAX;
        public double W_YTD;
    }

    // District
    public class DistrictPkey : TpccTableKey
    {
        public DistrictPkey() : base(TableType.DISTRICT) { }

        public uint D_ID;
        public uint D_W_ID;
        public override string ToString()
        {
            return "D-" + D_ID + "-" + D_W_ID;
        }

        public override int GetHashCode()
        {
            return TableUtil.HashFields(this.D_ID, this.D_W_ID);
        }

        public override bool Equals(object obj)
        {
            DistrictPkey that = obj as DistrictPkey;
            return that != null
                && this.D_ID == that.D_ID
                && this.D_W_ID == that.D_W_ID;
        }
    }
    public class DistrictPayload : TpccTablePayload
    {
        public DistrictPayload() : base(TableType.DISTRICT) { }

        public string D_NAME;
        public string D_STREET_1;
        public string D_STREET_2;
        public string D_CITY;
        public string D_STATE;
        public string D_ZIP;
        public double D_TAX;
        public double D_YTD;
        public uint D_NEXT_O_ID;
    }

    // Customer
    public class CustomerPkey : TpccTableKey
    {
        public CustomerPkey() : base(TableType.CUSTOMER) { }

        public uint C_ID;
        public uint C_D_ID;
        public uint C_W_ID;
        public override string ToString()
        {
            return "C-" + C_ID + "-" + C_D_ID + "-" + C_W_ID;
        }

        public override int GetHashCode()
        {
            return TableUtil.HashFields(this.C_ID, this.C_D_ID, this.C_W_ID);
        }

        public override bool Equals(object obj)
        {
            CustomerPkey that = obj as CustomerPkey;
            return that != null
                && this.C_ID == that.C_ID
                && this.C_D_ID == that.C_D_ID
                && this.C_W_ID == that.C_W_ID;
        }
    }
    public class CustomerLastNameIndexKey : TpccTableKey
    {
        public CustomerLastNameIndexKey() : base(TableType.CUSTOMER) { }

        public uint C_W_ID;
        public uint C_D_ID;
        public string C_LAST;

        public override int GetHashCode()
        {
            return TableUtil.HashFields(this.C_W_ID, this.C_D_ID, this.C_LAST);
        }
        public override bool Equals(object obj)
        {
            CustomerLastNameIndexKey that = obj as CustomerLastNameIndexKey;
            return that != null
                && this.C_W_ID == that.C_W_ID
                && this.C_D_ID == that.C_D_ID
                && this.C_LAST == that.C_LAST;
        }
        static public CustomerLastNameIndexKey
        FromPKeyAndPayload(CustomerPkey cpk, CustomerPayload cpl)
        {
            return new CustomerLastNameIndexKey
            {
                C_W_ID = cpk.C_W_ID,
                C_D_ID = cpk.C_D_ID,
                C_LAST = cpl.C_LAST,
            };
        }
    }
    public class CustomerLastNamePayloads : TpccTablePayload
    {
        public CustomerLastNamePayloads() : base(TableType.CUSTOMER) { }
        public uint GetRequiredId()
        {
            return C_IDs[C_IDs.Length / 2];
        }
        static public CustomerLastNamePayloads FromList(List<uint> cids)
        {
            uint[] cid_array = cids.ToArray();
            Array.Sort(cid_array);
            return new CustomerLastNamePayloads { C_IDs = cid_array };
        }
        public uint[] C_IDs;
    }
    public class CustomerPayload : TpccTablePayload
    {
        public CustomerPayload() : base(TableType.CUSTOMER) { }

        public string C_FIRST;
        public string C_MIDDLE;
        public string C_LAST;
        public string C_STREET_1;
        public string C_STREET_2;
        public string C_CITY;
        public string C_STATE;
        public string C_ZIP;
        public string C_PHONE;
        public string C_SINCE;
        public string C_CREDIT;
        public double C_CREDIT_LIM;
        public double C_DISCOUNT;
        public double C_BALANCE;
        public double C_YTD_PAYMENT;
        public uint C_PAYMENT_CNT;
        public uint C_DELIVERY_CNT;
        public string C_DATA;

        public string GetLastNameIndexKey(CustomerPkey cpk)
        {
            return "C-" + cpk.C_W_ID + "-" + cpk.C_D_ID + "-" + C_LAST;
        }
        public static string GetLastNameIndexKey(uint c_w_id, uint c_d_id, string c_last)
        {
            return "C-" + c_w_id + "-" + c_d_id + "-" + c_last;
        }
    }

    public class HistoryPKey : TpccTableKey
    {
        static public HistoryPKey New()
        {
            return new HistoryPKey { GUID = HistoryPayload.GetHPkey() };
        }

        public HistoryPKey() : base(TableType.HISTORY) { }

        public string GUID;
    }

    // HISTORY primary key is uuid
    public class HistoryPayload : TpccTablePayload
    {
        public HistoryPayload() : base(TableType.HISTORY) { }

        public uint H_C_ID;
        public uint H_C_D_ID;
        public uint H_C_W_ID;
        public uint H_D_ID;
        public uint H_W_ID;
        public string H_DATE;
        public double H_AMOUNT;
        public string H_DATA;

        public static string GetHPkey()
        {
            return "H-" + Guid.NewGuid().ToString("N");
        }
    }

    // NEW-ORDER
    public class NewOrderPkey : TpccTableKey
    {
        public NewOrderPkey() : base(TableType.NEW_ORDER) { }

        public uint NO_O_ID;
        public uint NO_D_ID;
        public uint NO_W_ID;
        public override string ToString()
        {
            return "NO-" + NO_O_ID + "-" + NO_D_ID + "-" + NO_W_ID;
        }

        public override int GetHashCode()
        {
            return TableUtil.HashFields(
                this.NO_O_ID, this.NO_D_ID, this.NO_W_ID);
        }

        public override bool Equals(object obj)
        {
            NewOrderPkey that = obj as NewOrderPkey;
            return that != null
                && this.NO_O_ID == that.NO_O_ID
                && this.NO_D_ID == that.NO_D_ID
                && this.NO_W_ID == that.NO_W_ID;
        }
    }
    public class NewOrderPayload : TpccTablePayload    // no use
    {
        public NewOrderPayload() : base(TableType.NEW_ORDER) { }

        static public NewOrderPayload Placeholder()
        {
            if (NewOrderPayload.instance == null)
            {
                NewOrderPayload.instance = new NewOrderPayload();
            }
            return NewOrderPayload.instance;
        }
        static private NewOrderPayload instance = null;
        /*
        public char NO_PL;  // Specially, it is just a placeholder character `*`, not json string
        */
    }

    // ORDER
    public class OrderPkey : TpccTableKey
    {
        public OrderPkey() : base(TableType.ORDERS) { }
        public uint O_ID;
        public uint O_D_ID;
        public uint O_W_ID;

        public override string ToString()
        {
            return "O-" + O_ID + "-" + O_D_ID + "-" + O_W_ID;
        }

        public override int GetHashCode()
        {
            return TableUtil.HashFields(this.O_ID, this.O_D_ID, this.O_W_ID);
        }

        public override bool Equals(object obj)
        {
            OrderPkey that = obj as OrderPkey;
            return that != null
                && this.O_ID == that.O_ID
                && this.O_D_ID == that.O_D_ID
                && this.O_W_ID == that.O_W_ID;
        }
    }
    public class OrderPayload : TpccTablePayload
    {
        public OrderPayload() : base(TableType.ORDERS) { }
        public uint O_C_ID;
        public string O_ENTRY_D;
        public uint O_CARRIER_ID;
        public uint O_OL_CNT;
        public uint O_ALL_LOCAL;
    }

    // ORDER LINE
    public class OrderLinePkey : TpccTableKey
    {
        public OrderLinePkey() : base(TableType.ORDER_LINE) { }
        public uint OL_O_ID;
        public uint OL_D_ID;
        public uint OL_W_ID;
        public uint OL_NUMBER;
        public override string ToString()
        {
            return "OL-" + OL_O_ID + "-" + OL_D_ID + "-" + OL_W_ID + "-" + OL_NUMBER;
        }

        public override int GetHashCode()
        {
            return TableUtil.HashFields(
                this.OL_D_ID, this.OL_O_ID, this.OL_W_ID, this.OL_NUMBER);
        }

        public override bool Equals(object obj)
        {
            OrderLinePkey that = obj as OrderLinePkey;
            return that != null
                && this.OL_O_ID == that.OL_O_ID
                && this.OL_D_ID == that.OL_D_ID
                && this.OL_W_ID == that.OL_W_ID
                && this.OL_NUMBER == that.OL_NUMBER;
        }
    }
    public class OrderLinePayload : TpccTablePayload
    {
        public OrderLinePayload() : base(TableType.ORDER_LINE) { }

        public uint OL_I_ID;
        public uint OL_SUPPLY_W_ID;
        public string OL_DELIVERY_D;
        public uint OL_QUANTITY;
        public double OL_AMOUNT;
        public string OL_DIST_INFO;
    }

    // ITEM
    public class ItemPkey : TpccTableKey
    {
        public ItemPkey() : base(TableType.ITEM) { }

        public uint I_ID;
        public override string ToString()
        {
            return "I-" + I_ID.ToString();
        }

        public override int GetHashCode()
        {
            return this.I_ID.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            ItemPkey that = obj as ItemPkey;
            return that != null && this.I_ID == that.I_ID;
        }
    }
    public class ItemPayload : TpccTablePayload
    {
        public ItemPayload() : base(TableType.ITEM) { }
        public uint I_IM_ID;
        public string I_NAME;
        public double I_PRICE;
        public string I_DATA;
    }

    // STOCK
    public class StockPkey : TpccTableKey
    {
        public StockPkey() : base(TableType.STOCK) { }

        public uint S_I_ID;
        public uint S_W_ID;
        public override string ToString()
        {
            return "S-" + S_I_ID + "-" + S_W_ID;
        }

        public override int GetHashCode()
        {
            return TableUtil.HashFields(this.S_I_ID, this.S_W_ID);
        }

        public override bool Equals(object obj)
        {
            StockPkey that = obj as StockPkey;
            return that != null
                && this.S_I_ID == that.S_I_ID
                && this.S_W_ID == that.S_W_ID;
        }

    }
    public class StockPayload : TpccTablePayload
    {
        public StockPayload() : base(TableType.STOCK) { }

        public int S_QUANTITY;
        public string S_DIST_01;
        public string S_DIST_02;
        public string S_DIST_03;
        public string S_DIST_04;
        public string S_DIST_05;
        public string S_DIST_06;
        public string S_DIST_07;
        public string S_DIST_08;
        public string S_DIST_09;
        public string S_DIST_10;
        public uint S_YTD;
        public uint S_ORDER_CNT;
        public uint S_REMOTE_CNT;
        public string S_DATA;
    }

    static class RecordGenerator
    {
        public static Tuple<string, string> BuildRedisKV(TableCode code, string[] columns, RedisClient redisClient = null)
        {
            string key = null;
            string value = null;

            switch (code)
            {
                case TableCode.W:   // Warehouse
                    var wpk = new WarehousePkey
                    {
                        W_ID = Convert.ToUInt32(columns[0])
                    };
                    var wpl = new WarehousePayload
                    {
                        W_NAME = columns[1],
                        W_STREET_1 = columns[2],
                        W_STREET_2 = columns[3],
                        W_CITY = columns[4],
                        W_STATE = columns[5],
                        W_ZIP = columns[6],
                        W_TAX = Convert.ToDouble(columns[7]),
                        W_YTD = Convert.ToDouble(columns[8])
                    };
                    key = wpk.ToString();
                    value = JsonConvert.SerializeObject(wpl);
                    break;

                case TableCode.D:   // District
                    var dpk = new DistrictPkey
                    {
                        D_ID = Convert.ToUInt32(columns[0]),
                        D_W_ID = Convert.ToUInt32(columns[1])
                    };
                    var dpl = new DistrictPayload
                    {
                        D_NAME = columns[2],
                        D_STREET_1 = columns[3],
                        D_STREET_2 = columns[4],
                        D_CITY = columns[5],
                        D_STATE = columns[6],
                        D_ZIP = columns[7],
                        D_TAX = Convert.ToDouble(columns[8]),
                        D_YTD = Convert.ToDouble(columns[9]),
                        D_NEXT_O_ID = Convert.ToUInt32(columns[10])
                    };
                    key = dpk.ToString();
                    value = JsonConvert.SerializeObject(dpl);
                    break;

                case TableCode.C:   // Customer
                    var cpk = new CustomerPkey
                    {
                        C_ID = Convert.ToUInt32(columns[0]),
                        C_D_ID = Convert.ToUInt32(columns[1]),
                        C_W_ID = Convert.ToUInt32(columns[2])
                    };
                    var cpl = new CustomerPayload
                    {
                        C_FIRST = columns[3],
                        C_MIDDLE = columns[4],
                        C_LAST = columns[5],
                        C_STREET_1 = columns[6],
                        C_STREET_2 = columns[7],
                        C_CITY = columns[8],
                        C_STATE = columns[9],
                        C_ZIP = columns[10],
                        C_PHONE = columns[11],
                        C_SINCE = columns[12],
                        C_CREDIT = columns[13],
                        C_CREDIT_LIM = Convert.ToDouble(columns[14]),
                        C_DISCOUNT = Convert.ToDouble(columns[15]),
                        C_BALANCE = Convert.ToDouble(columns[16]),
                        C_YTD_PAYMENT = Convert.ToDouble(columns[17]),
                        C_PAYMENT_CNT = Convert.ToUInt32(columns[18]),
                        C_DELIVERY_CNT = Convert.ToUInt32(columns[19]),
                        C_DATA = columns[20]
                    };
                    key = cpk.ToString();
                    value = JsonConvert.SerializeObject(cpl);
                    redisClient.AddItemToList(cpl.GetLastNameIndexKey(cpk), cpk.C_ID.ToString());   // last name index
                    break;

                case TableCode.I:   // Item
                    var ipk = new ItemPkey
                    {
                        I_ID = Convert.ToUInt32(columns[0])
                    };
                    var ipl = new ItemPayload
                    {
                        I_IM_ID = Convert.ToUInt32(columns[1]),
                        I_NAME = columns[2],
                        I_PRICE = Convert.ToDouble(columns[3]),
                        I_DATA = columns[4]
                    };
                    key = ipk.ToString();
                    value = JsonConvert.SerializeObject(ipl);
                    break;

                case TableCode.S:   // Stock
                    var spk = new StockPkey
                    {
                        S_I_ID = Convert.ToUInt32(columns[0]),
                        S_W_ID = Convert.ToUInt32(columns[1])
                    };
                    var spl = new StockPayload
                    {
                        S_QUANTITY = Convert.ToInt32(columns[2]),
                        S_DIST_01 = columns[3],
                        S_DIST_02 = columns[4],
                        S_DIST_03 = columns[5],
                        S_DIST_04 = columns[6],
                        S_DIST_05 = columns[7],
                        S_DIST_06 = columns[8],
                        S_DIST_07 = columns[9],
                        S_DIST_08 = columns[10],
                        S_DIST_09 = columns[11],
                        S_DIST_10 = columns[12],
                        S_YTD = Convert.ToUInt32(columns[13]),
                        S_ORDER_CNT = Convert.ToUInt32(columns[14]),
                        S_REMOTE_CNT = Convert.ToUInt32(columns[15]),
                        S_DATA = columns[16]
                    };
                    key = spk.ToString();
                    value = JsonConvert.SerializeObject(spl);
                    break;

                case TableCode.O:   // Order
                    var opk = new OrderPkey
                    {
                        O_ID = Convert.ToUInt32(columns[0]),
                        O_D_ID = Convert.ToUInt32(columns[1]),
                        O_W_ID = Convert.ToUInt32(columns[2])
                    };
                    var opl = new OrderPayload
                    {
                        O_C_ID = Convert.ToUInt32(columns[3]),
                        O_ENTRY_D = columns[4],
                        O_CARRIER_ID = Convert.ToUInt32(columns[5]),
                        O_OL_CNT = Convert.ToUInt32(columns[6]),
                        O_ALL_LOCAL = Convert.ToUInt32(columns[7])
                    };
                    key = opk.ToString();
                    value = JsonConvert.SerializeObject(opl);
                    break;

                case TableCode.OL:  // Order Line
                    var olpk = new OrderLinePkey
                    {
                        OL_O_ID = Convert.ToUInt32(columns[0]),
                        OL_D_ID = Convert.ToUInt32(columns[1]),
                        OL_W_ID = Convert.ToUInt32(columns[2]),
                        OL_NUMBER = Convert.ToUInt32(columns[3])
                    };
                    var olpl = new OrderLinePayload
                    {
                        OL_I_ID = Convert.ToUInt32(columns[4]),
                        OL_SUPPLY_W_ID = Convert.ToUInt32(columns[5]),
                        OL_DELIVERY_D = columns[6],
                        OL_QUANTITY = Convert.ToUInt32(columns[7]),
                        OL_AMOUNT = Convert.ToDouble(columns[8]),
                        OL_DIST_INFO = columns[9]
                    };
                    key = olpk.ToString();
                    value = JsonConvert.SerializeObject(olpl);
                    break;

                case TableCode.NO:  // New Order
                    var nopk = new NewOrderPkey
                    {
                        NO_O_ID = Convert.ToUInt32(columns[0]),
                        NO_D_ID = Convert.ToUInt32(columns[1]),
                        NO_W_ID = Convert.ToUInt32(columns[2])
                    };
                    key = nopk.ToString();
                    value = "1";    // placeholder
                    break;

                case TableCode.H:   // History
                    var hpl = new HistoryPayload
                    {
                        H_C_ID = Convert.ToUInt32(columns[0]),
                        H_C_D_ID = Convert.ToUInt32(columns[1]),
                        H_C_W_ID = Convert.ToUInt32(columns[2]),
                        H_D_ID = Convert.ToUInt32(columns[3]),
                        H_W_ID = Convert.ToUInt32(columns[4]),
                        H_DATE = columns[5],
                        H_AMOUNT = Convert.ToDouble(columns[6]),
                        H_DATA = columns[7]
                    };
                    key = HistoryPayload.GetHPkey();
                    value = JsonConvert.SerializeObject(hpl);
                    break;

                default:
                    break;
            }

            return new Tuple<string, string>(key, value);
        }
    }

}
