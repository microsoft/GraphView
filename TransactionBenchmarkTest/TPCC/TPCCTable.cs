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
        static private string[] TableTypeNames = typeof(TableType).GetEnumNames();
    }
    public enum TableType
    {
        CUSTOMER, WAREHOUSE, DISTRICT,
        HISTORY, ITEM, NEW_ORDER,
        ORDER_LINE, ORDERS, STOCK, CUSTOMER_INDEX
    }


    public abstract class TpccTable
    {
        static private TpccTable[] instances =
        {
            new Customer(), new Warehouse(), new District(),
            new History(), new Item(), new NewOrder(),
            new OrderLine(), new Order(), new Stock(),
            new CustomerLastNameIndex()
        };
        static public TableType[] allTypes =
            Enum.GetValues(typeof(TableType)) as TableType[];

        static public TableType[] AllUsedTypes = new TableType[] {
                TableType.CUSTOMER, TableType.WAREHOUSE, TableType.DISTRICT,
                TableType.ITEM, TableType.STOCK };

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

        class CustomerLastNameIndex : TpccTable
        {
            public override Tuple<TpccTableKey, TpccTablePayload> ParseColumns(string[] columns)
            {
                return null;
            }
            public override TableType Type()
            {
                return TableType.CUSTOMER_INDEX;
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
                    O_D_ID = Convert.ToUInt32(columns[2]),
                    O_W_ID = Convert.ToUInt32(columns[3])
                };
                var opl = new OrderPayload
                {
                    O_C_ID = Convert.ToUInt32(columns[1]),
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
        public TpccTable Table { get; protected set; }
    }
    public abstract class TpccTableKey : TpccTableKV, Copyable
    {
        protected TpccTableKey(TableType t) : base(t) { }

        public abstract Copyable Copy();

        public abstract bool CopyFrom(Copyable copyable);
    }
    public abstract class TpccTablePayload : TpccTableKV, Copyable
    {
        protected TpccTablePayload(TableType t) : base(t) { }

        public abstract Copyable Copy();

        public abstract bool CopyFrom(Copyable copyable);
    }
    // Warehouse
    public class WarehousePkey : TpccTableKey
    {
        public WarehousePkey() : base(TableType.WAREHOUSE) { }

        public const int IdBits = 7;

        public uint W_ID;
        public override string ToString()
        {
            return "W-" + W_ID.ToString();
        }

        public override int GetHashCode()
        {
            return (int)W_ID;
        }

        public override bool Equals(object obj)
        {
            WarehousePkey that = obj as WarehousePkey;
            return that != null && this.W_ID == that.W_ID;
        }
        public override Copyable Copy()
        {
            WarehousePkey copy = new WarehousePkey();
            copy.SafeCopyFrom(this);
            return copy;
        }
        public override bool CopyFrom(Copyable copyable)
        {
            WarehousePkey that = copyable as WarehousePkey;
            if (that == null) return false;
            this.SafeCopyFrom(that);
            return true;
        }
        public void SafeCopyFrom(WarehousePkey that)
        {
            this.Set(that.W_ID);
        }
        public void Set(uint W_ID)
        {
            this.W_ID = W_ID;
        }
    }
    public class WarehousePayload : TpccTablePayload
    {
        public WarehousePayload() : base(TableType.WAREHOUSE) { }

        public override Copyable Copy()
        {
            WarehousePayload copy = new WarehousePayload();
            copy.SafeCopyFrom(this);
            return copy;
        }
        public override bool CopyFrom(Copyable copyable)
        {
            WarehousePayload that = copyable as WarehousePayload;
            if (that == null) return false;
            this.SafeCopyFrom(that);
            return true;
        }
        public void SafeCopyFrom(WarehousePayload that)
        {
            this.Set(
                that.W_NAME, that.W_STREET_1, that.W_STREET_2, that.W_CITY,
                that.W_STATE, that.W_ZIP, that.W_TAX, that.W_YTD);
        }

        public void Set(
            string W_NAME, string W_STREET_1, string W_STREET_2, string W_CITY,
            string W_STATE, string W_ZIP, double W_TAX, double W_YTD)
        {
            this.W_NAME = W_NAME;
            this.W_STREET_1 = W_STREET_1;
            this.W_STREET_2 = W_STREET_2;
            this.W_CITY = W_CITY;
            this.W_STATE = W_STATE;
            this.W_ZIP = W_ZIP;
            this.W_TAX = W_TAX;
            this.W_YTD = W_YTD;
        }

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

        public const int IdBits = 4;

        public uint D_ID;
        public uint D_W_ID;
        public override string ToString()
        {
            return "D-" + D_ID + "-" + D_W_ID;
        }

        public override int GetHashCode()
        {
            return (int)(this.D_ID << WarehousePkey.IdBits | this.D_W_ID);
        }

        public override bool Equals(object obj)
        {
            DistrictPkey that = obj as DistrictPkey;
            return that != null
                && this.D_ID == that.D_ID
                && this.D_W_ID == that.D_W_ID;
        }

        public override Copyable Copy()
        {
            DistrictPkey copy = new DistrictPkey();
            copy.SafeCopyFrom(this);
            return copy;
        }

        public override bool CopyFrom(Copyable copyable)
        {
            DistrictPkey that = copyable as DistrictPkey;
            if (that == null) return false;
            this.SafeCopyFrom(that);
            return true;
        }

        public void SafeCopyFrom(DistrictPkey that)
        {
            this.Set(that.D_ID, that.D_W_ID);
        }

        public void Set(uint D_ID, uint D_W_ID)
        {
            this.D_ID = D_ID;
            this.D_W_ID = D_W_ID;
        }
    }
    public class DistrictPayload : TpccTablePayload
    {
        public DistrictPayload() : base(TableType.DISTRICT) { }

        public override Copyable Copy()
        {
            DistrictPayload copy = new DistrictPayload();
            copy.SafeCopyFrom(this);
            return copy;
        }

        public override bool CopyFrom(Copyable copyable)
        {
            DistrictPayload that = copyable as DistrictPayload;
            if (that == null)
            {
                return false;
            }
            this.SafeCopyFrom(that);
            return true;
        }

        public void SafeCopyFrom(DistrictPayload that)
        {
            this.Set(
                that.D_NAME, that.D_STREET_1, that.D_STREET_2, that.D_CITY,
                that.D_STATE, that.D_ZIP, that.D_TAX, that.D_YTD,
                that.D_NEXT_O_ID);
        }

        public void Set(
            string D_NAME, string D_STREET_1, string D_STREET_2, string D_CITY,
            string D_STATE, string D_ZIP, double D_TAX, double D_YTD,
            uint D_NEXT_O_ID)
        {
            this.D_NAME = D_NAME;
            this.D_STREET_1 = D_STREET_1;
            this.D_STREET_2 = D_STREET_2;
            this.D_CITY = D_CITY;
            this.D_STATE = D_STATE;
            this.D_ZIP = D_ZIP;
            this.D_TAX = D_TAX;
            this.D_YTD = D_YTD;
            this.D_NEXT_O_ID = D_NEXT_O_ID;
        }

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

        public const int IdBits = 12;

        public uint C_ID;
        public uint C_D_ID;
        public uint C_W_ID;
        public override string ToString()
        {
            return "C-" + C_ID + "-" + C_D_ID + "-" + C_W_ID;
        }

        public override int GetHashCode()
        {
            return (int)(
                this.C_ID << WarehousePkey.IdBits + DistrictPkey.IdBits |
                this.C_D_ID << WarehousePkey.IdBits |
                this.C_W_ID);
        }

        public override bool Equals(object obj)
        {
            CustomerPkey that = obj as CustomerPkey;
            return that != null
                && this.C_ID == that.C_ID
                && this.C_D_ID == that.C_D_ID
                && this.C_W_ID == that.C_W_ID;
        }

        public override Copyable Copy()
        {
            CustomerPkey copy = new CustomerPkey();
            copy.SafeCopyFrom(this);
            return copy;
        }

        public override bool CopyFrom(Copyable copyable)
        {
            CustomerPkey that = copyable as CustomerPkey;
            if (that == null) return false;
            this.SafeCopyFrom(that);
            return true;
        }

        public void SafeCopyFrom(CustomerPkey that)
        {
            this.Set(that.C_ID, that.C_D_ID, that.C_W_ID);
        }

        public void Set(uint C_ID, uint C_D_ID, uint C_W_ID)
        {
            this.C_ID = C_ID;
            this.C_D_ID = C_D_ID;
            this.C_W_ID = C_W_ID;
        }
    }
    public class CustomerLastNameIndexKey : TpccTableKey
    {
        public CustomerLastNameIndexKey() : base(TableType.CUSTOMER_INDEX) { }

        public uint C_W_ID;
        public uint C_D_ID;
        public string C_LAST;

        public override int GetHashCode()
        {
            return (int)(
                C_D_ID << WarehousePkey.IdBits | C_W_ID) * 17 +
                C_LAST.GetHashCode();
        }
        public override bool Equals(object obj)
        {
            CustomerLastNameIndexKey that = obj as CustomerLastNameIndexKey;
            return that != null
                && this.C_W_ID == that.C_W_ID
                && this.C_D_ID == that.C_D_ID
                && this.C_LAST == that.C_LAST;
        }

        public override Copyable Copy()
        {
            CustomerLastNameIndexKey copy = new CustomerLastNameIndexKey();
            copy.SafeCopyFrom(this);
            return copy;
        }

        public override bool CopyFrom(Copyable copyable)
        {
            CustomerLastNameIndexKey that = copyable as CustomerLastNameIndexKey;
            if (that == null) return false;
            this.SafeCopyFrom(that);
            return true;
        }

        public void SafeCopyFrom(CustomerLastNameIndexKey that)
        {
            this.Set(that.C_W_ID, that.C_D_ID, that.C_LAST);
        }

        public void Set(uint C_W_ID, uint C_D_ID, string C_LAST)
        {
            this.C_W_ID = C_W_ID;
            this.C_D_ID = C_D_ID;
            this.C_LAST = C_LAST;
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
        public override Copyable Copy()
        {
            return this;
        }
        public override bool CopyFrom(Copyable that)
        {
            return false;
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

        public override Copyable Copy()
        {
            CustomerPayload copy = new CustomerPayload();
            copy.SafeCopyFrom(this);
            return copy;
        }

        public override bool CopyFrom(Copyable copyable)
        {
            CustomerPayload that = copyable as CustomerPayload;
            if (that == null) return false;
            this.SafeCopyFrom(that);
            return true;
        }

        public void SafeCopyFrom(CustomerPayload that)
        {
            this.Set(
                that.C_FIRST, that.C_MIDDLE, that.C_LAST, that.C_STREET_1,
                that.C_STREET_2, that.C_CITY, that.C_STATE, that.C_ZIP,
                that.C_PHONE, that.C_SINCE, that.C_CREDIT, that.C_CREDIT_LIM,
                that.C_DISCOUNT, that.C_BALANCE, that.C_YTD_PAYMENT,
                that.C_PAYMENT_CNT, that.C_DELIVERY_CNT, that.C_DATA);
        }

        public void Set(
            string C_FIRST, string C_MIDDLE, string C_LAST, string C_STREET_1,
            string C_STREET_2, string C_CITY, string C_STATE, string C_ZIP,
            string C_PHONE, string C_SINCE, string C_CREDIT,
            double C_CREDIT_LIM, double C_DISCOUNT, double C_BALANCE,
            double C_YTD_PAYMENT, uint C_PAYMENT_CNT, uint C_DELIVERY_CNT,
            string C_DATA)
        {
            this.C_FIRST = C_FIRST;
            this.C_MIDDLE = C_MIDDLE;
            this.C_LAST = C_LAST;
            this.C_STREET_1 = C_STREET_1;
            this.C_STREET_2 = C_STREET_2;
            this.C_CITY = C_CITY;
            this.C_STATE = C_STATE;
            this.C_ZIP = C_ZIP;
            this.C_PHONE = C_PHONE;
            this.C_SINCE = C_SINCE;
            this.C_CREDIT = C_CREDIT;
            this.C_CREDIT_LIM = C_CREDIT_LIM;
            this.C_DISCOUNT = C_DISCOUNT;
            this.C_BALANCE = C_BALANCE;
            this.C_YTD_PAYMENT = C_YTD_PAYMENT;
            this.C_PAYMENT_CNT = C_PAYMENT_CNT;
            this.C_DELIVERY_CNT = C_DELIVERY_CNT;
            this.C_DATA = C_DATA;
        }

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

    public class HistoryPKey : TpccTableKey, Copyable
    {
        static public HistoryPKey New()
        {
            return new HistoryPKey { GUID = HistoryPayload.GetHPkey() };
        }

        public HistoryPKey() : base(TableType.HISTORY) { }

        public override bool Equals(object obj)
        {
            HistoryPKey that = obj as HistoryPKey;
            return that != null && that.GUID == this.GUID;
        }

        public override int GetHashCode()
        {
            return this.GUID.GetHashCode();
        }

        public override Copyable Copy()
        {
            HistoryPKey copy = new HistoryPKey();
            copy.CopyFrom(this);
            return copy;
        }

        public override bool CopyFrom(Copyable copyable)
        {
            HistoryPKey that = copyable as HistoryPKey;
            if (that == null) return false;
            this.SafeCopyFrom(that);
            return true;
        }

        public void SafeCopyFrom(HistoryPKey that)
        {
            this.GUID = that.GUID;
        }

        public string GUID;
    }

    // HISTORY primary key is uuid
    public class HistoryPayload : TpccTablePayload
    {
        public HistoryPayload() : base(TableType.HISTORY) { }

        public override Copyable Copy()
        {
            return this;
        }

        public override bool CopyFrom(Copyable copyable)
        {
            return false;
        }

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
            return (int)(
                this.NO_O_ID << DistrictPkey.IdBits + WarehousePkey.IdBits |
                this.NO_D_ID << WarehousePkey.IdBits |
                this.NO_W_ID);
        }

        public override bool Equals(object obj)
        {
            NewOrderPkey that = obj as NewOrderPkey;
            return that != null
                && this.NO_O_ID == that.NO_O_ID
                && this.NO_D_ID == that.NO_D_ID
                && this.NO_W_ID == that.NO_W_ID;
        }

        public override Copyable Copy()
        {
            NewOrderPkey copy = new NewOrderPkey();
            copy.CopyFrom(this);
            return copy;
        }

        public override bool CopyFrom(Copyable copyable)
        {
            NewOrderPkey that = copyable as NewOrderPkey;
            if (that == null) return false;
            this.SafeCopyFrom(that);
            return true;
        }

        public void SafeCopyFrom(NewOrderPkey that)
        {
            this.Set(that.NO_O_ID, that.NO_D_ID, that.NO_W_ID);
        }

        public void Set(uint NO_O_ID, uint NO_D_ID, uint NO_W_ID)
        {
            this.NO_O_ID = NO_O_ID;
            this.NO_D_ID = NO_D_ID;
            this.NO_W_ID = NO_W_ID;
        }
    }
    public class NewOrderPayload : TpccTablePayload    // no use
    {
        public NewOrderPayload() : base(TableType.NEW_ORDER) { }

        public override Copyable Copy()
        {
            return this;
        }
        public override bool CopyFrom(Copyable copyable)
        {
            return copyable is NewOrderPayload;
        }

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

        public const int IdBits = 12;

        public uint O_ID;
        public uint O_D_ID;
        public uint O_W_ID;

        public override string ToString()
        {
            return "O-" + O_ID + "-" + O_D_ID + "-" + O_W_ID;
        }

        public override int GetHashCode()
        {
            return (int)(
                this.O_ID << WarehousePkey.IdBits + DistrictPkey.IdBits |
                this.O_D_ID << WarehousePkey.IdBits |
                this.O_W_ID);
        }

        public override bool Equals(object obj)
        {
            OrderPkey that = obj as OrderPkey;
            return that != null
                && this.O_ID == that.O_ID
                && this.O_D_ID == that.O_D_ID
                && this.O_W_ID == that.O_W_ID;
        }

        public override Copyable Copy()
        {
            OrderPkey copy = new OrderPkey();
            copy.SafeCopyFrom(this);
            return copy;
        }

        public override bool CopyFrom(Copyable copyable)
        {
            OrderPkey that = copyable as OrderPkey;
            if (that == null) return false;
            this.SafeCopyFrom(that);
            return true;
        }

        public void SafeCopyFrom(OrderPkey that)
        {
            this.Set(that.O_ID, that.O_D_ID, that.O_W_ID);
        }

        public void Set(uint O_ID, uint O_D_ID, uint O_W_ID)
        {
            this.O_ID = O_ID;
            this.O_D_ID = O_D_ID;
            this.O_W_ID = O_W_ID;
        }
    }
    public class OrderPayload : TpccTablePayload
    {
        public OrderPayload() : base(TableType.ORDERS) { }

        public override Copyable Copy()
        {
            OrderPayload copy = new OrderPayload();
            copy.SafeCopyFrom(this);
            return copy;
        }

        public override bool CopyFrom(Copyable copyable)
        {
            OrderPayload that = copyable as OrderPayload;
            if (that == null)
            {
                return false;
            }
            this.SafeCopyFrom(that);
            return true;
        }

        public void SafeCopyFrom(OrderPayload that)
        {
            this.Set(
                that.O_C_ID, that.O_ENTRY_D, that.O_CARRIER_ID,
                that.O_OL_CNT, that.O_ALL_LOCAL);
        }

        public void Set(
            uint O_C_ID, string O_ENTRY_D, uint O_CARRIER_ID, uint O_OL_CNT,
            uint O_ALL_LOCAL)
        {
            this.O_C_ID = O_C_ID;
            this.O_ENTRY_D = O_ENTRY_D;
            this.O_CARRIER_ID = O_CARRIER_ID;
            this.O_OL_CNT = O_OL_CNT;
            this.O_ALL_LOCAL = O_ALL_LOCAL;
        }

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
            return (int)(((
                OL_O_ID << DistrictPkey.IdBits + WarehousePkey.IdBits |
                OL_D_ID << WarehousePkey.IdBits |
                OL_W_ID) + 17) * 23 + OL_NUMBER);
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

        public override Copyable Copy()
        {
            OrderLinePkey copy = new OrderLinePkey();
            copy.CopyFrom(this);
            return copy;
        }

        public override bool CopyFrom(Copyable copyable)
        {
            OrderLinePkey that = copyable as OrderLinePkey;
            if (that == null) return false;
            this.SafeCopyFrom(that);
            return true;
        }

        public void SafeCopyFrom(OrderLinePkey that)
        {
            this.Set(that.OL_O_ID, that.OL_D_ID, that.OL_W_ID, that.OL_NUMBER);
        }

        public void Set(uint OL_O_ID, uint OL_D_ID, uint OL_W_ID, uint OL_NUMBER)
        {
            this.OL_O_ID = OL_O_ID;
            this.OL_D_ID = OL_D_ID;
            this.OL_W_ID = OL_W_ID;
            this.OL_NUMBER = OL_NUMBER;
        }
    }
    public class OrderLinePayload : TpccTablePayload
    {
        public OrderLinePayload() : base(TableType.ORDER_LINE) { }

        public override Copyable Copy()
        {
            OrderLinePayload copy = new OrderLinePayload();
            copy.SafeCopyFrom(this);
            return copy;
        }

        public override bool CopyFrom(Copyable copyable)
        {
            OrderLinePayload that = copyable as OrderLinePayload;
            if (that == null) return false;
            this.SafeCopyFrom(that);
            return true;
        }

        public void SafeCopyFrom(OrderLinePayload that)
        {
            this.Set(
                that.OL_I_ID, that.OL_SUPPLY_W_ID, that.OL_DELIVERY_D,
                that.OL_QUANTITY, that.OL_AMOUNT, that.OL_DIST_INFO);
        }

        public void Set(
            uint OL_I_ID, uint OL_SUPPLY_W_ID, string OL_DELIVERY_D,
            uint OL_QUANTITY, double OL_AMOUNT, string OL_DIST_INFO)
        {
            this.OL_I_ID = OL_I_ID;
            this.OL_SUPPLY_W_ID = OL_SUPPLY_W_ID;
            this.OL_DELIVERY_D = OL_DELIVERY_D;
            this.OL_QUANTITY = OL_QUANTITY;
            this.OL_AMOUNT = OL_AMOUNT;
            this.OL_DIST_INFO = OL_DIST_INFO;
        }

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
            return (int)this.I_ID;
        }

        public override bool Equals(object obj)
        {
            ItemPkey that = obj as ItemPkey;
            return that != null && this.I_ID == that.I_ID;
        }

        public override Copyable Copy()
        {
            ItemPkey copy = new ItemPkey();
            copy.SafeCopyFrom(this);
            return copy;
        }

        public override bool CopyFrom(Copyable copyable)
        {
            ItemPkey that = copyable as ItemPkey;
            if (that == null) return false;
            this.SafeCopyFrom(that);
            return true;
        }

        public void SafeCopyFrom(ItemPkey that)
        {
            this.Set(that.I_ID);
        }

        public void Set(uint I_ID)
        {
            this.I_ID = I_ID;
        }
    }
    public class ItemPayload : TpccTablePayload
    {
        public ItemPayload() : base(TableType.ITEM) { }

        public override Copyable Copy()
        {
            ItemPayload copy = new ItemPayload();
            copy.SafeCopyFrom(this);
            return copy;
        }

        public override bool CopyFrom(Copyable copyable)
        {
            ItemPayload that = copyable as ItemPayload;
            if (that == null)
            {
                return false;
            }
            this.SafeCopyFrom(that);
            return true;
        }

        public void SafeCopyFrom(ItemPayload that)
        {
            this.Set(that.I_IM_ID, that.I_NAME, that.I_PRICE, that.I_DATA);
        }

        public void Set(
            uint I_IM_ID, string I_NAME, double I_PRICE, string I_DATA)
        {
            this.I_IM_ID = I_IM_ID;
            this.I_NAME = I_NAME;
            this.I_PRICE = I_PRICE;
            this.I_DATA = I_DATA;
        }
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
            return (int)(this.S_I_ID << WarehousePkey.IdBits | this.S_W_ID);
        }

        public override bool Equals(object obj)
        {
            StockPkey that = obj as StockPkey;
            return that != null
                && this.S_I_ID == that.S_I_ID
                && this.S_W_ID == that.S_W_ID;
        }

        public override Copyable Copy()
        {
            StockPkey copy = new StockPkey();
            copy.CopyFrom(this);
            return copy;
        }

        public override bool CopyFrom(Copyable copyable)
        {
            StockPkey that = copyable as StockPkey;
            if (that == null) return false;
            this.SafeCopyFrom(that);
            return true;
        }

        public void SafeCopyFrom(StockPkey that)
        {
            this.Set(that.S_I_ID, that.S_W_ID);
        }

        public void Set(uint S_I_ID, uint S_W_ID)
        {
            this.S_I_ID = S_I_ID;
            this.S_W_ID = S_W_ID;
        }

    }
    public class StockPayload : TpccTablePayload
    {
        public StockPayload() : base(TableType.STOCK) { }

        public override Copyable Copy()
        {
            StockPayload copy = new StockPayload();
            copy.SafeCopyFrom(this);
            return copy;
        }

        public override bool CopyFrom(Copyable copyable)
        {
            StockPayload that = copyable as StockPayload;
            if (that == null)
            {
                return false;
            }
            this.SafeCopyFrom(that);
            return true;
        }

        public void SafeCopyFrom(StockPayload that)
        {
            this.Set(
                that.S_QUANTITY, that.S_DIST_01, that.S_DIST_02, that.S_DIST_03,
                that.S_DIST_04, that.S_DIST_05, that.S_DIST_06, that.S_DIST_07,
                that.S_DIST_08, that.S_DIST_09, that.S_DIST_10, that.S_YTD,
                that.S_ORDER_CNT, that.S_REMOTE_CNT, that.S_DATA);
        }

        public void Set(
            int S_QUANTITY, string S_DIST_01, string S_DIST_02,
            string S_DIST_03, string S_DIST_04, string S_DIST_05,
            string S_DIST_06, string S_DIST_07, string S_DIST_08,
            string S_DIST_09, string S_DIST_10, uint S_YTD,
            uint S_ORDER_CNT, uint S_REMOTE_CNT, string S_DATA)
        {
            this.S_QUANTITY = S_QUANTITY;
            this.S_DIST_01 = S_DIST_01;
            this.S_DIST_02 = S_DIST_02;
            this.S_DIST_03 = S_DIST_03;
            this.S_DIST_04 = S_DIST_04;
            this.S_DIST_05 = S_DIST_05;
            this.S_DIST_06 = S_DIST_06;
            this.S_DIST_07 = S_DIST_07;
            this.S_DIST_08 = S_DIST_08;
            this.S_DIST_09 = S_DIST_09;
            this.S_DIST_10 = S_DIST_10;
            this.S_YTD = S_YTD;
            this.S_ORDER_CNT = S_ORDER_CNT;
            this.S_REMOTE_CNT = S_REMOTE_CNT;
            this.S_DATA = S_DATA;
        }

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
