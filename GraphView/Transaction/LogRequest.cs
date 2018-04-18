using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.Transaction
{
	internal enum LogRequestType
	{
		WriteTxLog,
		WriteVersionLog
	}

	internal class LogRequest
	{
		internal string TableId { get; private set; }
		internal long TxId { get; private set; }
		internal LogVersionEntry LogEntry { get; private set; } 
		internal bool Finished { get; set; } = false;
		internal bool IsSuccess { get; set; } = false;
		internal LogRequestType Type { get; private set; }
		
		/// <summary>
		/// For write committed transaction's txId to log.
		/// </summary>
		public LogRequest(long txId, LogRequestType type)
		{
			this.TxId = txId;
			this.Type = type;
		}

		/// <summary>
		/// For write committed version to log.
		/// </summary>
		public LogRequest(string tableId, long txId, LogVersionEntry logEntry, LogRequestType type)
		{
			this.TableId = tableId;
			this.TxId = txId;
			this.LogEntry = logEntry;
			this.Type = type;
		}
	}
}
