using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.Transaction
{
    using System.Threading;

    internal class VersionNode
    {
        public VersionEntry VersionEntry;
        // TODO: confirm whether it's a reference variable
        public VersionNode Next;
    }

    internal class VersionList
    {
        private VersionNode head;

        public VersionList()
        {
            this.head = new VersionNode();
            head.VersionEntry = new VersionEntry(false, long.MaxValue, false, long.MaxValue, null);
            head.Next = null;
        }

        public void PushFront(VersionEntry versionEntry)
        {
            VersionNode newNode = new VersionNode();
            newNode.VersionEntry = versionEntry;

            do
            {
                newNode.Next = this.head;
            }
            while (newNode.Next != Interlocked.CompareExchange(ref this.head, newNode, newNode.Next));
        }

        public bool ChangeNodeValue(VersionEntry oldVersion, VersionEntry newVersion)
        {
            VersionNode node = this.head;
            while (node != null && node.VersionEntry.Record != null)
            {
                // try to find the old version
                if (node.VersionEntry == oldVersion)
                {
                    return oldVersion == Interlocked.CompareExchange(ref node.VersionEntry, newVersion, oldVersion);
                }
                node = node.Next;
            }

            return false;
        }

        public IList<VersionEntry> ToList()
        {
            IList<VersionEntry> versionList = new List<VersionEntry>();
            VersionNode node = this.head;
            while (node != null && node.VersionEntry.Record != null)
            {
                versionList.Add(node.VersionEntry);
                node = node.Next;
            }

            return versionList;
        }
    }
}
