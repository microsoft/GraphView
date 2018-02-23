
namespace GraphView.Transaction
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Threading;

    internal class VersionNode
    {
        public VersionEntry versionEntry;
        public VersionNode next;

        public VersionNode(VersionEntry versionEntry)
        {
            this.versionEntry = versionEntry;
            this.next = null;
        }
    }

    internal class VersionList : IEnumerable<VersionEntry>
    {
        private VersionNode head;

        public VersionList()
        {
            this.head = new VersionNode(new VersionEntry(false, long.MaxValue, false, long.MaxValue, null, null));
        }

        public void PushFront(VersionEntry versionEntry)
        {
            VersionNode newNode = new VersionNode(versionEntry);

            do
            {
                newNode.next = this.head;
            }
            while (newNode.next != Interlocked.CompareExchange(ref this.head, newNode, newNode.next));
        }

        public bool ChangeNodeValue(VersionEntry oldVersion, VersionEntry newVersion)
        {
            VersionNode node = this.head;
            while (node != null && node.versionEntry.Record != null)
            {
                // try to find the old version
                if (node.versionEntry == oldVersion)
                {
                    return oldVersion == Interlocked.CompareExchange(ref node.versionEntry, newVersion, oldVersion);
                }
                node = node.next;
            }

            return false;
        }

        public IEnumerator<VersionEntry> GetEnumerator()
        {
            return new VersionListEnumerator(this.head);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
    }
}
