
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

        //Note:
        //A transaction can only delete a node created by itself.
        //It is not possible that two thread what to delete the same node at the same time.
        public void DeleteNode(object recordKey, long versionKey)
        {
            VersionNode current = this.head;
            VersionNode previous = null;
            while (current != null && current.versionEntry.Record != null)
            {
                //try to find the version to be deleted
                if (current.versionEntry.RecordKey == recordKey && current.versionEntry.VersionKey == versionKey)
                {
                    if (previous == null)
                    {
                        //the node is the head node
                        this.head = head.next;
                    }
                    else
                    {
                        Interlocked.Exchange(ref previous.next, current.next);
                    }

                    return;
                }
                previous = current;
                current = current.next;
            }
        }

        //Todo: Redesign the parameter.
        //This method and the corresponding interface UpdateAndUploadVersion() need discussion.
        //Still need more info about the old version. 
        //This method can not work correctly at this time.
        public bool ChangeNodeValue(object recordKey, long versionKey, VersionEntry newVersion)
        {
            VersionNode node = this.head;
            while (node != null && node.versionEntry.Record != null)
            {
                //try to find the old version
                if (node.versionEntry.RecordKey == recordKey && node.versionEntry.VersionKey == versionKey)
                {
                    VersionEntry oldVersion = node.versionEntry;
                    //Todo:
                    return oldVersion == Interlocked.CompareExchange(ref node.versionEntry, newVersion, oldVersion);
                }
                node = node.next;
            }

            return false;
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
