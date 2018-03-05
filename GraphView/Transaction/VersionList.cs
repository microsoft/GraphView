
using ServiceStack;

namespace GraphView.Transaction
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Threading;

    internal class VersionNode
    {
        public volatile VersionEntry versionEntry;
        public volatile NextNode nextNode;

        public VersionNode(VersionEntry versionEntry, NextNode nextNode = null)
        {
            this.versionEntry = versionEntry;
            this.nextNode = nextNode;
        }
    }

    internal class NextNode
    {
        private readonly VersionNode next;
        /// The highest 4 bits represent a 'hold tag'.
        /// The lowest 4 bits represent a 'delete tag'.
        private Byte tag;

        public VersionNode Next
        {
            get
            {
                return this.next;
            }
        }

        public Byte Tag
        {
            get
            {
                return this.tag;
            }
            set
            {
                this.tag = value;
            }
        }

        public NextNode(VersionNode node, Byte tag)
        {
            this.next = node;
            this.tag = tag;
        }
    }

    internal class VersionList : IEnumerable<VersionEntry>
    {
        private VersionNode head;

        public VersionList()
        {
            this.head = new VersionNode(new VersionEntry(false, long.MaxValue, false, long.MaxValue, null, null),
                                        new NextNode(null, 0x00));
        }

        public void PushFront(VersionEntry versionEntry)
        {
            VersionNode newNode = new VersionNode(versionEntry);

            while (true)
            {
                VersionNode oldHead = this.head;
                //check the head node's tag is 0xF0 or not
                if ((this.head.nextNode.Tag & 0x0F).Equals(0))
                {
                    newNode.nextNode = new NextNode(this.head, 0x00);                   
                    if (oldHead == Interlocked.CompareExchange<VersionNode>(ref this.head, newNode, oldHead))
                    {
                        return;
                    }
                }
            }
        }

        //Note:
        //A transaction can only delete a node created by itself.
        //It is not possible that two thread what to delete the same node at the same time.
        public void DeleteNode(object recordKey, long versionKey)
        {
            while (true)
            {
                VersionNode current = this.head;
                VersionNode previous = null;

                while (current != null && current.versionEntry.Record != null)
                {
                    //try to find the version to be deleted
                    if (current.versionEntry.RecordKey == recordKey && current.versionEntry.VersionKey == versionKey)
                    {
                        NextNode currentOldNextNode = current.nextNode;
                        //(1) try to set the current node's tag from 0x00 to 0xFF
                        if ((current.nextNode.Tag & 0xFF).Equals(0))
                        {
                            if (currentOldNextNode == Interlocked.CompareExchange<NextNode>(
                                    ref current.nextNode, 
                                    new NextNode(current.nextNode.Next, 0xFF),
                                    currentOldNextNode))
                            {
                                //(1) success
                                //(2) try to set the next node's tag from 0xX0 to 0xF0
                                VersionNode next = current.nextNode.Next;
                                NextNode nextOldNextNode = next.nextNode;
                                if ((next.nextNode.Tag & 0xF0).Equals(0))
                                {
                                    if (nextOldNextNode == Interlocked.CompareExchange<NextNode>(
                                            ref next.nextNode,
                                            new NextNode(next.nextNode.Next, 0xF0), 
                                            nextOldNextNode))
                                    {
                                        //(2) success
                                        //(3) try to set the previous node's from 0xX0 to 0xX0 and change the pointer
                                        if (previous != null)
                                        {
                                            NextNode previousOldNextNode = previous.nextNode;
                                            if ((previous.nextNode.Tag & 0x0F).Equals(0))
                                            {
                                                if (previousOldNextNode != Interlocked.CompareExchange<NextNode>(
                                                        ref previous.nextNode,
                                                        new NextNode(current.nextNode.Next, previous.nextNode.Tag),
                                                        previousOldNextNode))
                                                {
                                                    //(3) failed
                                                    //change the next node's Tag from 0xF0 back to 0xX0 (undo (2))
                                                    next.nextNode.Tag = nextOldNextNode.Tag;
                                                    //change the current node's Tag from 0xFF back to 0x00 (undo (1))
                                                    current.nextNode.Tag = 0x00;
                                                    break;
                                                }
                                            }
                                        }
                                        //(3) success
                                        //change the next node's Tag from 0xF0 back to 0xX0 (undo (2))
                                        next.nextNode.Tag = nextOldNextNode.Tag;
                                        return;
                                    }
                                    else
                                    {
                                        //(2) failed
                                        //change the current node's Tag from 0xFF back to 0x00 (undo (1))
                                        current.nextNode.Tag = 0x00;
                                        break;
                                    }
                                }
                            }
                            else
                            {
                                //(1) failed
                                break;
                            }
                        }
                    }
                    previous = current;
                    current = current.nextNode.Next;
                }
            }
        }

        public bool ChangeNodeValue(object recordKey, long versionKey, VersionEntry toBeChangedVersion, VersionEntry newVersion)
        {
            VersionNode node = this.head;
            while (node != null && node.versionEntry.Record != null)
            {
                //try to find the old version
                if (node.versionEntry.RecordKey == recordKey && node.versionEntry.VersionKey == versionKey)
                {
                    VersionEntry oldVersion = node.versionEntry;
                    if (toBeChangedVersion.ContentEqual(oldVersion))
                    {
                        return oldVersion == Interlocked.CompareExchange<VersionEntry>(ref node.versionEntry, newVersion, oldVersion);
                    }
                    else
                    {
                        return false;
                    }
                }
                node = node.nextNode.Next;
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
