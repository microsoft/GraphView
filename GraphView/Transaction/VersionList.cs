
using ServiceStack;

namespace GraphView.Transaction
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Threading;

    internal class VersionNode
    {
        internal VersionEntry versionEntry;
        internal VersionNextPointer nextPointer;

        public VersionNode(VersionEntry versionEntry, VersionNextPointer nextNode = null)
        {
            this.versionEntry = versionEntry;
            this.nextPointer = nextNode;
        }

        internal VersionEntry VersionEntry
        {
            get
            {
                return Volatile.Read<VersionEntry>(ref this.versionEntry);
            }
        }

        internal VersionNextPointer NextPointer
        {
            get
            {
                return Volatile.Read<VersionNextPointer>(ref this.nextPointer);
            }
        }

        internal VersionNode NextNode
        {
            get
            {
                return Volatile.Read<VersionNextPointer>(ref this.nextPointer).PointingNode;
            }
        }

        internal byte State
        {
            get
            {
                return Volatile.Read<VersionNextPointer>(ref this.nextPointer).HostState;
            }
            set
            {
                Volatile.Write(ref this.nextPointer.tag, value);
            }
        }
    }

    internal class VersionNextPointer
    {
        private readonly VersionNode nextNode;
        /// The highest 4 bits represent a 'hold tag'.
        /// The lowest 4 bits represent a 'delete tag'.
        internal byte tag;

        public VersionNode PointingNode
        {
            get
            {
                return this.nextNode;
            }
        }

        public byte HostState
        {
            get
            {
                return this.tag;
            }
        }

        public VersionNextPointer(VersionNode node, byte tag)
        {
            this.nextNode = node;
            this.tag = tag;
        }
    }

    internal class VersionList
    {
        private VersionNode head;

        public VersionList()
        {
            this.head = new VersionNode(new VersionEntry(false, long.MaxValue, false, long.MaxValue, null, null),
                                        new VersionNextPointer(null, 0x00));
        }

        internal VersionNode Head
        {
            get
            {
                return Volatile.Read<VersionNode>(ref this.head);
            }
        }

        public void PushFront(VersionEntry versionEntry)
        {
            VersionNode newNode = new VersionNode(versionEntry);

            while (true)
            {
                VersionNode oldHead = this.Head;
                //check the head node's tag is 0xF0 or not
                if ((this.Head.State & 0x0F).Equals(0))
                {
                    newNode.nextPointer = new VersionNextPointer(this.Head, 0x00);                   
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
        public bool DeleteNode(object recordKey, long versionKey)
        {
            while (true)
            {
                VersionNode current = this.Head;
                VersionNode previous = null;

                do
                {
                    if (current.NextNode == null)
                    {
                        //arrive at the end of the list, and can not find the to be deleted version
                        return false;
                    }

                    //if current node is being deleted, rescan the list from head.
                    if ((current.State & 0x0F).Equals(0x0F))
                    {
                        break;
                    }

                    //try to find the version to be deleted
                    if (current.VersionEntry.RecordKey == recordKey && current.VersionEntry.VersionKey == versionKey)
                    {
                        VersionNextPointer currentOldNextNode = current.NextPointer;
                        //(1) try to set the current node's tag from 0x00 to 0xFF
                        if ((current.State & 0xFF).Equals(0))
                        {
                            if (currentOldNextNode == Interlocked.CompareExchange<VersionNextPointer>(
                                    ref current.nextPointer,
                                    new VersionNextPointer(current.NextNode, 0xFF),
                                    currentOldNextNode))
                            {
                                //(1) success
                                //(2) try to set the next node's tag from 0xX0 to 0xF0
                                VersionNode next = current.NextNode;
                                VersionNextPointer nextOldNextNode = next.NextPointer;
                                byte nextOldHostState = nextOldNextNode.HostState;
                                if ((next.State & 0xF0).Equals(0))
                                {
                                    if (nextOldNextNode == Interlocked.CompareExchange<VersionNextPointer>(
                                            ref next.nextPointer,
                                            new VersionNextPointer(next.NextNode, 0xF0),
                                            nextOldNextNode))
                                    {
                                        //(2) success
                                        //(3) try to set the previous node's from 0xX0 to 0xX0 and change the pointer
                                        if (previous != null)
                                        {
                                            VersionNextPointer previousOldNextNode = previous.NextPointer;
                                            if ((previous.State & 0x0F).Equals(0))
                                            {
                                                if (previousOldNextNode !=
                                                    Interlocked.CompareExchange<VersionNextPointer>(
                                                        ref previous.nextPointer,
                                                        new VersionNextPointer(current.NextNode, previous.State),
                                                        previousOldNextNode))
                                                {
                                                    //(3) failed
                                                    //change the next node's Tag from 0xF0 back to 0xX0 (undo (2))
                                                    next.State = nextOldHostState;
                                                    //change the current node's Tag from 0xFF back to 0x00 (undo (1))
                                                    current.State = 0x00;
                                                    break;
                                                }
                                            }
                                        }

                                        //(3) success
                                        //change the next node's Tag from 0xF0 back to 0xX0 (undo (2))
                                        next.State = nextOldHostState;
                                        return true;
                                    }
                                    else
                                    {
                                        //(2) failed
                                        //change the current node's Tag from 0xFF back to 0x00 (undo (1))
                                        current.State = 0x00;
                                        break;
                                    }
                                }
                                else
                                {
                                    //the next node's Tag is not 0xX0
                                    //change the current node's Tag from 0xFF back to 0x00 (undo (1))
                                    current.State = 0x00;
                                    break;
                                }
                            }
                            else
                            {
                                //(1) failed
                                break;
                            }
                        }
                        else
                        {
                            //current node's Tag is not 0x00, can not perform delete
                            break;
                        }
                    }
                    previous = current;
                    current = current.NextNode;
                } while (true);
            }
        }

        public bool ChangeNodeValue(object recordKey, long versionKey, VersionEntry toBeChangedVersion, VersionEntry newVersion)
        {
            while (true)
            {
                VersionNode node = this.Head;

                do
                {
                    if (node.NextNode == null)
                    {
                        //arrive at the end of the list, and can not find the to be changed version
                        return false;
                    }

                    //if current node is being deleted, rescan the list from head.
                    if ((node.State & 0x0F).Equals(0x0F))
                    {
                        break;
                    }

                    //try to find the old version
                    if (node.VersionEntry.RecordKey == recordKey && node.VersionEntry.VersionKey == versionKey)
                    {
                        VersionEntry oldVersion = node.VersionEntry;
                        if (toBeChangedVersion.ContentEqual(oldVersion))
                        {
                            return oldVersion ==
                                   Interlocked.CompareExchange<VersionEntry>(ref node.versionEntry, newVersion,
                                       oldVersion);
                        }
                        else
                        {
                            return false;
                        }
                    }
                    else
                    {
                        //go to next node
                        node = node.NextNode;
                    }
                } while (true);
            }
        }
    }
}
