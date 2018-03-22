
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

        public bool PushFront(VersionEntry versionEntry)
        {
            throw new NotImplementedException();
        }

        //Note:
        //A transaction can only delete a node created by itself.
        //It is not possible that two thread what to delete the same node at the same time.
        public bool DeleteNode(object recordKey, long versionKey)
        {
            throw new NotImplementedException();
        }

        public bool ChangeNodeValue(object recordKey, long versionKey, VersionEntry toBeChangedVersion, VersionEntry newVersion)
        {
            throw new NotImplementedException();
        }
    }
}
