
namespace GraphView.Transaction
{
    using System.Collections;
    using System.Collections.Generic;

    internal class VersionListEnumerator : IEnumerator<VersionEntry>
    {
        private VersionNode head;
        private VersionNode currentNode;

        public VersionListEnumerator(VersionNode head)
        {
            this.head = head;
            this.currentNode = head;
        }

        public VersionEntry Current => this.currentNode.versionEntry;

        object IEnumerator.Current => this.Current;

        public void Dispose()
        {
            return;
        }

        public bool MoveNext()
        {
            this.currentNode = this.currentNode.next;
            return this.currentNode == null;
        }

        public void Reset()
        {
            this.currentNode = this.head;
        }
    }
}
