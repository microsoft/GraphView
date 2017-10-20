using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace GraphView
{
    internal abstract class DeltaField
    {
        protected DeltaType Type;
        protected GraphViewCommand Command;

        protected DeltaField(DeltaType type, GraphViewCommand command)
        {
            this.Type = type;
            this.Command = command;
        }

        public abstract void Upload();
    }

    internal class DeltaAddV : DeltaField
    {
        private JObject VertexObject;

        public DeltaAddV(GraphViewCommand command, JObject vertexObject) : base(DeltaType.AddV, command)
        {
            this.VertexObject = vertexObject;
        }

        public override void Upload()
        {
            try
            {
                this.Command.Connection.CreateDocumentAsync(this.VertexObject, this.Command).Wait();
            }
            catch (AggregateException ex)
            {
                throw new GraphViewException("Error when uploading the vertex", ex.InnerException);
            }
        }
    }

    internal class DeltaDropV : DeltaField
    {
        private string VertexId;
        private JObject VertexObject;

        public DeltaDropV(GraphViewCommand command, string vertexId, JObject vertexObject)
            : base(DeltaType.DropV, command)
        {
            this.VertexId = vertexId;
            this.VertexObject = vertexObject;
        }

        public override void Upload()
        {
            this.Command.Connection.ReplaceOrDeleteDocumentAsync(this.VertexId, null,
                this.Command.Connection.GetDocumentPartition(this.VertexObject), this.Command).Wait();
        }
    }

    internal class DeltaUpdateV : DeltaField
    {
        private string VertexId;
        private JObject VertexObject;

        public DeltaUpdateV(GraphViewCommand command, string vertexId, JObject vertexObject)
            : base(DeltaType.UpdateV, command)
        {
            this.VertexId = vertexId;
            this.VertexObject = vertexObject;
        }

        public override void Upload()
        {
            this.Command.Connection.ReplaceOrDeleteDocumentAsync(this.VertexId, this.VertexObject,
                this.Command.Connection.GetDocumentPartition(this.VertexObject), this.Command).Wait();
        }
    }

    internal class DeltaAddE : DeltaField
    {
        private VertexField VertexField;
        private JObject VertexObject;
        private JObject EdgeObject;
        private bool IsReverse;

        public DeltaAddE(GraphViewCommand command, VertexField vertexField, JObject vertexObject, JObject edgeObject, bool isReverse)
            : base(DeltaType.AddE, command)
        {
            this.Command = command;
            this.VertexField = vertexField;
            this.VertexObject = vertexObject;
            this.EdgeObject = edgeObject;
            this.IsReverse = isReverse;
        }

        public override void Upload()
        {
            string DocId;
            EdgeDocumentHelper.InsertEdgeObjectInternal(this.Command, this.VertexObject, this.VertexField,
                this.EdgeObject, this.IsReverse, out DocId);
        }

    }

    internal class DeltaDropE : DeltaField
    {
        private string SrcId;
        private string EdgeId;
        private JObject SrcVertexObject;
        private bool SrcViaGraphAPI;
        private JObject SinkVertexObject;
        private bool SinkViaGraphAPI;

        public DeltaDropE(GraphViewCommand command, string srcId, string edgeId, VertexField srcVertexField, VertexField sinkVertexField)
            : base(DeltaType.DropE, command)
        {
            this.SrcId = srcId;
            this.EdgeId = edgeId;
            this.SrcVertexObject = srcVertexField.VertexJObject;
            this.SrcViaGraphAPI = srcVertexField.ViaGraphAPI;
            this.SinkVertexObject = sinkVertexField.VertexJObject;
            this.SinkViaGraphAPI = sinkVertexField.ViaGraphAPI;
        }

        public override void Upload()
        {
            JObject srcEdgeObject;
            string srcEdgeDocId;
            EdgeDocumentHelper.FindEdgeBySourceAndEdgeId(
                this.Command, this.SrcVertexObject, this.SrcId, this.EdgeId, false,
                out srcEdgeObject, out srcEdgeDocId);
            if (srcEdgeObject == null)
            {
                return;
            }

            string sinkId = (string)srcEdgeObject[DocumentDBKeywords.KW_EDGE_SINKV];
            string sinkEdgeDocId = null;
            if (this.Command.Connection.UseReverseEdges)
            {
                if (!string.Equals(sinkId, this.SrcId))
                {
                    JObject dummySinkEdgeObject;
                    EdgeDocumentHelper.FindEdgeBySourceAndEdgeId(
                        this.Command, this.SinkVertexObject, this.SrcId, this.EdgeId, true,
                        out dummySinkEdgeObject, out sinkEdgeDocId);
                }
                else
                {
                    sinkEdgeDocId = srcEdgeDocId;
                }
            }

            Dictionary<string, Tuple<JObject, string>> uploadDocuments = new Dictionary<string, Tuple<JObject, string>>();
            EdgeDocumentHelper.RemoveEdge(uploadDocuments, this.Command, srcEdgeDocId,
                this.SrcVertexObject, this.SrcViaGraphAPI, false, this.SrcId, this.EdgeId);
            if (this.Command.Connection.UseReverseEdges)
            {
                EdgeDocumentHelper.RemoveEdge(uploadDocuments, this.Command, sinkEdgeDocId,
                    this.SinkVertexObject, this.SinkViaGraphAPI, true, this.SrcId, this.EdgeId);
            }
            this.Command.Connection.ReplaceOrDeleteDocumentsAsync(uploadDocuments, this.Command).Wait();
        }
    }

    internal class DeltaUpdateE : DeltaField
    {
        private string SrcVertexId;
        private string EdgeId;
        private JObject SrcVertexObject;
        private JObject SinkVertexObject;

        public DeltaUpdateE(GraphViewCommand command, string srcVertexId, string edgeId, JObject srcVertexObject, JObject sinkVertexObject)
            : base(DeltaType.UpdateE, command)
        {
            this.SrcVertexId = srcVertexId;
            this.EdgeId = edgeId;
            this.SrcVertexObject = srcVertexObject;
            this.SinkVertexObject = sinkVertexObject;
        }

        public override void Upload()
        {
            string outEdgeDocId;
            JObject outEdgeObject;
            EdgeDocumentHelper.FindEdgeBySourceAndEdgeId(
                this.Command, this.SrcVertexObject, this.SrcVertexId, this.EdgeId, false,
                out outEdgeObject, out outEdgeDocId);
            if (outEdgeObject == null)
            {
                Debug.WriteLine($"[DeltaUpdateE] The edge does not exist: vertexId = {this.SrcVertexId}, edgeId = {this.EdgeId}");
                return;
            }

            string inEdgeDocId = null;
            JObject inEdgeObject = null;

            if (this.Command.Connection.UseReverseEdges)
            {
                EdgeDocumentHelper.FindEdgeBySourceAndEdgeId(
                    this.Command, this.SinkVertexObject, this.SrcVertexId, this.EdgeId, true,
                    out inEdgeObject, out inEdgeDocId);
            }

            EdgeDocumentHelper.UpdateEdgeProperty(this.Command, this.SrcVertexObject, outEdgeDocId, false, outEdgeObject);
            if (this.Command.Connection.UseReverseEdges)
            {
                EdgeDocumentHelper.UpdateEdgeProperty(this.Command, this.SinkVertexObject, inEdgeDocId, true, inEdgeObject);
            }
        }
    }


    internal enum DeltaType
    {
        AddV,
        DropV,
        UpdateV,
        AddE,
        DropE,
        UpdateE
    };
}
