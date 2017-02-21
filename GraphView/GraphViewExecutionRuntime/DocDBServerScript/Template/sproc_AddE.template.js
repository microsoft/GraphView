
function AddE(srcVertexId, edgeObject, isReverse) {

    "use strict";

    "include Common.snippet";
    "include RetrieveDocument.snippet";

    RetrieveDocumentById(srcVertexId, callback);
}
