
function AddE(srcVertexId, sinkVertexId, edgeObject, isReverse) {

    "use strict";
    "include Common.snippet";

    "include RetrieveDocument.snippet";

    RetrieveDocumentById(srcVertexId, retrvCallback);

    function retrvCallback(error, resources, options) {
        if (error) {
            ERROR(error);
        }

        SUCCESS(resources);
    }
}
