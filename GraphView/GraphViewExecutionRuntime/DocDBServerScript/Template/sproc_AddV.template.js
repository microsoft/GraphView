
/**
 * 
 * @param {string} rid 
 * @returns {} 
 */
function AddV(vertexObject) {

    "use strict";

    "include Common.snippet";
    
    ASSERT(vertexObject && typeof (vertexObject) === "object",
           "[AddV] vertexObject should be a json object");
    ASSERT(typeof (vertexObject.id) === "string" && vertexObject.id.length > 0,
           "[AddV] vertexObject.id should be a non-empty string");
    ASSERT(typeof (vertexObject._partition) === "string" && vertexObject._partition.length > 0,
           "[AddV] vertexObject._partition should be a non-empty string");
    ASSERT(vertexObject.id === vertexObject._partition,
           "[AddV] Expecting id(\"" + vertexObject.id + "\") equivalent to _partition(\"" + vertexObject._partition + "\")");

    var isAccepted = collection.createDocument(
        collection.getSelfLink(), // collectionLink: string
        vertexObject, // body: object
        { }, // options: CreateOptionsRequestCallback
        function(error, resources, options) {
            if (error) {
                ERROR(error);
            }
        } // callback: RequestCallback
    );

    isAccepted || ERROR(Status.NotAccepted, "[AddV] createDocument() not accepted");

    SUCCESS();
}
