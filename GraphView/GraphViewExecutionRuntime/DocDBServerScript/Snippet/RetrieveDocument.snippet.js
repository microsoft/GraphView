
function RetrieveDocumentById(id, callback) {
    ASSERT(id && typeof id === "string");
    ASSERT(callback && typeof callback === "function");

    var isAccepted = collection.queryDocuments(
        collection.getSelfLink(), // collectionLink: string
        "SELECT * FROM doc WHERE doc['id'] = '" + id + "'", // filterQuery: string|object
        { enableScan: false, enableLowPrecisionOrderBy: true }, // options: FeedOptions
        function(error, resources, options) { // callback: FeedCallback
            if (error) {
                callback(error);
            }
            else {
                callback(error, resources, options);
            }
        }
    );
    if (!isAccepted) {
        throw new Error("[RetrieveDocumentById] not accepted");
    }
}

function RetrieveDocumentsByIds(ids, callback) {
    ASSERT(ids && typeof ids === "object");
    ASSERT(callback && typeof callback === "function");

    var strIds = "'" + ids.join("', '") + "'";
    var isAccepted = collection.queryDocuments(
        collection.getSelfLink(), // collectionLink: string
        "SELECT * FROM doc WHERE doc['id'] IN (" + strIds + ")", // filterQuery: string|object
        { enableScan: false, enableLowPrecisionOrderBy: true }, // options: FeedOptions
        function(error, resources, options) { // callback: FeedCallback
            if (error) {
                callback(error);
            }
            else {
                callback(error, resources, options);
            }
        }
    );
    if (!isAccepted) {
        throw new Error("[RetrieveDocumentsByIds] not accepted");
    }
}
