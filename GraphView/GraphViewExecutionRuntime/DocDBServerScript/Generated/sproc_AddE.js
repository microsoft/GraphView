
function AddE(srcVertexId, edgeObject, isReverse) {

    "use strict";


    //++++++++++++++++ BEGIN: Common.snippet.js ++++++++++++++++
    /**
     * Description:
     *   This file provides basic functions, and should be included in every stored procedure & trigger!
     *
     * Export variables:
     *   collection, request, response
     *
     * Export functions:
     *   ASSERT(condition, [message])
     *   ERROR(e, [message])
     *   SUCCESS([content])
     */
    
    var collection = getContext().getCollection();
    var request = getContext().getRequest();
    var response = getContext().getResponse();
    
    var Status = { // REQUIRES: errCode < 0
        Success: 0,
        DBError: -1,
        NotAccepted: -2, // usually timeout
        AssertionFailed: -3,
        InternalError: -4,
    };
    Object.freeze(Status);
    
    var __respObject = {
        Status: 0,
        Message: "",
        DocDBError: null,
        Content: null,
    };
    Object.preventExtensions(__respObject);
    
    /**
     * Abort the procedure (and throw Error) and report the error
     * 
     * @param {string|object} e - docDBErrorObject or `Status`
     * @param @ {string|object|undefined} message - anything, the message
     * @returns {} - will not return!
     * @example
     *      ERROR(docDBErrorObject);
     *      ERROR(docDBErrorObject, myMessage);
     *      ERROR(myErrorStatus, myMessage);
     */
    function ERROR(e, message) {
        // __respObject.Status, __respObject.DocDBError
        if (typeof (e) === "number") {
            ASSERT(e !== Status.Success, "[ERROR] Should not pass Status.Success to ERROR() function.");
            __respObject.Status = e;
            __respObject.DocDBError = null;
        }
        else {
            __respObject.Status = Status.DBError;
            __respObject.DocDBError = e;
        }
    
        // __respObject.Message
        if (typeof (message) === "undefined") {
            __respObject.Message = "Unknown error";
        }
        else if (typeof (message) === "object") {
            __respObject.Message = JSON.stringify(message);
        }
        else {
            __respObject.Message = message.toString();
        }
    
        response.setBody(JSON.stringify(__respObject));
        throw new Error(JSON.stringify(__respObject));
    }
    
    /**
     * 
     * @param {string|object} errorOrMessage 
     * @param @ {number|undefined} [optional] status 
     * @returns {} 
     */
    function SUCCESS(content) {
        __respObject.Status = Status.Success;
        __respObject.Message = "OK";
        __respObject.DocDBError = null;
        __respObject.Content = (typeof (content) === "undefined") ? null : content;
    
        response.setBody(JSON.stringify(__respObject));
    }
    
    /**
     * 
     * @param {} condition 
     * @param {} messageOnFail 
     * @returns {} 
     */
    function ASSERT(condition, messageOnFail) {
        if (!condition) {
            ERROR(Status.AssertionFailed, messageOnFail || "Assertion failed");
        }
    }
    
    //---------------- END: Common.snippet.js ----------------


    //++++++++++++++++ BEGIN: RetrieveDocument.snippet.js ++++++++++++++++
    
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
            throw Error("[RetrieveDocumentById] not accepted");
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
            throw Error("[RetrieveDocumentsByIds] not accepted");
        }
    }
    
    //---------------- END: RetrieveDocument.snippet.js ----------------


    RetrieveDocumentById(srcVertexId, callback);
}

