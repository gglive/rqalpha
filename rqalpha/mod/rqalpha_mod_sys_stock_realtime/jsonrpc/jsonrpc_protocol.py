import uuid
import json #, msgpack, snappy

class _Request_jsonrpc_v20: # (Event):
    """ A rpc call is represented by sending a Request object to a Server.
    """
    
    JSON_RPC_VERSION = '2.0'
    JSON_RPC_DUMMY_ID = object()

    def __init__ (self, clientSession, payloadData):
        
        # Identity of the client session
        self.session     = clientSession

        # NB: notification is not support by now 
        if "id" not in payloadData:
            payloadData['id'] = uuid.uuid4().__str__() #.bytes

        # Original message data from rpc call
        self.data = payloadData

        # Enforce to specify the version of the JSON-RPC protocol.
        if "jsonrpc" not in payloadData:
            self.data["jsonrpc"] = _Request_jsonrpc_v20.JSON_RPC_VERSION

        # A String containing the name of the method to be invoked. 
        # Method names that begin with the word rpc followed by a period character (U+002E or ASCII 46) 
        # are reserved for rpc-internal methods and extensions and MUST NOT be used for anything else. 
        self.method = payloadData.get ('method')
       
       # A Structured value that holds the parameter values to be used during the invocation
       #  of the method. This member MAY be omitted.
        self.params = payloadData.get ('params')

        # The Identifier that established by the Client that MUST contain a `String`, `Number`, or `NULL` 
        # if included. If it is not included it is assumed to be a notification. The value SHOULD normally
        # not be Null [1] and Numbers SHOULD NOT contain fractional parts [2].
        #
        # [1] The use of Null as a value for the id member in a Request object 
        # is discouraged, because this specification uses a value of Null for Responses 
        # with an unknown id. Also, because JSON-RPC 1.0 uses an id value of Null
        # for Notifications this could cause confusion in handling.
        #
        # [2] Fractional parts may be problematic, since many decimal fractions
        #  cannot be represented exactly as binary fractions.
        self.identity = payloadData.get ('id')


class _Response_jsonrpc_v20:
    """ When a rpc call is made, the Server MUST reply with a Response, except that 
    in the case of Notifications. The Response is expressed as a single JSON Object.
    """

    JSON_RPC_VERSION = '2.0'
    JSON_RPC_NO_ERROR = [0, '']
    
    def __init__(self, sessionRequest, resultData, errorData):
        
        # Client session of rpc call.
        self.session = sessionRequest.session

        # Message data payload of reply of a rpc call.
        self.data = dict()

        #  A String specifying the version of the JSON-RPC protocol. MUST be exactly "2.0".
        self.data['jsonrpc'] = _Response_jsonrpc_v20.JSON_RPC_VERSION

        # `id` is REQUIRED. It MUST be the same as the value of the id member 
        # in the Request Object. If there was an error in detecting the id in 
        # the Request object (e.g. Parse error/Invalid Request), it MUST be Null.
        self.data['id'] = sessionRequest.identity

        # Either the result member or error member MUST be included, but both 
        # members MUST NOT be included.
        #
        if resultData is not None:
            # The `result` field is REQUIRED on success. It MUST NOT exist 
            # if there was an error invoking the method. The value of this 
            # field is determined by the method invoked on the Server.
            self.data['result'] = resultData
        if errorData:
            # The `error` field is REQUIRED on error. It MUST NOT exist if 
            # there was no error triggered during invocation. The value for 
            # this field MUST be an Object.
            # NB: JAQS, this field maybe exist on success, with empty payload,
            # that is NOT consist with JSON-RPC specification.
            self.data['error'] = { 
                "error"     : errorData[0], 
                'message'   : errorData[1] 
            }

class _Respond_jsonrpc_v20:

    def __init__( self, clientSession, returnData):
        # @copydoc: _Response_jsonrpc_v20.client
        self.session    = clientSession
        # @copydoc: _Response_jsonrpc_v20.data
        self.data       = returnData
        # @copydoc: _Response_jsonrpc_v20.data["id"]
        self.identity   = returnData.get("id")
        # @copydoc: _Response_jsonrpc_v20.data["result"]
        self.result     = returnData.get("result")
        # @copydoc: _Response_jsonrpc_v20.data["error"]
        self.error      = returnData.get("error")
