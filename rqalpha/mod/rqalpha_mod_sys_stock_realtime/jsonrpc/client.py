import threading
import functools
import time
from collections import defaultdict
try:
    import queue
except ImportError:
    import Queue as queue

# import copy

import zmq #, zmq.asyncio
import msgpack #, snappy

from .jsonrpc_protocol import _Request_jsonrpc_v20, _Respond_jsonrpc_v20


class Client:
    
    def __init__(self, ):
        
        # new context of client session
        self.context        = None # zmq.Context.instance()
        
        # push calls by logic threads into and 
        # pull out by the zmq thread  
        self.pull_socket    = None
        self.push_socket    = None

        # listen to zmq messages in shadow
        self.shadow_socket  = None 

        
        # identity of current endpoint, used as routing id for server side
        self.identity       =  None # 'rhino/' + id
        
        # address of the physical endpoint 
        self.endpoint       = ""

        # max time allowed to send, recv of 0MQ
        self.timeout        = 3 # timeout

        # if active to loop to handle messages
        self.active         = False

        # heartbeat settings
        self._heartbeat_interval    = 1
        self._heartbeat_timeout     = 3

        
        # Threads'  settings
        self._zmq_thread    = None
        self._wcb_thread    = None

        # locks
        self._send_lock = threading.Lock()
        self._wait_lock = threading.Lock()
        
        # Waitable-Queues one for each thread
        self.jsonrpc_waitables      = {} # threading.local ()
        # waits for rpc call return
        self.jsonrpc_waitMapping    = {}

        # jsonrpc notifications &N connection events
        self.jsonrpc_events = queue.Queue ()
        # callables to handle notifications 
        self.jsonrpc_callbacks = {} 


    def link ( self, _Identity, _Endpoint):
        self.context        = zmq.Context.instance()
        self.identity       =  'rqalpha/' + _Identity
        self.endpoint       = _Endpoint
        self.shadow_socket  = self.context.socket ( zmq.DEALER)

        self.shadow_socket = self.context.socket(zmq.DEALER)
        self.shadow_socket.setsockopt_string(zmq.IDENTITY, self.identity)
        self.shadow_socket.setsockopt(zmq.SNDTIMEO, self.timeout*1000)
        self.shadow_socket.setsockopt(zmq.RCVTIMEO, self.timeout*1000)
        self.shadow_socket.setsockopt(zmq.LINGER, 0)
        self.shadow_socket.setsockopt(zmq.PROBE_ROUTER, True)
        # self.shadow_socket.setsockopt(zmq.MAX_RECONNECT_IVL, 1000)
        
        # connect to the endpoint
        self.shadow_socket.connect (self.endpoint)

        # bridging frontend and backend  
        self.pull_socket = self.context.socket (zmq.PULL)
        self.pull_socket.bind ("inproc://rqalpha")
        self.push_socket = self.context.socket(zmq.PUSH)
        self.push_socket.connect("inproc://rqalpha")


    def start (self, ):
        self.active = True
        
        self._zmq_thread   = threading.Thread (target=self._zmq_loop, daemon=True)
        self._wcb_thread   = threading.Thread (target=self._wcb_loop, daemon=True)
        self._zmq_thread.start()
        self._wcb_thread.start()
        # 

    def close ( self, ):
        self.active     = False
        self._wcb_thread.join()
        self._zmq_thread.join()

    
    def call ( self, method, params, ):
        clientRequest = _Request_jsonrpc_v20( self.identity, {"method": method, "params": params,})
        call_id = clientRequest.identity
        
        thread_id = threading.current_thread().ident
        # no waitable queue on current thread, create a new one.
        with self._wait_lock:
            if thread_id not in self.jsonrpc_waitables:
                self.jsonrpc_waitables[ thread_id ]= queue.Queue()
        waitable = self.jsonrpc_waitables[thread_id]
        self.jsonrpc_waitMapping[call_id] = waitable
        
        self.send ( clientRequest)
        try:
            # wait to get result that zmq thread put into the waitable queue 
            returnData = waitable.get( timeout=self.timeout)
            waitable.task_done()
        except queue.Empty:
            returnData = None
        
        with self._wait_lock:
            del self.jsonrpc_waitMapping [call_id]

        if returnData:
            return ( returnData.result, returnData.error )
        else:
            return ( None, {"code": -1, "message": "jsonrpc, timeout"} )

    def send ( self, payload):
        try:
            jsonData = msgpack.dumps(payload.data, encoding='utf-8')
            with self._send_lock:
                self.push_socket.send ( jsonData)
        except zmq.error.ZMQError as zmqerror:
            print ("0MQ: ", zmqerror)

    
    def heartbeat (self, ):
        clientRequest = _Request_jsonrpc_v20 ( self.identity, {
            'jsonrpc' : '2.0',
            'method'  : 'x.heartbeat',
            'params'  : { 'time': time.time() },
        })
        self.send ( clientRequest)

        
    def _zmq_loop (self, ):

        heartbeat_ping = 0
        heartbeat_pong = 0
        poller = zmq.Poller()
        poller.register (self.pull_socket, zmq.POLLIN)
        poller.register ( self.shadow_socket, zmq.POLLIN)

        while self.active:
            try:
                if time.time() - heartbeat_pong > self._heartbeat_timeout:
                    # TODO: heartbeat timeout, retry to build connection 
                    print ("RPC: Heartbeat timeout")
                if time.time() - heartbeat_ping > self._heartbeat_interval:
                    self.heartbeat()
                    heartbeat_ping = time.time()

                ss = dict ( poller.poll(500) )
                if ss.get ( self.pull_socket) == zmq.POLLIN:
                    msgData = self.pull_socket.recv()
                    # if cmd.startswith (b"#"):
                    self.shadow_socket.send ( msgData )

                if ss.get(self.shadow_socket) == zmq.POLLIN:
                    data = self.shadow_socket.recv()
                    try:
                        msgData = msgpack.loads (data, encoding='utf-8')
                        if not msgData:
                            print("RPC: Can't parse message data")

                        if msgData.get( 'method') == 'x.heartbeat':
                            heartbeat_pong = time.time()
                            # print ( " == RPC: HEARTBEAT ==")
                            continue

                        # print (msgData)
                        self.dispatch ( msgData)
                    except Exception as e:
                        print("RPC: handle msg failed, ", e)
                        pass
            except zmq.error.Again as zmqerror:
                print ("0MQ: RECV TIMEOUT: ", zmqerror)
            except Exception as e:
                print("RPC: recv data failed, ", e)

    def _wcb_loop (self, ):
        while self.active:
            try:
                rpc_callback, rpc_args = self.jsonrpc_events.get( timeout=1)
                if rpc_callback:
                    rpc_callback ( rpc_args)
            except queue.Empty as e:
                pass
            except TypeError as e:
                if str(e) == "'NoneType' object is not callable":
                    pass
                else:
                    print("RPC: msg handler, {}".format(rpc_callback), type(e), e)
            except Exception as e:
                print("RPC: msg handler, {}".format(rpc_callback), type(e), e)


    def dispatch (self, msgData):
        if 'id' in msgData: # rpc call return
            returnData = _Respond_jsonrpc_v20 ( self.identity, msgData)
            _Identity = returnData.identity
            with self._wait_lock:
                waitable = self.jsonrpc_waitMapping.get (_Identity )
                waitable.put ( returnData )
        else: # notification 
            rpc_callback = self.jsonrpc_callbacks.get( msgData['method'])
            # put into the background thread
            self.jsonrpc_events.put ( ( rpc_callback, msgData["params"]) )


    
    def on (self, rpc_meth, rpc_callback=None):

        def set_callback (rpc_callback):
            self.jsonrpc_callbacks[rpc_meth] = rpc_callback
            return rpc_callback

        if rpc_callback is None:
            return set_callback
        #
        set_callback (rpc_callback)
