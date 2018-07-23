# -*- coding: utf-8 -*-
import json

class RealtimeTradeAPI:

    def __init__(self,):

        self.jsonrpc = None

        self._username    = ""
        self._password    = ""

    def close (self):
        self.jsonrpc.close()
   
    def login ( self, username, password ):
        self._username = username
        self._password = password

        # Shouldn't check connected flag here. ZMQ is a mesageq queue!
        # if !self._connected :
        #    return (False, "-1,no connection")

        if self._username and self._password:
            rpc_params = {
                "username": self._username,
                "password": self._password
            }

            cr = self.jsonrpc.call("rqalpha.login", rpc_params)
        else:
            return (False, "-1,empty username or password")

    def place_order (self, security, action, price, size, algo="", algo_param={} ):
        """
        return (result, message)
        if result is None, message contains error information
        """

        rpc_params = { "security"    : security,
                       "action"      : action,
                       "price"       : price,
                       "size"        : int(size),
                       "algo"        : algo,
                       "algo_param"  : json.dumps(algo_param),
                       "user"        : self._username}

        cr = self.jsonrpc.call ("rqalpha.order.place", rpc_params)
        return utils.extract_result(cr)

    def cancel_order (self, order_id):
        """
        return (result, message)
        if result is None, message contains error information
        """

        rpc_params = {"order_id": order_id}

        cr = self.jsonrpc.call("rqalpha.order.cancel", rpc_params)
        return utils.extract_result(cr)
