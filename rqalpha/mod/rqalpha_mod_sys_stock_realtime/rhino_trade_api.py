# -*- coding: utf-8 -*-
import json
from .jsonrpc.client import Client

class RealtimeTradeAPI:

    def __init__(self,):

        self.jsonrpc = Client()

        self._username    = ""
        self._password    = ""

    def close (self):
        self.jsonrpc.close()
   
    def login ( self, username, password ):
        self.jsonrpc.link ( _Identity="rqalpha-mod", _Endpoint="tcp://127.0.0.1:58086")#
        self.jsonrpc.start ()#

        self._username = username
        self._password = password

        # Shouldn't check connected flag here. ZMQ is a mesageq queue!
        # if !self._connected :
        #    return (False, "-1,no connection")

        if self._username and self._password:
            rpc_params = { "username": self._username, "password": self._password }
            # TODO:
            returnData, retMsg = self.jsonrpc.call("rqalpha.login", rpc_params)
            return ( True, self._username + " has login")
        else:
            return (False, "-1,empty username or password")

    def place_order (self, security_id, order_side,  order_qty, order_px_limit,):
        """
        return (result, message)
        if result is None, message contains error information
        """

        order_payload = { 
            "security_id": security_id,
            "order_side": order_side,
            "order_qty" : int(order_qty),
            "order_px_limit": order_px_limit,
            "order_place_strategy": "TWAP_KY_01",
            "order_place_extraopts": json.dumps({
                'algo.style': 2,
                'algo.order_position': 'OP1',
                'algo.order_tick': 99 ,
                'algo.append_position': 'OP1',
                'algo.append_tick': 99,
                'algo.cancel_cycle': 60,
                'offer_start_time': '09:30:00',
                'offer_stop_time': '15:00:00'
            }),
        }

        return self.jsonrpc.call ("rqalpha.order.place", order_payload)

    def cancel_order (self, order_id):
        """
        return (result, message)
        if result is None, message contains error information
        """
        return self.jsonrpc.call("rqalpha.order.cancel", { "order_id": order_id } )
