# -*- coding: utf-8 -*-
import datetime
import time
import uuid
import json
from .jsonrpc.client import Client


class RealtimeTradeAPI:

    def __init__(self, endpoint):

        self.jsonrpc = Client( _Endpoint=endpoint, _Identity="rqalpha-mod-realtime-"+uuid.uuid4().__str__(), )

        self._username    = ""
        self._password    = ""

    def close (self):
        self.jsonrpc.close()
   
    def login ( self, username, password ):
        self.jsonrpc.start ()#

        self._username = username
        self._password = password

        # Shouldn't check connected flag here. ZMQ is a mesageq queue!
        # if !self._connected :
        #    return (False, "-1,no connection")

        if self._username and self._password:
            rpc_params = { "username": self._username, "password": self._password }
            # TODO:
            returnData, returnMsg = self.jsonrpc.call("rqalpha.login", rpc_params)
            return ( returnMsg["code"] == 0, self._username + " has login")
        else:
            return (False, "-1,empty username or password")


    def get_positions ( self, ):
        return self.jsonrpc.call ("rqalpha.portfolio", { "tag": self._username})
    
    def get_orders (self, ):
        return self.jsonrpc.call ( "rqalpha.order.status", { "tag": self._username})


    def place_order (self, order_book_id, order_side,  order_qty, order_px_limit,):
        """
        return (result, message)
        if result is None, message contains error information
        """

        # 
        # order_delta_time = datetime.datetime.now() + datetime.timedelta( minutes=1)
        order_payload = { 
            "order_book_id": order_book_id,
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
                'offer_stop_time': "COMMON_ORDER" # order_delta_time.strftime("%H:%M:%S") # 
            }),
        }

        return self.jsonrpc.call ("rqalpha.order.place", order_payload)

    def cancel_order (self, order_id):
        """
        return (result, message)
        if result is None, message contains error information
        """
        return self.jsonrpc.call("rqalpha.order.cancel", { "order_id": order_id } )

