# -*- coding: utf-8 -*-

from rqalpha.interface import AbstractBroker
from rqalpha.const import DEFAULT_ACCOUNT_TYPE
from rqalpha.events import EVENT, Event
from rqalpha.model.order import *
from rqalpha.model.base_position import Positions
from rqalpha.model.portfolio import Portfolio
from rqalpha.model.trade import *
from rqalpha.utils.i18n import gettext as _

from .rhino_trade_api import RealtimeTradeAPI

from time import sleep
from threading import Thread
import datetime

class RealtimeBroker(AbstractBroker):

    def __init__(self, env, mod_config):
        self._env = env
        self._mod_config = mod_config
        self._portfolio = None
        self._open_orders = {}

        self._env.event_bus.add_listener(EVENT.PRE_BEFORE_TRADING, self._before_trading)
        self._env.event_bus.add_listener(EVENT.PRE_AFTER_TRADING, self._after_trading)

        # trade api创建及参数
        self._trade_api =  RealtimeTradeAPI()
        # TODO: config the username
        resultData, returnMsg = self._trade_api.login( self._mod_config.trade_srv_endpoint, self._mod_config.trade_strategy, "keep-secret")
        print ( "Login:", resultData, returnMsg )


        # 后台线程，查询成交
        _brokerQuery_thread = Thread (target=self._loopBrokerQuery)
        _brokerQuery_thread.setDaemon(True)
        _brokerQuery_thread.start()

    def get_portfolio (self):
        """
        获取投资组合。系统初始化时，会调用此接口，获取包含账户信息、净值、份额等内容的投资组合
        :return: Portfolio
        """
        if self._portfolio is not None:
            return self._portfolio
        
        self._portfolio = self._create_portfolio()

        if not self._portfolio._accounts:
            raise RuntimeError("accout config error")

        return self._portfolio

    def submit_order (self, order):
        """
        提交订单。在当前版本，RQAlpha 会生成 :class:`~Order` 对象，再通过此接口提交到 Broker。
        TBD: 由 Broker 对象生成 Order 并返回？
        """

        print("Broker.submit_order" )
        # if order.type == ORDER_TYPE.MARKET:
        #     raise RuntimeError("submit_order not support ORDER_TYPE.MARKET")

        account = self._get_account(order.order_book_id)
        self._env.event_bus.publish_event(Event(EVENT.ORDER_PENDING_NEW, account=account, order=order))
        order.active()
        
        security_id, exchange_id = order.order_book_id.split(".")
        # convert to wind style
        if exchange_id == "XSHG": 
            exchange_id = "SH"
        if exchange_id == "XSHE":
            exchange_id = "SZ"

        if order.side == SIDE.BUY:
            order_side = "B"
        if order.side == SIDE.SELL:
            order_side = "S"

        resultData, returnMsg = self._trade_api.place_order (
            security_id +"."+exchange_id, order_side, order.quantity, order.price)

        # 事件通知
        if resultData == "-1":
            order.mark_rejected("trade api req err:{} ".format( returnMsg[1]) )
            self._env.event_bus.publish_event(Event(EVENT.ORDER_CREATION_REJECT, account=account, order=order))
        else:
            # order.secondary_order_id = resultData
            self._open_orders[resultData] = order
            self._env.event_bus.publish_event(Event(EVENT.ORDER_CREATION_PASS, account=account, order=order))
      

    def cancel_order (self, order):
        """
        撤单。
        :param order: 订单
        :type order: :class:`~Order`
        """

        print("Broker.cancel_order" )

        account = self._get_account(order.order_book_id)
        order_id = self._get_order_id (order)

        if order_id is None:
            return

        if order.is_final():
            return

        self._env.event_bus.publish_event(Event(EVENT.ORDER_PENDING_CANCEL, account=account, order=order))

        ###################### 0 = 撤单
        resultData, returnMsg = self._trade_api.cancel_order (order_id)  
        ######################
        if resultData:
            self._env.event_bus.publish_event(Event(EVENT.ORDER_CANCELLATION_PASS, account=account, order=order))
        else:
            print ( returnMsg)
            self._env.event_bus.publish_event(Event(EVENT.ORDER_CANCELLATION_REJECT, account=account, order=order))

    def get_open_orders (self, order_book_id=None):
        """
        [Required]
        获得当前未完成的订单。
        :return: list[:class:`~Order`]
        """
        if order_book_id is None:
            return [ order for _, order in self._open_orders.items() ]
        else:
            return [ order for _, order in self._open_orders.items() if order.order_book_id == order_book_id ]

    def _before_trading(self, event):
        print("BROKER: before_trading")

    def _after_trading(self, event):
        # 收盘时清掉未完成的订单

        for __, order in self._open_orders:
            order.mark_rejected(_(u"Order Rejected: {order_book_id} can not match. Market close.").format(
                order_book_id=order.order_book_id
            ))
            account = self._env.get_account(order.order_book_id)
            self._env.event_bus.publish_event(Event(EVENT.ORDER_UNSOLICITED_UPDATE, account=account, order=order))
        self._open_orders = {}
        print("BROKER: after_trading")

    def _create_portfolio (self):
        config = self._env.config

        accounts = {}
        print (self._env.config.base.accounts)
        total_cash = 0
        StockAccount = self._env.get_account_model(DEFAULT_ACCOUNT_TYPE.STOCK.name)
        positions = self._get_broker_positions()
        accounts[DEFAULT_ACCOUNT_TYPE.STOCK.name] = StockAccount(total_cash, positions)
        return Portfolio(self._env.config.base.start_date, 1, total_cash, accounts)

    def _get_account (self, order_book_id):
        # account = self._env.get_account(order_book_id)
        # for debug
        account = self._env.portfolio.accounts [DEFAULT_ACCOUNT_TYPE.STOCK.name]
        return account

    def _get_broker_positions (self, ):

        StockPosition = self._env.get_position_model(DEFAULT_ACCOUNT_TYPE.STOCK.name)
        positions = Positions(StockPosition)
        resultData, returnMsg = self._trade_api.get_positions ()

        if returnMsg['code'] != 0 or resultData is None:
            return positions

        for poData in resultData:
            security_id, exchange_id = poData['order_book_id'].split(".")
            # convert to rqalpha style
            if exchange_id == "SH": 
                exchange_id = "XSHG"
            if exchange_id == "SZ":
                exchange_id = "XSHE"

            order_book_id = security_id + "." + exchange_id

            poItem = positions.get_or_create( order_book_id)
            poItem.set_state ({
                "order_book_id": order_book_id,
                "quantity": poData["position_qty"],
                "avg_price": poData["position_avgPx"],
                "non_closable": 0,
                "frozen": 0,
                "transaction_cost":0
            })
            poItem.prev_qty = poData['prev_qty']
            poItem.prev_avgPx = poData['prev_avgPx']
        return positions

    def _get_broker_orders (self, specifiedOrderId):

        resultData, returnMsg = self._trade_api.get_orders()
        if resultData is None:
            return

        for orderId, orderData in resultData.items():
            order = self._get_order_by_id ( orderId, )
            if order is None:
                print ("New Order: ",  orderId, orderData["order_qty"], orderData["knock_qty"], orderData["knock_avgPx"])
                order = self._create_order_by_data( orderId, orderData)
                order.set_secondary_order_id(orderId)
                self._open_orders[orderId] = order
                # HACK: force update once ...
                orderData["order_status"] = "EXECUTING"

            account = self._get_account (order.order_book_id)

            order_status = orderData["order_status"]
            if order_status == 'EXECUTING' or order_status == 'FULFILLED':

                knock_qty = int(orderData['knock_qty'])
                knock_avgPx = orderData['knock_avgPx']

                filled_qty = order.filled_quantity # order.quantity - order.unfilled_quantity
                filled_avgPx = order.avg_price
                # 无新的成交
                if filled_qty == knock_qty:
                    continue

                # 新的成交 
                trade = Trade.__from_create__(
                    order_id = order.order_id,
                    price = 0.0,
                    amount = 0,
                    side = order.side,
                    position_effect = order.position_effect,
                    order_book_id = order.order_book_id,
                    frozen_price = order.frozen_price,
                    commission = 0.0,
                    tax = 0.0
                )
                trade._amount = knock_qty - filled_qty
                trade._price = ( (knock_avgPx * knock_qty) - (filled_qty * filled_avgPx) ) / (knock_qty - filled_qty)
                print ( "New Trade: ", trade.order_book_id, trade.last_price, trade.last_quantity)
                
                order.fill( trade)
                self._env.event_bus.publish_event(Event(EVENT.TRADE, account=account, trade=trade, order=order))

            elif order_status == "CANCELLED" and order.status != ORDER_STATUS.CANCELLED:  # 6=已撤单
                order.mark_cancelled(_(u"{order_id} order has been cancelled by user.").format(order_id=order.order_id))
                self._env.event_bus.publish_event(Event(EVENT.ORDER_CANCELLATION_PASS, account=account, order=order))

            elif order_status == "INVALID" and order.status != ORDER_STATUS.REJECTED:  # 4=已失= "INVALID":  # 4=已失效  	7=已删除
                reason = _(u"Order Cancelled:  code = {0} status = {1} ").format(order.order_book_id, order_status)
                order.mark_rejected( reason)
                self._env.event_bus.publish_event(Event(EVENT.ORDER_CREATION_REJECT, account=account, order=order))
            else:
                # TODO: ORDER IS PENDING
                pass

            # if order_status != 'EXECUTING': # or 'PENDING'
            #     self._delete_open_order ( orderId)

    def _create_order_by_data ( self, order_id, order_data):

        security_id, exchange_id = order_data['order_book_id'].split(".")
        # convert to rqalpha style
        if exchange_id == "SH": 
            exchange_id = "XSHG"
        if exchange_id == "SZ":
            exchange_id = "XSHE"

        order_book_id = security_id + "." + exchange_id

        if order_data['order_side'] == "B":
            order_side = SIDE.BUY
        if order_data['order_side'] == "S":
            order_side = SIDE.SELL

        return Order.__from_create__(
            order_book_id   = order_book_id, 
            quantity        = order_data['order_qty'], 
            side            = order_side, 
            style           = MarketOrder(), 
            # TODO:
            position_effect = None,
        )

    def _get_order_id (self, order):
        for order_id, order_impl in self._open_orders.items():
            if order_impl is order:
                return order_id
        return None
        # return order.secondary_order_id

    def _get_order_by_id (self, order_id, ):

        # for _order_id, order_impl in self._open_orders.items():
        #     if order_id == _order_id:
        #         return order_impl
        # return None
        order_impl = self._open_orders.get (order_id)
        return order_impl

    def _delete_open_order ( self, order_id):
        order = self._get_order_by_id( order_id)
        if order is not None:
            del self._open_orders[order_id]
    
    
    def _loopBrokerQuery ( self, ):
        while True:
            if self._portfolio is None:
                continue
            sleep(1.0)
            # print ("BROKER: Sync Orders")
            self._get_broker_orders ( None )