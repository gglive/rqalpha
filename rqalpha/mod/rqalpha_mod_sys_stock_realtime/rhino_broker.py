# -*- coding: utf-8 -*-

from rqalpha.interface import AbstractBroker
from rqalpha.const import DEFAULT_ACCOUNT_TYPE
from rqalpha.events import EVENT, Event
from rqalpha.model.order import *
from rqalpha.model.base_position import Positions
from rqalpha.model.portfolio import Portfolio
from rqalpha.model.trade import *
from rqalpha.utils.i18n import gettext as _

from threading import Thread
from .rhino_trade_api import RealtimeTradeAPI

class RealtimeBroker(AbstractBroker):

    def __init__(self, env, mod_config):
        self._env = env
        self._mod_config = mod_config
        self._portfolio = None
        self._open_orders = []

        self._env.event_bus.add_listener(EVENT.PRE_BEFORE_TRADING, self._before_trading)
        self._env.event_bus.add_listener(EVENT.PRE_AFTER_TRADING, self._after_trading)

        # trade api创建及参数
        self._trade_api =  RealtimeTradeAPI()
        # TODO: config the username
        resultData, returnMsg = self._trade_api.login( "diryox", "8080")
        print ( resultData, returnMsg )

    def get_portfolio (self):
        """
        获取投资组合。系统初始化时，会调用此接口，获取包含账户信息、净值、份额等内容的投资组合
        :return: Portfolio
        """
        if self._portfolio is not None:
            return self._portfolio
        self._portfolio = self._init_portfolio()

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
            self._open_orders.append( ( resultData, order) )
            self._env.event_bus.publish_event(Event(EVENT.ORDER_CREATION_PASS, account=account, order=order))
      
        self._env.event_bus.publish_event(Event(EVENT.ORDER_CREATION_REJECT, account=account, order=order))

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
            return [order for __, order in self._open_orders]
        else:
            return [order for __, order in self._open_orders if order.order_book_id == order_book_id]

    def _before_trading(self, event):
        print("broker before_trading")

    def _after_trading(self, event):
        # 收盘时清掉未完成的订单

        for __, order in self._open_orders:
            order.mark_rejected(_(u"Order Rejected: {order_book_id} can not match. Market close.").format(
                order_book_id=order.order_book_id
            ))
            account = self._env.get_account(order.order_book_id)
            self._env.event_bus.publish_event(Event(EVENT.ORDER_UNSOLICITED_UPDATE, account=account, order=order))
        self._open_orders = []
        print("broker after_trading")

    def _init_portfolio (self):
        config = self._env.config

        accounts = {}
        total_cash = 100000 # config.base.stock_starting_cash
        
        StockAccount = self._env.get_account_model(DEFAULT_ACCOUNT_TYPE.STOCK.name)
        positions = self._get_positions(self._env)
        accounts[DEFAULT_ACCOUNT_TYPE.STOCK.name] = StockAccount(total_cash, positions)

        return Portfolio(self._env.config.base.start_date, 1, total_cash, accounts)

    def _get_account(self, order_book_id):
        # account = self._env.get_account(order_book_id)
        # for debug
        account = self._env.portfolio.accounts[DEFAULT_ACCOUNT_TYPE.STOCK.name]
        return account

    def _get_positions (self, env):
            StockPosition = env.get_position_model(DEFAULT_ACCOUNT_TYPE.STOCK.name)
            positions = Positions(StockPosition)
            # TODO:
            return positions

    def _get_order_id (self, order):
        for order_id, order_impl in self._open_orders:
            if order_impl is order:
                return order_id
        return None
        return order.secondary_order_id

    def _get_order_by_id (self, order_id):
        for _order_id, order_impl in self._open_orders:
            if order_id == _order_id:
                return order_impl
        return None