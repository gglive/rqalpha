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

#from .trade_api import RealtimeTradeAPI

class RealtimeBroker(AbstractBroker):

    def __init__(self, env, mod_config):
        self._env = env
        self._mod_config = mod_config
        self._portfolio = None
        self._open_orders = []

        self._env.event_bus.add_listener(EVENT.PRE_BEFORE_TRADING, self._before_trading)
        self._env.event_bus.add_listener(EVENT.PRE_AFTER_TRADING, self._after_trading)

        # trade api创建及参数
        # self._trade_api = RealtimeTradeAPI(self._mod_config.api_svr.ip, self._mod_config.api_svr.port)

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

        print("Broker.submit_order:{}".format(order) )
        if order.type == ORDER_TYPE.MARKET:
            raise RuntimeError("submit_order not support ORDER_TYPE.MARKET")

        account = self._get_account(order.order_book_id)
        self._env.event_bus.publish_event(Event(EVENT.ORDER_PENDING_NEW, account=account, order=order))
        order.active()

        # futu_order_side = 0 if order.side == SIDE.BUY else 1
        # futu_order_type = 0  # 港股增强限价单
        # ret_code, ret_msg = self._trade_api.place_order(order.price, order.quantity, order.order_book_id,futu_order_side, futu_order_type,)

        # 事件通知
        # if ret_msg[0] != 0:
        #     order.mark_rejected("trade api req err:{} ".format(ret_msg[1]))
        #     self._env.event_bus.publish_event(Event(EVENT.ORDER_CREATION_REJECT, account=account, order=order))
        # else:
        #     futu_order_id = ret_data.loc[0, 'orderid']
        #     self._open_orders.append( (futu_order_id, order) )
        #     self._env.event_bus.publish_event(Event(EVENT.ORDER_CREATION_PASS, account=account, order=order))
      
        self._env.event_bus.publish_event(Event(EVENT.ORDER_CREATION_REJECT, account=account, order=order))

    def cancel_order (self, order):
        """
        撤单。
        :param order: 订单
        :type order: :class:`~Order`
        """
        account = self._get_account(order.order_book_id)
        order_id = self._get_order_id (order)

        if order_id is None:
            return

        if order.is_final():
            return

        self._env.event_bus.publish_event(Event(EVENT.ORDER_PENDING_CANCEL, account=account, order=order))
        # ret_code, ret_msg = self._trade_api.cancel_order (0, futu_order_id, self._env)  # 0 = 撤单
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

    def _get_order_by_id (self, order_id):
        for _order_id, order_impl in self._open_orders:
            if order_id == _order_id:
                return order_impl
        return None