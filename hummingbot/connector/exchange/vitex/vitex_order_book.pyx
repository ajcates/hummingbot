#!/usr/bin/env python
import logging
from typing import (
    Dict,
    Optional
)

from hummingbot.connector.exchange.vitex.vitex_api import VitexAPI
from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.data_type.order_book_message import (
    OrderBookMessage,
    OrderBookMessageType
)

_bob_logger = None


cdef class VitexOrderBook(OrderBook):
    @classmethod
    def logger(cls) -> HummingbotLogger:
        global _bob_logger
        if _bob_logger is None:
            _bob_logger = logging.getLogger(__name__)
        return _bob_logger

    @classmethod
    def snapshot_message_from_exchange(cls,
                                       msg: Dict[str, any],
                                       timestamp: float,
                                       metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": VitexAPI.convert_from_exchange_trading_pair(msg["trading_pair"]),
            "update_id": msg["timestamp"],
            "bids": msg["bids"],
            "asks": msg["asks"]
        }, timestamp=timestamp)

    @classmethod
    def diff_message_from_exchange(cls,
                                   msg: Dict[str, any],
                                   timestamp: Optional[float] = None,
                                   metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)
        # TODO: ViteX Websocket API does not support incremental depth messages, use snapshot message instead
        message_type = OrderBookMessageType.SNAPSHOT

        return OrderBookMessage(message_type, {
            "trading_pair": VitexAPI.convert_from_exchange_trading_pair(msg["topic"].split(".")[1]),
            "update_id": msg["timestamp"],
            "bids": msg["data"]["bids"],
            "asks": msg["data"]["asks"]
        }, timestamp=timestamp)

    @classmethod
    def trade_message_from_exchange(cls, msg: Dict[str, any], metadata: Optional[Dict] = None):
        if metadata:
            msg.update(metadata)
        ts = msg["timestamp"]
        data = msg["data"][0]
        return OrderBookMessage(OrderBookMessageType.TRADE, {
            "trading_pair": VitexAPI.convert_from_exchange_trading_pair(data["s"]),
            "trade_type": VitexAPI.convert_trade_type(data["side"]),
            "trade_id": data["id"],
            "update_id": ts,
            "price": data["p"],
            "amount": data["q"]
        }, timestamp=ts * 1e-3)

    @classmethod
    def from_snapshot(cls, msg: OrderBookMessage) -> "OrderBook":
        result = VitexOrderBook()
        result.apply_snapshot(msg.bids, msg.asks, msg.update_id)
        return result
