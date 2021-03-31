#!/usr/bin/env python

import asyncio
import logging
import pandas as pd
from typing import (
    Any,
    AsyncIterable,
    Dict,
    List,
    Optional
)
import time
import ujson
import websockets
from websockets.exceptions import ConnectionClosed

from hummingbot.connector.exchange.vitex.vitex_api import VitexAPI
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.vitex.vitex_order_book import VitexOrderBook

STREAM_URL = "wss://api.vitex.net/v2/ws"


class VitexAPIOrderBookDataSource(OrderBookTrackerDataSource):
    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _baobds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._baobds_logger is None:
            cls._baobds_logger = logging.getLogger(__name__)
        return cls._baobds_logger

    def __init__(self, trading_pairs: List[str]):
        super().__init__(trading_pairs)
        self._order_book_create_function = lambda: OrderBook()
        self._vitex_api = VitexAPI()

    async def get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        symbols = VitexAPI.convert_to_exchange_trading_pair(",".join(trading_pairs))
        params = {
            "symbols": symbols
        }
        response_data = await self._vitex_api.api_request("GET", "/ticker/24hr", params)
        results = dict()
        for trading_pair in trading_pairs:
            resp_record = \
                [o for o in response_data if VitexAPI.convert_from_exchange_trading_pair(o["symbol"]) == trading_pair][
                    0]
            results[trading_pair] = float(resp_record["closePrice"])
        return results

    async def get_snapshot(self, trading_pair: str, limit: int = 1000) -> Dict[str, Any]:
        symbol = VitexAPI.convert_to_exchange_trading_pair(trading_pair)
        params: Dict = {
            "limit": limit,
            "symbol": symbol
        } if limit != 0 \
            else {
            "symbol": symbol
        }

        result = await self._vitex_api.api_request("GET", "/depth", params)
        return result

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        snapshot: Dict[str, Any] = await self.get_snapshot(trading_pair, 1000)
        snapshot_timestamp: float = time.time()
        snapshot_msg: OrderBookMessage = VitexOrderBook.snapshot_message_from_exchange(
            snapshot,
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair}
        )
        order_book = self.order_book_create_function()
        order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
        return order_book

    @staticmethod
    async def fetch_trading_pairs() -> List[str]:
        try:
            path = "/markets"
            trading_pairs = await VitexAPI.api_get(path)
            return [VitexAPI.convert_from_exchange_trading_pair(item["symbol"]) for item in trading_pairs]
        except Exception:
            # Do nothing if the request fails -- there will be no autocomplete for ViteX trading pairs
            pass
        return []

    async def _inner_messages(self,
                              ws: websockets.WebSocketClientProtocol) -> AsyncIterable[Any]:
        # Terminate the recv() loop as soon as the next message timed out, so the outer loop can reconnect.
        try:
            while True:
                try:
                    msg: str = await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)
                    msg_json = ujson.loads(msg)
                    event = msg_json["event"]

                    if event == "push":
                        yield msg_json

                except asyncio.TimeoutError:
                    try:
                        ping_msg = '{"command": "ping"}'
                        await ws.send(ping_msg)
                    except asyncio.TimeoutError:
                        raise
        except asyncio.TimeoutError:
            self.logger().warning("WebSocket ping timed out. Going to reconnect...")
            return
        except ConnectionClosed:
            return
        finally:
            await ws.close()

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                topics = []
                for pair in self._trading_pairs:
                    symbol = VitexAPI.convert_to_exchange_trading_pair(pair)
                    topics.append(f"market.{symbol}.trade")

                async with websockets.connect(STREAM_URL) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    # Subscribe topics
                    subscribe_request: Dict[str, Any] = {
                        "command": "sub",
                        "params": topics
                    }
                    await ws.send(ujson.dumps(subscribe_request))
                    # Receive and parse messages
                    async for msg in self._inner_messages(ws):
                        trade_msg: OrderBookMessage = VitexOrderBook.trade_message_from_exchange(msg)
                        output.put_nowait(trade_msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                topics = []
                for pair in self._trading_pairs:
                    symbol = VitexAPI.convert_to_exchange_trading_pair(pair)
                    topics.append(f"market.{symbol}.depth")
                async with websockets.connect(STREAM_URL) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    subscribe_request: Dict[str, Any] = {
                        "command": "sub",
                        "params": topics
                    }
                    # Subscribe topics
                    await ws.send(ujson.dumps(subscribe_request))
                    # Receive and parse messages
                    async for msg in self._inner_messages(ws):
                        order_book_message: OrderBookMessage = VitexOrderBook.diff_message_from_exchange(
                            msg, time.time())
                        output.put_nowait(order_book_message)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    try:
                        snapshot: Dict[str, Any] = await self.get_snapshot(trading_pair)
                        snapshot_timestamp: float = time.time()
                        snapshot_msg: OrderBookMessage = VitexOrderBook.snapshot_message_from_exchange(
                            snapshot,
                            snapshot_timestamp,
                            metadata={"trading_pair": trading_pair}
                        )
                        output.put_nowait(snapshot_msg)
                        self.logger().debug(f"Saved order book snapshot for {trading_pair}")
                        await asyncio.sleep(5.0)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        self.logger().error("Unexpected error.", exc_info=True)
                        await asyncio.sleep(5.0)
                this_hour: pd.Timestamp = pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0)
                next_hour: pd.Timestamp = this_hour + pd.Timedelta(hours=1)
                delta: float = next_hour.timestamp() - time.time()
                await asyncio.sleep(delta)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await asyncio.sleep(5.0)
