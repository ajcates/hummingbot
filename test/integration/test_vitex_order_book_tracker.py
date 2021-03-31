#!/usr/bin/env python
import math
from os.path import join, realpath
import sys; sys.path.insert(0, realpath(join(__file__, "../../../")))
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import OrderBookEvent, OrderBookTradeEvent, TradeType
from hummingbot.connector.exchange.vitex.vitex_order_book_tracker import VitexOrderBookTracker

import asyncio
import logging
from typing import (
    Dict,
    Optional,
    List,
)
import unittest

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.utils.async_utils import (
    safe_ensure_future,
    safe_gather,
)


class VitexOrderBookTrackerUnitTest(unittest.TestCase):
    order_book_tracker: Optional[VitexOrderBookTracker] = None
    events: List[OrderBookEvent] = [
        OrderBookEvent.TradeEvent
    ]
    trading_pairs: List[str] = [
        "VITE-BTC.000",
        "VX-BTC.000"
    ]

    @classmethod
    def setUpClass(cls):
        cls.ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        cls.order_book_tracker: VitexOrderBookTracker = VitexOrderBookTracker(
            trading_pairs=cls.trading_pairs)
        cls.order_book_tracker_task: asyncio.Task = safe_ensure_future(cls.order_book_tracker.start())
        cls.ev_loop.run_until_complete(cls.wait_til_tracker_ready())

    @classmethod
    async def wait_til_tracker_ready(cls):
        while True:
            # if len(cls.order_book_tracker.order_books) > 0:
            if cls.order_book_tracker.ready:
                print("Order book tracker initialized.")
                return
            print("Wait for order book tracker ready...")
            await asyncio.sleep(1)

    async def run_parallel_async(self, *tasks):
        future: asyncio.Future = safe_ensure_future(safe_gather(*tasks))
        while not future.done():
            await asyncio.sleep(1.0)
        return future.result()

    def run_parallel(self, *tasks):
        return self.ev_loop.run_until_complete(self.run_parallel_async(*tasks))

    def setUp(self):
        self.event_logger = EventLogger()
        for event_tag in self.events:
            for trading_pair, order_book in self.order_book_tracker.order_books.items():
                order_book.add_listener(event_tag, self.event_logger)

    def test_order_book_trade_event_emission(self):
        """
        Test if order book tracker is able to retrieve order book trade message from exchange and
        emit order book trade events after correctly parsing the trade messages
        """
        self.run_parallel(self.event_logger.wait_for(OrderBookTradeEvent))
        for ob_trade_event in self.event_logger.event_log:
            print(f"ob_trade_event: {ob_trade_event}")
            self.assertTrue(type(ob_trade_event) == OrderBookTradeEvent)
            self.assertTrue(ob_trade_event.trading_pair in self.trading_pairs)
            self.assertTrue(type(ob_trade_event.timestamp) == float)
            self.assertTrue(type(ob_trade_event.amount) == float)
            self.assertTrue(type(ob_trade_event.price) == float)
            self.assertTrue(type(ob_trade_event.type) == TradeType)
            self.assertTrue(math.ceil(math.log10(ob_trade_event.timestamp)) == 10)
            self.assertTrue(ob_trade_event.amount > 0)
            self.assertTrue(ob_trade_event.price > 0)

    def test_tracker_integrity(self):
        # Wait 10 seconds to process some diffs.
        self.ev_loop.run_until_complete(asyncio.sleep(100.0))
        order_books: Dict[str, OrderBook] = self.order_book_tracker.order_books
        vite_btc_book: OrderBook = order_books["VITE-BTC.000"]
        vx_btc_book: OrderBook = order_books["VX-BTC.000"]
        print(vite_btc_book.snapshot)
        print(vx_btc_book.snapshot)
        self.assertGreaterEqual(vite_btc_book.get_price_for_volume(True, 1000).result_price,
                                vite_btc_book.get_price(True))
        self.assertLessEqual(vite_btc_book.get_price_for_volume(False, 1000).result_price,
                             vite_btc_book.get_price(False))
        self.assertGreaterEqual(vx_btc_book.get_price_for_volume(True, 1000).result_price,
                                vx_btc_book.get_price(True))
        self.assertLessEqual(vx_btc_book.get_price_for_volume(False, 1000).result_price,
                             vx_btc_book.get_price(False))
        for order_book in self.order_book_tracker.order_books.values():
            print(order_book.last_trade_price)
            self.assertFalse(math.isnan(order_book.last_trade_price))

    def test_api_get_last_traded_prices(self):
        prices = self.ev_loop.run_until_complete(
            self.order_book_tracker.data_source.get_last_traded_prices(["VITE-BTC.000", "VX-BTC.000"]))
        for key, value in prices.items():
            print(f"{key} last_trade_price: {value}")
        self.assertGreater(prices["VITE-BTC.000"], 0)
        self.assertLess(prices["VX-BTC.000"], 1)


def main():
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()


if __name__ == "__main__":
    main()
