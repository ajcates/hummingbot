from os.path import join, realpath
import sys

import asyncio
import conf
import contextlib
import logging
import os
import time
from typing import List
import unittest
from decimal import Decimal

from hummingbot.core.clock import Clock, ClockMode
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import (
    MarketEvent,
    OrderCancelledEvent,
    TradeType,
    TradeFee,
)
from hummingbot.connector.exchange.vitex.vitex_exchange import VitexExchange
from hummingbot.market.market_base import OrderType

sys.path.insert(0, realpath(join(__file__, "../../../")))


class VitexMarketUnitTest(unittest.TestCase):
    market_events: List[MarketEvent] = [
        MarketEvent.ReceivedAsset,
        MarketEvent.BuyOrderCompleted,
        MarketEvent.SellOrderCompleted,
        MarketEvent.WithdrawAsset,
        MarketEvent.OrderFilled,
        MarketEvent.BuyOrderCreated,
        MarketEvent.SellOrderCreated,
        MarketEvent.OrderCancelled,
    ]

    exchange: VitexExchange
    market_logger: EventLogger
    stack: contextlib.ExitStack

    @classmethod
    def setUpClass(cls):
        cls.exchange: VitexExchange = VitexExchange(
            conf.vitex_vite_address,
            conf.vitex_api_key,
            conf.vitex_secret_key,
            10.0,
            trading_pairs=["VITE-BTC.000"],
        )
        print("Initializing Vitex Exchange... ")
        cls.ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        cls.clock: Clock = Clock(ClockMode.REALTIME)
        cls.clock.add_iterator(cls.exchange)
        cls.stack = contextlib.ExitStack()
        cls._clock = cls.stack.enter_context(cls.clock)
        cls.ev_loop.run_until_complete(cls.wait_til_ready())
        print("Ready.")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.stack.close()

    @classmethod
    async def wait_til_ready(cls):
        # await cls.market.start_network()
        while True:
            now = time.time()
            next_iteration = now // 1.0 + 1
            if cls.exchange.ready:
                break
            else:
                await cls._clock.run_til(next_iteration)
            await asyncio.sleep(1.0)

    def setUp(self):
        self.db_path: str = realpath(join(__file__, "../vitex_test.sqlite"))
        try:
            os.unlink(self.db_path)
        except FileNotFoundError:
            pass

        self.market_logger = EventLogger()
        for event_tag in self.market_events:
            self.exchange.add_listener(event_tag, self.market_logger)

    def tearDown(self):
        for event_tag in self.market_events:
            self.exchange.remove_listener(event_tag, self.market_logger)
        self.market_logger = None

    async def run_parallel_async(self, *tasks):
        future: asyncio.Future = asyncio.ensure_future(asyncio.gather(*tasks))
        while not future.done():
            now = time.time()
            next_iteration = now // 1.0 + 1
            await self._clock.run_til(next_iteration)
            await asyncio.sleep(1.0)
        return future.result()

    def run_parallel(self, *tasks):
        return self.ev_loop.run_until_complete(self.run_parallel_async(*tasks))

    # ====================================================

    def test_get_fee(self):
        limit_trade_fee: TradeFee = self.exchange.get_fee("VX", "VITE", OrderType.LIMIT, TradeType.BUY, 100, 10)
        print(limit_trade_fee)
        self.assertLess(limit_trade_fee.percent, 0.01)

    def test_get_balances(self):
        balances = self.exchange.get_all_balances()
        print(balances)
        self.assertGreaterEqual((balances["BTC.000"]), 0)
        self.assertGreaterEqual((balances["VITE"]), 0)

    def test_get_available_balances(self):
        balance = self.exchange.get_available_balance("BTC.000")
        self.assertGreaterEqual(balance, 0)

    def test_limit_orders(self):
        orders = self.exchange.limit_orders
        self.assertGreaterEqual(len(orders), 0)

    def test_cancel_order(self):
        trading_pair = "VITE-BTC.000"
        bid_price: Decimal = self.exchange.get_price(trading_pair, True)
        print("best bid price is", bid_price)
        amount = Decimal("187.23")
        price = bid_price * Decimal(1.5)
        # Intentionally setting price far away from best ask
        order_id = self.exchange.sell(trading_pair, amount, OrderType.LIMIT, price)
        print("sell order id", order_id)
        self.run_parallel(asyncio.sleep(5.0))
        print("try to cancel order", order_id)
        self.exchange.cancel(trading_pair, order_id)
        [order_cancelled_event] = self.run_parallel(self.market_logger.wait_for(OrderCancelledEvent))
        order_cancelled_event: OrderCancelledEvent = order_cancelled_event

        self.run_parallel(asyncio.sleep(6.0))
        self.assertEqual(0, len(self.exchange.limit_orders))
        self.assertEqual(order_id, order_cancelled_event.order_id)


def main():
    logging.basicConfig(level=logging.INFO)
    unittest.main()


if __name__ == "__main__":
    main()
