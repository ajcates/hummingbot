#!/usr/bin/env python
import sys
import asyncio
import logging
import unittest
from decimal import Decimal
from os.path import join, realpath
from hummingbot.connector.exchange.vitex.vitex_api import VitexAPI
from typing import (
    Optional,
)
from hummingbot.core.utils.async_utils import (
    safe_ensure_future,
    safe_gather,
)
sys.path.insert(0, realpath(join(__file__, "../../../")))


class VitexAPIUnitTest(unittest.TestCase):
    api: Optional[VitexAPI] = None

    @classmethod
    def setUpClass(cls):
        pass

    async def run_parallel_async(self, *tasks):
        future: asyncio.Future = safe_ensure_future(safe_gather(*tasks))
        while not future.done():
            await asyncio.sleep(1.0)
        return future.result()

    def run_parallel(self, *tasks):
        return self.ev_loop.run_until_complete(self.run_parallel_async(*tasks))

    def setUp(self):
        pass

    def test_format_params(self):
        params = {"s": "abc", "i": 3, "d": Decimal('5.1E-7')}
        VitexAPI.format_params(params)
        formatted = {"s": "abc", "i": 3, "d": "0.00000051"}
        self.assertEqual(params, formatted)

    def test_symbol_conversion(self):
        vitex_symbol = "BTC-000"
        hb_symbol = "BTC.000"
        convert_from = VitexAPI.convert_from_exchange_symbol(vitex_symbol)
        self.assertEqual(convert_from, hb_symbol)
        convert_to = VitexAPI.convert_to_exchange_symbol(hb_symbol)
        self.assertEqual(convert_to, vitex_symbol)

    def test_trading_pair_conversion(self):
        vitex_pair = "BTC-000_USDT-000"
        hb_pair = "BTC.000-USDT.000"
        convert_from = VitexAPI.convert_from_exchange_trading_pair(vitex_pair)
        self.assertEqual(convert_from, hb_pair)
        convert_to = VitexAPI.convert_to_exchange_trading_pair(hb_pair)
        self.assertEqual(convert_to, vitex_pair)


def main():
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()


if __name__ == "__main__":
    main()
