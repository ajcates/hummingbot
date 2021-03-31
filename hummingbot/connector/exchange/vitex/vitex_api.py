from decimal import Decimal
from typing import Dict, Any, Optional

import aiohttp

from hummingbot.connector.exchange.vitex.vitex_auth import VitexAuth
from hummingbot.core.event.events import (
    OrderType,
    TradeType
)

API_REST_ENDPOINT = "https://api.vitex.net/api/v2"
API_CALL_TIMEOUT = 10

VITEX_ORDER_TYPE = ["LIMIT", "MARKET"]
VITEX_ORDER_STATE = ["Unknown", "PendingRequest", "Received", "Open", "Filled", "PartiallyFilled", "PendingCancel",
                     "Cancelled", "PartiallyCancelled", "Failed", "Expired"]


class VitexAPIError(IOError):
    def __init__(self, error_payload: Dict[str, Any]):
        super().__init__(str(error_payload))
        self.error_payload = error_payload


class VitexAPI:
    def __init__(self, vitex_auth: VitexAuth = None):
        self._auth = vitex_auth
        self._shared_client = None

    @staticmethod
    def convert_from_exchange_symbol(symbol: str) -> Optional[str]:
        return symbol.replace("-", ".")

    @staticmethod
    def convert_to_exchange_symbol(symbol: str) -> Optional[str]:
        return symbol.replace(".", "-")

    @staticmethod
    def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> Optional[str]:
        return exchange_trading_pair.replace("-", ".").replace("_", "-")

    @staticmethod
    def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
        return hb_trading_pair.replace("-", "_").replace(".", "-")

    @staticmethod
    def convert_order_type(vitex_order_type: int) -> OrderType:
        if vitex_order_type == 0:
            return OrderType.LIMIT
        if vitex_order_type == 1:
            return OrderType.MARKET
        else:
            return OrderType.LIMIT

    @staticmethod
    def convert_trade_type(vitex_order_side: int) -> TradeType:
        return TradeType.BUY if vitex_order_side == 0 else TradeType.SELL

    @staticmethod
    def convert_order_state(vitex_order_state: int) -> str:
        state_value = vitex_order_state if vitex_order_state in range(11) else 0
        return VITEX_ORDER_STATE[state_value]

    @property
    def shared_client(self) -> str:
        return self._shared_client

    @shared_client.setter
    def shared_client(self, client: aiohttp.ClientSession):
        self._shared_client = client

    @staticmethod
    def format_params(params: Optional[Dict[str, Any]]):
        if params is not None:
            for k, v in params.items():
                if isinstance(v, Decimal):
                    params[k] = "{0:f}".format(v)  # format decimal: Decimal('5.1E-7') -> '0.00000051'
                elif isinstance(v, str) or isinstance(v, int):
                    pass  # string or int remains as it is
                else:
                    params[k] = str(v)  # format other types as string

    @staticmethod
    async def api_get(
            path: str,
            params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        full_url = f"{API_REST_ENDPOINT}{path}"

        # Format params
        VitexAPI.format_params(params)

        async with aiohttp.ClientSession() as client:
            async with client.get(full_url, timeout=API_CALL_TIMEOUT, params=params) as response:
                if response.status != 200:
                    raise IOError(f"Error getting data from {path}. HTTP status is {response.status}.")
                try:
                    parsed_response = await response.json()
                    if parsed_response.get("code") != 0:
                        raise VitexAPIError(parsed_response)
                except Exception:
                    raise IOError(f"Error parsing data from {path}.")

            return parsed_response.get("data")

    async def api_request(
            self,
            http_method: str,
            path: str,
            params: Optional[Dict[str, Any]] = None,
            data: Optional[Dict[str, Any]] = None,
            secure: bool = False) -> Dict[str, Any]:

        if self._shared_client is None:
            self._shared_client = aiohttp.ClientSession()

        full_url = f"{API_REST_ENDPOINT}{path}"

        # Format params
        VitexAPI.format_params(params)

        # Sign requests
        if secure:
            if self._auth is None:
                raise IOError("VitexAuth is required.")
            params = self._auth.generate_auth_dict(params)

        async with self._shared_client.request(http_method, url=full_url,
                                               timeout=API_CALL_TIMEOUT,
                                               data=data, params=params) as response:
            if response.status != 200:
                raise IOError(f"Error fetching data from {path}. HTTP status is {response.status}.")
            try:
                parsed_response = await response.json()
                if parsed_response.get("code") != 0:
                    raise VitexAPIError(parsed_response)
            except Exception:
                raise IOError(f"Error parsing data from {path}.")

            return parsed_response.get("data")
