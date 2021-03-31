from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.config_methods import using_exchange

CENTRALIZED = True

EXAMPLE_PAIR = "VITE-BTC.000"

DEFAULT_FEES = [0.25, 0.25]

KEYS = {
    "vitex_vite_address":
        ConfigVar(key="vitex_vite_address",
                  prompt="Enter your Vite address >>> ",
                  type_str="str",
                  required_if=using_exchange("vitex"),
                  is_secure=True,
                  is_connect_key=True),
    "vitex_api_key":
        ConfigVar(key="vitex_api_key",
                  prompt="Enter your ViteX API key >>> ",
                  required_if=using_exchange("vitex"),
                  is_secure=True,
                  is_connect_key=True),
    "vitex_secret_key":
        ConfigVar(key="vitex_secret_key",
                  prompt="Enter your ViteX API secret >>> ",
                  required_if=using_exchange("vitex"),
                  is_secure=True,
                  is_connect_key=True)
}
