import time
import hashlib
import hmac
from typing import (
    Any,
    Dict
)
from urllib.parse import urlencode
from collections import OrderedDict


class VitexAuth:
    def __init__(self, api_key: str, secret_key: str):
        self.api_key: str = api_key
        self.secret_key: str = secret_key

    @staticmethod
    def keysort(dictionary: Dict[str, str]) -> Dict[str, str]:
        return OrderedDict(sorted(dictionary.items(), key=lambda t: t[0]))

    def sign(self, sorted_params: Dict[str, str]) -> str:
        encoded_params = urlencode(sorted_params)
        signature = hmac.new(self.secret_key.encode("utf8"), encoded_params.encode("utf8"), hashlib.sha256)
        signature_str = signature.hexdigest()
        return signature_str

    def generate_auth_dict(self,
                           params: Dict[str, Any] = None
                           ) -> Dict[str, Any]:
        timestamp = int(time.time() * 1000)
        new_params = {
            "key": self.api_key,
            "timestamp": timestamp
        }

        if params is not None:
            new_params.update(params)

        sorted_params = self.keysort(new_params)
        signature = self.sign(sorted_params)
        sorted_params["signature"] = signature
        return sorted_params
