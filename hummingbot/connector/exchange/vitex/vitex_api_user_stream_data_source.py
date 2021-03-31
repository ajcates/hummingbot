#!/usr/bin/env python

import asyncio
import logging
import time
from typing import (
    Any,
    AsyncIterable,
    Dict,
    Optional
)
import ujson
import websockets
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.logger import HummingbotLogger

STREAM_URL = "wss://api.vitex.net/v2/ws"


class VitexAPIUserStreamDataSource(UserStreamTrackerDataSource):
    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _bausds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._bausds_logger is None:
            cls._bausds_logger = logging.getLogger(__name__)
        return cls._bausds_logger

    def __init__(
            self,
            vite_address: Optional[str] = None
    ):
        self._vite_address: str = vite_address
        self._last_recv_time: float = 0
        super().__init__()

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    async def _inner_messages(self, ws: websockets.WebSocketClientProtocol) -> AsyncIterable[Any]:
        # Terminate the recv() loop as soon as the next message timed out, so the outer loop can reconnect.
        try:
            while True:
                try:
                    msg: str = await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)
                    msg_json = ujson.loads(msg)
                    event = msg_json["event"]
                    if event == "push":
                        self._last_recv_time = time.time()
                        yield msg_json
                except asyncio.TimeoutError:
                    try:
                        await ws.send('{"command":"ping"}')
                        self._last_recv_time = time.time()
                    except asyncio.TimeoutError:
                        raise
        except asyncio.TimeoutError:
            self.logger().warning("WebSocket ping timed out. Going to reconnect...")
            return
        except websockets.exceptions.ConnectionClosed:
            return
        finally:
            await ws.close()

    async def listen_for_user_stream(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                topic = f"order.{self._vite_address}"
                async with websockets.connect(STREAM_URL) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    subscribe_request: Dict[str, Any] = {
                        "command": "sub",
                        "params": [topic]
                    }
                    # Subscribe topics
                    await ws.send(ujson.dumps(subscribe_request))
                    # Receive and parse messages
                    async for msg in self._inner_messages(ws):
                        user_message = msg["data"]
                        output.put_nowait(user_message)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)
