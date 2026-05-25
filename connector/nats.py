from __future__ import annotations

import asyncio
from typing import Any, Callable

import nats as nats_lib
from nats.aio.client import Client as NatsClient
from nats.errors import ConnectionClosedError, NoServersError, TimeoutError as NatsTimeoutError
from loguru import logger


class NatsConnector:
    """NATS messaging connector."""

    def __init__(self, servers: str | list[str] = "nats://localhost:4222", user: str | None = None, password: str | None = None, token: str | None = None, connect_timeout: int = 10):
        if isinstance(servers, str):
            self.servers = [servers]
        else:
            self.servers = servers
        self.user = user
        self.password = password
        self.token = token
        self.connect_timeout = connect_timeout
        self._client: NatsClient | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    async def connect_async(self):
        logger.info(f"Connecting to NATS at {self.servers}")
        kwargs: dict[str, Any] = {
            "servers": self.servers,
            "connect_timeout": self.connect_timeout,
        }
        if self.user and self.password:
            kwargs["user"] = self.user
            kwargs["password"] = self.password
        if self.token:
            kwargs["token"] = self.token
        try:
            self._client = await nats_lib.connect(**kwargs)
        except (NoServersError, ConnectionClosedError, OSError) as e:
            logger.error(f"Failed to connect to NATS at {self.servers}: {e}")
            raise ConnectionError(f"NATS connection failed: {e}") from e
        return self

    def connect(self):
        loop = self._get_or_create_loop()
        loop.run_until_complete(self.connect_async())
        return self

    async def close_async(self):
        if self._client:
            try:
                await self._client.drain()
            except Exception as e:
                logger.warning(f"Error draining NATS connection: {e}")
            self._client = None
            logger.info("NATS connection closed")

    def close(self):
        loop = self._get_or_create_loop()
        loop.run_until_complete(self.close_async())

    @property
    def client(self) -> NatsClient:
        if self._client is None:
            raise RuntimeError("Not connected. Call connect() first.")
        return self._client

    async def publish_async(self, subject: str, payload: bytes):
        try:
            await self.client.publish(subject, payload)
        except (ConnectionClosedError, NatsTimeoutError) as e:
            logger.error(f"Failed to publish to '{subject}': {e}")
            raise

    def publish(self, subject: str, payload: bytes):
        loop = self._get_or_create_loop()
        loop.run_until_complete(self.publish_async(subject, payload))

    async def subscribe_async(self, subject: str, callback: Callable):
        try:
            return await self.client.subscribe(subject, cb=callback)
        except (ConnectionClosedError, NatsTimeoutError) as e:
            logger.error(f"Failed to subscribe to '{subject}': {e}")
            raise

    async def request_async(self, subject: str, payload: bytes, timeout: float = 5.0) -> bytes:
        try:
            msg = await self.client.request(subject, payload, timeout=timeout)
            return msg.data
        except NatsTimeoutError as e:
            logger.error(f"Request to '{subject}' timed out after {timeout}s")
            raise
        except ConnectionClosedError as e:
            logger.error(f"Failed to request '{subject}': {e}")
            raise

    def _get_or_create_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                self._loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._loop)
        return self._loop

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
