# -*- coding: utf-8 -*-
import asyncio
import aiohttp
import logging
import threading
from typing import Any, Dict, Optional

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from .constants import VK_API_BASE, VK_API_VERSION

logger = logging.getLogger(__name__)


class ResilientVKClient:
    """Устойчивый клиент VK с повторными попытками"""

    def __init__(self):
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._ready = threading.Event()
        self._session: Optional[aiohttp.ClientSession] = None
        self._stopping = False

        self._thread.start()
        if not self._ready.wait(timeout=10):
            raise RuntimeError("VK client init timeout")

    def _run_loop(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._init())
        self._ready.set()
        self._loop.run_forever()

    async def _init(self):
        timeout = aiohttp.ClientTimeout(total=30)
        connector = aiohttp.TCPConnector(limit=150, ttl_dns_cache=300)
        self._session = aiohttp.ClientSession(timeout=timeout, connector=connector)

    def call(self, coro):
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return fut.result(timeout=30)

    # ============= НОВЫЙ МЕТОД С RETRY =============
    def call_with_retry(self, method: str, data: Dict[str, Any], max_retries: int = 3) -> Dict[str, Any]:
        """
        Вызвать метод VK API с повторными попытками при ошибках.
        Использует tenacity с экспоненциальной задержкой.
        """
        @retry(
            stop=stop_after_attempt(max_retries),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
            before_sleep=lambda retry_state: logger.warning(
                f"⏳ Retry {method} (попытка {retry_state.attempt_number}/{max_retries})"
            )
        )
        async def _call_with_retry():
            if not self._session:
                raise RuntimeError("VK session not ready")
            url = f"{VK_API_BASE}/{method}"
            async with self._session.post(url, data=data) as resp:
                return await resp.json()

        try:
            result = self.call(_call_with_retry())
            if "error" in result:
                logger.error(f"❌ VK API {method} error: {result['error']}")
            return result
        except Exception as e:
            logger.error(f"❌ VK API {method} failed after {max_retries} retries: {e}")
            return {"error": {"error_code": -1, "error_msg": str(e)}}
    # ================================================

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
    )
    async def post_with_retry(self, method: str, data: Dict[str, Any]) -> Dict[str, Any]:
        if not self._session:
            raise RuntimeError("VK session not ready")
        url = f"{VK_API_BASE}/{method}"
        async with self._session.post(url, data=data) as resp:
            return await resp.json()

    async def raw_post(self, url: str, data: Dict[str, Any]) -> Dict[str, Any]:
        if not self._session:
            raise RuntimeError("VK session not ready")
        async with self._session.post(url, data=data) as resp:
            return await resp.json()

    async def post(self, method: str, data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return await self.post_with_retry(method, data)
        except Exception as e:
            logging.error(f"❌ VK API ошибка после 3 попыток: {e}")
            return {"error": {"error_code": -1, "error_msg": str(e)}}
