# -*- coding: utf-8 -*-
import logging
import threading
import time
import asyncio
from typing import Optional, Dict, Any

import aiohttp

from .constants import VK_API_VERSION

logger = logging.getLogger(__name__)


class LongPollWorker:
    """Поток для получения сообщений через User LongPoll"""
    
    def __init__(self, bot):
        self.bot = bot
        self._thread = None
        self._running = False
        
        self._lp_server = ""
        self._lp_key = ""
        self._lp_ts = ""
        self._error_count = 0
        self._ready = False

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._running = True
        self._thread = threading.Thread(target=self._worker, daemon=True)
        self._thread.start()
        logger.info("✅ LongPoll поток запущен")

    def stop(self):
        self._running = False

    def _worker(self):
        logger.info("👂 LongPoll worker начал работу")
        
        while self._running:
            try:
                if not self._get_server():
                    self._error_count += 1
                    wait = min(5 * (2 ** min(self._error_count, 5)), 300)
                    logger.warning(f"⏳ Ошибка, пауза {wait}с")
                    time.sleep(wait)
                    continue
                
                self._error_count = 0
                self._ready = True
                logger.info(f"✅ LongPoll готов. Слушаю чат {self.bot.source_peer_id}")
                
                while self._running:
                    try:
                        lp = self._check()
                        if not lp:
                            time.sleep(1)
                            continue
                        
                        if "failed" in lp:
                            if self._handle_error(lp):
                                break
                            continue
                        
                        new_ts = lp.get("ts")
                        if new_ts:
                            self._lp_ts = str(new_ts)
                        
                        updates = lp.get("updates", []) or []
                        if updates:
                            self._process_updates(updates)
                        
                    except Exception as e:
                        logger.error(f"❌ Ошибка цикла: {e}")
                        time.sleep(5)
                        
            except Exception as e:
                logger.error(f"❌ Критическая ошибка: {e}")
                time.sleep(10)

    def _get_server(self) -> bool:
        data = {
            "access_token": self.bot.observer.access_token,
            "v": VK_API_VERSION,
            "lp_version": 3
        }
        ret = self.bot.observer._vk.call(
            self.bot.observer._vk.post("messages.getLongPollServer", data)
        )

        if "error" in ret:
            return False

        resp = ret.get("response", {})
        self._lp_server = str(resp.get("server", "")).strip()
        self._lp_key = str(resp.get("key", "")).strip()
        self._lp_ts = str(resp.get("ts", "")).strip()

        return bool(self._lp_server and self._lp_key and self._lp_ts)

    def _check(self) -> Optional[Dict]:
        if not self._lp_server:
            return None

        server = "https://" + self._lp_server
        data = {
            "act": "a_check",
            "key": self._lp_key,
            "ts": self._lp_ts,
            "wait": 25,
            "mode": 2,
            "version": 3
        }

        try:
            timeout = aiohttp.ClientTimeout(total=30)
            async def req():
                async with aiohttp.ClientSession(timeout=timeout) as s:
                    async with s.get(server, params=data) as r:
                        return await r.json()
            return asyncio.run(req())
        except:
            return {"failed": 2}

    def _handle_error(self, lp: Dict) -> bool:
        code = lp.get("failed")
        if code == 1:
            new_ts = lp.get("ts")
            if new_ts:
                self._lp_ts = str(new_ts)
            return False
        elif code == 2:
            logger.warning("🔄 Ключ устарел")
            return True
        return False

    def _process_updates(self, updates: list):
        for upd in updates:
            if isinstance(upd, list) and len(upd) > 3 and upd[0] == 4:
                if upd[3] == self.bot.source_peer_id:
                    msg_id = upd[1]
                    items = self.bot.observer.get_by_id([msg_id])
                    for item in items:
                        self.bot.message_queue.put(item)
