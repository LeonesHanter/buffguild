# -*- coding: utf-8 -*-
"""
GroupHandler - обработчик сообщества для Observer
Использует токен сообщества вместо пользовательского
"""
import logging
import random
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from .constants import VK_API_VERSION
from .utils import jitter_sleep

logger = logging.getLogger(__name__)


class GroupHandler:
    """Обработчик сообщества для Observer"""
    
    def __init__(self, cfg: Dict[str, Any], vk):
        self.group_id: int = int(cfg.get("group_id", 0))
        self.access_token: str = cfg.get("access_token", "")
        self.name: str = cfg.get("group_name", f"Group-{self.group_id}")
        
        self._vk = vk
        self._lock = threading.RLock()
        
        # Для LongPoll сообщества
        self._lp_server: str = ""
        self._lp_key: str = ""
        self._lp_ts: str = ""
        
        # Кэш сообщений
        self._history_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
        self._cache_ttl = 3
        self._cache_lock = threading.Lock()
        
        logger.info(f"👥 GroupHandler создан: {self.name} (ID: {self.group_id})")
    
    def is_valid(self) -> bool:
        """Проверяет валидность конфигурации сообщества"""
        # Упрощенная проверка для токенов группы
        if not self.access_token or not isinstance(self.access_token, str):
            logger.warning(f"⚠️ GroupHandler: access_token отсутствует или не строка")
            return False
        
        # Токен должен быть достаточно длинным
        token_len = len(self.access_token.strip())
        if token_len < 50:
            logger.warning(f"⚠️ GroupHandler: access_token слишком короткий ({token_len} chars)")
            return False
        
        if not self.group_id or self.group_id >= 0:
            logger.warning(f"⚠️ GroupHandler: group_id должен быть отрицательным, получен: {self.group_id}")
            return False
        
        # Проверяем через API
        try:
            import requests
            
            # Быстрая проверка токена через groups.getById
            response = requests.get(
                "https://api.vk.com/method/groups.getById",
                params={
                    "group_id": str(abs(self.group_id)),
                    "access_token": self.access_token,
                    "v": VK_API_VERSION
                },
                timeout=5
            ).json()
            
            if "error" in response:
                error_code = response["error"].get("error_code")
                error_msg = response["error"].get("error_msg", "")
                logger.error(f"❌ GroupHandler: токен невалиден ({error_code}: {error_msg})")
                return False
            
            logger.info(f"✅ GroupHandler валиден: {self.name} (ID: {self.group_id})")
            return True
            
        except Exception as e:
            logger.warning(f"⚠️ GroupHandler: не удалось проверить токен через API: {e}")
            # Возвращаем True, если базовая проверка прошла
            if token_len > 50 and self.group_id < 0:
                logger.info(f"✅ GroupHandler: базовая проверка пройдена, продолжаем")
                return True
            return False
    
    async def _group_get_long_poll_server(self) -> Dict[str, Any]:
        """Получает LongPoll сервер для сообщества"""
        data = {
            "access_token": self.access_token,
            "v": VK_API_VERSION,
            "group_id": abs(self.group_id),  # Без минуса для API
        }
        return await self._vk.post("groups.getLongPollServer", data)
    
    def get_long_poll_server(self) -> bool:
        """Инициализирует LongPoll для сообщества"""
        try:
            ret = self._vk.call(self._group_get_long_poll_server())
            
            if "error" in ret:
                err = ret["error"]
                logger.error(f"❌ Group LongPoll error {err.get('error_code')} {err.get('error_msg')}")
                return False
            
            resp = ret.get("response", {})
            self._lp_server = str(resp.get("server", "")).strip()
            self._lp_key = str(resp.get("key", "")).strip()
            self._lp_ts = str(resp.get("ts", "")).strip()
            
            if not self._lp_server or not self._lp_key or not self._lp_ts:
                logger.error("❌ Group LongPoll: missing server/key/ts")
                return False
            
            logger.info(f"✅ Group LongPoll initialized for {self.name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Group LongPoll init error: {e}")
            return False
    
    async def _messages_send(
        self,
        peer_id: int,
        text: str,
        forward_msg_id: Optional[int] = None,
        reply_to_cmid: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Отправка сообщения от имени сообщества"""
        jitter_sleep()
        data: Dict[str, Any] = {
            "access_token": self.access_token,
            "v": VK_API_VERSION,
            "peer_id": int(peer_id),
            "message": text,
            "random_id": random.randrange(1, 2_000_000_000),
            "disable_mentions": 1,
            "group_id": abs(self.group_id),  # Без минуса для API
        }
        
        if forward_msg_id:
            data["forward_messages"] = str(int(forward_msg_id))
        elif reply_to_cmid:
            data["reply_to"] = str(int(reply_to_cmid))
            
        return await self._vk.post("messages.send", data)
    
    def send_to_peer(
        self,
        peer_id: int,
        text: str,
        forward_msg_id: Optional[int] = None,
        reply_to_cmid: Optional[int] = None,
    ) -> Tuple[bool, str]:
        """Отправка сообщения (публичный интерфейс)"""
        try:
            ret = self._vk.call(
                self._messages_send(peer_id, text, forward_msg_id, reply_to_cmid)
            )
            
            if "error" in ret:
                err = ret["error"]
                code = int(err.get("error_code", 0))
                msg = str(err.get("error_msg", ""))
                
                if code == 14:
                    logger.warning(f"⛔ {self.name}: CAPTCHA detected")
                    return False, "CAPTCHA"
                if code == 9:
                    return False, "FLOOD"
                if code in (4, 5, 27, 125):
                    # 27 - Код для сообщества, если нет прав на отправку
                    # 125 - неверный идентификатор сообщества
                    return False, "GROUP_AUTH"
                
                logger.error(f"❌ {self.name}: send error {code} {msg}")
                return False, "ERROR"
            
            message_id = ret.get("response", 0)
            return True, f"OK:{message_id}"
            
        except Exception as e:
            logger.error(f"❌ {self.name}: send exception {e}")
            return False, "ERROR"
    
    async def _messages_get_history(self, peer_id: int, count: int = 20) -> Dict[str, Any]:
        """Получение истории сообщений"""
        data = {
            "access_token": self.access_token,
            "v": VK_API_VERSION,
            "peer_id": int(peer_id),
            "count": int(count),
        }
        
        # Для сообщества добавляем group_id
        data["group_id"] = abs(self.group_id)
            
        return await self._vk.post("messages.getHistory", data)
    
    def get_history(self, peer_id: int, count: int = 20) -> List[Dict[str, Any]]:
        """Получение истории сообщений (публичный интерфейс)"""
        try:
            ret = self._vk.call(self._messages_get_history(peer_id, count))
            if "error" in ret:
                err = ret["error"]
                logger.error(f"❌ {self.name}: getHistory error {err.get('error_code')} {err.get('error_msg')}")
                return []
            return ret.get("response", {}).get("items", []) or []
        except Exception as e:
            logger.error(f"❌ {self.name}: getHistory exception {e}")
            return []
    
    def get_history_cached(self, peer_id: int, count: int = 20) -> List[Dict[str, Any]]:
        """Получение истории с кэшированием"""
        cache_key = f"history_{peer_id}_{count}"
        now = time.time()
        
        with self._cache_lock:
            if cache_key in self._history_cache:
                cached_time, cached_data = self._history_cache[cache_key]
                if now - cached_time < self._cache_ttl:
                    return cached_data.copy()
        
        fresh_data = self.get_history(peer_id, count)
        
        with self._cache_lock:
            self._history_cache[cache_key] = (now, fresh_data.copy())
        
        return fresh_data
    
    async def _messages_get_by_id(self, message_ids: List[int]) -> Dict[str, Any]:
        """Получение сообщений по ID"""
        data = {
            "access_token": self.access_token,
            "v": VK_API_VERSION,
            "message_ids": ",".join(str(int(x)) for x in message_ids),
        }
        
        data["group_id"] = abs(self.group_id)
            
        return await self._vk.post("messages.getById", data)
    
    def get_by_id(self, message_ids: List[int]) -> List[Dict[str, Any]]:
        """Получение сообщений по ID (публичный интерфейс)"""
        try:
            ret = self._vk.call(self._messages_get_by_id(message_ids))
            if "error" in ret:
                err = ret["error"]
                logger.error(f"❌ {self.name}: getById error {err.get('error_code')} {err.get('error_msg')}")
                return []
            return ret.get("response", {}).get("items", []) or []
        except Exception as e:
            logger.error(f"❌ {self.name}: getById exception {e}")
            return []
    
    def invalidate_cache(self, peer_id: Optional[int] = None) -> None:
        """Инвалидация кэша"""
        with self._cache_lock:
            if peer_id is None:
                self._history_cache.clear()
                return
            keys_to_delete = [
                k for k in self._history_cache.keys()
                if k.startswith(f"history_{peer_id}_")
            ]
            for k in keys_to_delete:
                del self._history_cache[k]
