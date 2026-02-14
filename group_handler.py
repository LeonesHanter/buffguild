# -*- coding: utf-8 -*-
"""
GroupHandler - обработчик сообщества для Observer
С защитой от rate limit и экспоненциальными задержками
"""
import logging
import random
import threading
import time
import asyncio
from typing import Any, Dict, List, Optional, Tuple

from .constants import VK_API_VERSION
from .utils import jitter_sleep

logger = logging.getLogger(__name__)


class GroupHandler:
    """Обработчик сообщества для Observer с защитой от rate limit"""
    
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
        
        # ============= ЗАЩИТА ОТ RATE LIMIT =============
        self._rate_limit_until = 0          # Время, до которого длится rate limit
        self._consecutive_failures = 0       # Количество последовательных ошибок
        self._last_server_request = 0        # Время последнего запроса сервера
        self.MIN_REQUEST_INTERVAL = 5        # Минимум 5 секунд между запросами
        self.MAX_RATE_LIMIT_WAIT = 3600      # Максимальное время ожидания (1 час)
        self.MAX_CONSECUTIVE_FAILURES = 10   # Максимум ошибок до переключения
        # ================================================
        
        # Кэш сообщений
        self._history_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
        self._cache_ttl = 3
        self._cache_lock = threading.Lock()
        
        logger.info(f"👥 GroupHandler создан: {self.name} (ID: {self.group_id})")
    
    def is_valid(self) -> bool:
        """Упрощённая проверка без requests"""
        if not self.access_token or not isinstance(self.access_token, str):
            return False
        
        token_len = len(self.access_token.strip())
        if token_len < 50:
            return False
        
        if not self.group_id or self.group_id >= 0:
            return False
        
        return True
    
    async def _group_get_long_poll_server(self) -> Dict[str, Any]:
        """Получает LongPoll сервер для сообщества"""
        data = {
            "access_token": self.access_token,
            "v": VK_API_VERSION,
            "group_id": abs(self.group_id),
        }
        return await self._vk.post("groups.getLongPollServer", data)
    
    def get_long_poll_server(self) -> bool:
        """
        Получение LongPoll сервера с оптимизированной защитой
        """
        now = time.time()
        
        # ============= ПРОВЕРКА RATE LIMIT =============
        if now < self._rate_limit_until:
            wait_remaining = int(self._rate_limit_until - now)
            logger.warning(f"⏳ В rate limit, осталось {wait_remaining}с")
            return False
        # ===============================================
        
        # Просто проверяем, что запросы не чаще 5 секунд
        if now - self._last_server_request < self.MIN_REQUEST_INTERVAL:
            wait_remaining = int(self.MIN_REQUEST_INTERVAL - (now - self._last_server_request))
            logger.debug(f"⏳ Слишком частые запросы, нужно подождать {wait_remaining}с")
            return False
        
        self._last_server_request = now
        
        try:
            ret = self._vk.call(self._group_get_long_poll_server())
            
            if "error" in ret:
                err = ret["error"]
                error_code = err.get('error_code')
                error_msg = err.get('error_msg', '')
                
                # ============= ОБРАБОТКА RATE LIMIT =============
                if error_code == 29:  # Rate limit reached
                    self._consecutive_failures += 1
                    
                    wait_time = 60 * (2 ** (self._consecutive_failures - 1))
                    wait_time = min(wait_time, self.MAX_RATE_LIMIT_WAIT)
                    
                    self._rate_limit_until = now + wait_time
                    
                    logger.error(
                        f"⛔ Rate limit! Пауза {wait_time}с (попытка {self._consecutive_failures})"
                    )
                    return False
                # ================================================
                
                logger.warning(f"⚠️ LongPoll error {error_code}: {error_msg}")
                return False
            
            # Успех - сбрасываем счётчик ошибок
            self._consecutive_failures = 0
            self._rate_limit_until = 0
            
            resp = ret.get("response", {})
            self._lp_server = str(resp.get("server", "")).strip()
            self._lp_key = str(resp.get("key", "")).strip()
            self._lp_ts = str(resp.get("ts", "")).strip()
            
            if not self._lp_server or not self._lp_key or not self._lp_ts:
                logger.error("❌ LongPoll: missing server/key/ts")
                return False
            
            logger.info(f"✅ LongPoll OK: {self.name}")
            logger.debug(f"   Server: {self._lp_server}")
            logger.debug(f"   TS: {self._lp_ts}")
            return True
            
        except Exception as e:
            logger.error(f"❌ LongPoll init error: {e}")
            return False
    
    def is_rate_limited(self) -> bool:
        """Проверяет, находимся ли мы в rate limit"""
        return time.time() < self._rate_limit_until
    
    def get_rate_limit_remaining(self) -> int:
        """Сколько секунд осталось в rate limit"""
        remaining = self._rate_limit_until - time.time()
        return max(0, int(remaining))
    
    def reset_rate_limit(self):
        """Принудительный сброс rate limit"""
        self._rate_limit_until = 0
        self._consecutive_failures = 0
        logger.info("🔄 Rate limit сброшен принудительно")
    
    def should_switch_to_user(self) -> bool:
        """Проверяет, нужно ли переключаться на пользовательский токен"""
        return self._consecutive_failures >= self.MAX_CONSECUTIVE_FAILURES
    
    def handle_longpoll_error(self, error_code: int, response_ts: Optional[str] = None) -> Tuple[bool, bool]:
        """
        Обработка ошибок LongPoll
        
        Returns:
            (should_continue, should_switch)
        """
        if error_code == 1:
            if response_ts:
                self._lp_ts = str(response_ts)
                logger.info(f"🔄 LongPoll: обновлен ts на {self._lp_ts}")
            return True, False
            
        elif error_code == 2:
            self._consecutive_failures += 1
            logger.info(f"🔄 LongPoll: ключ истёк (error 2), попытка {self._consecutive_failures}")
            
            if self._consecutive_failures >= self.MAX_CONSECUTIVE_FAILURES:
                logger.critical(f"🚫 Слишком много ошибок ({self._consecutive_failures})")
                return False, True
            
            return False, False
            
        elif error_code == 3:
            logger.warning(f"🔄 LongPoll: информация потеряна (error 3)")
            self._lp_server = ""
            self._lp_key = ""
            self._lp_ts = ""
            return False, False
            
        elif error_code == 4:
            logger.error(f"❌ LongPoll: неверная версия протокола (error 4)")
            return False, False
            
        return False, False
    
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
            "group_id": abs(self.group_id),
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
        """Отправка сообщения"""
        logger.info(f"📤 GroupHandler отправляет в {peer_id}: {text[:50]}...")
        
        if self.is_rate_limited():
            remaining = self.get_rate_limit_remaining()
            logger.warning(f"⏳ Отправка отложена: группа в rate limit, осталось {remaining}с")
            return False, "RATE_LIMITED"
        
        try:
            ret = self._vk.call(
                self._messages_send(peer_id, text, forward_msg_id, reply_to_cmid)
            )
            
            if "error" in ret:
                err = ret["error"]
                code = int(err.get("error_code", 0))
                msg = str(err.get("error_msg", ""))
                
                logger.error(f"❌ Ошибка отправки {code}: {msg}")
                
                if code == 14:
                    return False, "CAPTCHA"
                if code == 9:
                    return False, "FLOOD"
                if code == 29:
                    self._consecutive_failures += 1
                    wait_time = min(60 * (2 ** (self._consecutive_failures - 1)), 3600)
                    self._rate_limit_until = time.time() + wait_time
                    return False, "RATE_LIMITED"
                if code == 917:
                    logger.critical(f"🚫 НЕТ ДОСТУПА К ЧАТУ {peer_id}! Проверь, добавлен ли бот в беседу")
                    return False, "NO_ACCESS"
                if code in (4, 5, 27, 125):
                    return False, "GROUP_AUTH"
                
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
            "group_id": abs(self.group_id),
        }
        return await self._vk.post("messages.getHistory", data)
    
    def get_history(self, peer_id: int, count: int = 20) -> List[Dict[str, Any]]:
        """Получение истории сообщений"""
        if self.is_rate_limited():
            logger.warning(f"⏳ getHistory отложен: группа в rate limit")
            return []
        
        try:
            ret = self._vk.call(self._messages_get_history(peer_id, count))
            if "error" in ret:
                err = ret["error"]
                error_code = err.get('error_code')
                
                if error_code == 29:
                    self._consecutive_failures += 1
                    wait_time = min(60 * (2 ** (self._consecutive_failures - 1)), 3600)
                    self._rate_limit_until = time.time() + wait_time
                    logger.warning(f"⏳ Rate limit при getHistory, пауза {wait_time}с")
                
                logger.error(f"❌ {self.name}: getHistory error {error_code}")
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
            "group_id": abs(self.group_id),
        }
        return await self._vk.post("messages.getById", data)
    
    def get_by_id(self, message_ids: List[int]) -> List[Dict[str, Any]]:
        """Получение сообщений по ID"""
        if self.is_rate_limited():
            logger.warning(f"⏳ getById отложен: группа в rate limit")
            return []
        
        try:
            ret = self._vk.call(self._messages_get_by_id(message_ids))
            if "error" in ret:
                err = ret["error"]
                error_code = err.get('error_code')
                
                if error_code == 29:
                    self._consecutive_failures += 1
                    wait_time = min(60 * (2 ** (self._consecutive_failures - 1)), 3600)
                    self._rate_limit_until = time.time() + wait_time
                    logger.warning(f"⏳ Rate limit при getById, пауза {wait_time}с")
                
                logger.error(f"❌ {self.name}: getById error {error_code}")
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
    
    def send_reaction_success(self, peer_id: int, cmid: int) -> bool:
        """Отправка реакции успеха"""
        if cmid is None:
            return False
        
        if self.is_rate_limited():
            logger.warning(f"⏳ sendReaction отложен: группа в rate limit")
            return False
        
        jitter_sleep()
        data = {
            "access_token": self.access_token,
            "v": VK_API_VERSION,
            "peer_id": int(peer_id),
            "cmid": int(cmid),
            "reaction_id": 16,
            "group_id": abs(self.group_id),
        }
        try:
            ret = self._vk.call(self._vk.post("messages.sendReaction", data))
            if "error" in ret:
                err = ret["error"]
                error_code = err.get('error_code')
                
                if error_code == 29:
                    self._consecutive_failures += 1
                    wait_time = min(60 * (2 ** (self._consecutive_failures - 1)), 3600)
                    self._rate_limit_until = time.time() + wait_time
                    logger.warning(f"⏳ Rate limit при sendReaction, пауза {wait_time}с")
                
                logger.error(f"❌ {self.name}: sendReaction error {error_code}")
                return False

            logger.info(f"🙂 {self.name}: реакция 🎉 поставлена (peer={peer_id} cmid={cmid})")
            return True
        except Exception as e:
            logger.error(f"❌ {self.name}: sendReaction exception {e}")
            return False
    
    def delete_message(self, peer_id: int, message_id: int) -> bool:
        """Удаление сообщения"""
        if not self.access_token:
            return False
        
        if self.is_rate_limited():
            logger.warning(f"⏳ deleteMessage отложен: группа в rate limit")
            return False
        
        try:
            jitter_sleep()
            data = {
                "access_token": self.access_token,
                "v": VK_API_VERSION,
                "peer_id": int(peer_id),
                "cmids": str(int(message_id)),
                "delete_for_all": 1,
                "group_id": abs(self.group_id),
            }
            ret = self._vk.call(self._vk.post("messages.delete", data))
            if "error" in ret:
                err = ret["error"]
                error_code = err.get('error_code')
                
                if error_code == 29:
                    self._consecutive_failures += 1
                    wait_time = min(60 * (2 ** (self._consecutive_failures - 1)), 3600)
                    self._rate_limit_until = time.time() + wait_time
                    logger.warning(f"⏳ Rate limit при deleteMessage, пауза {wait_time}с")
                
                logger.error(f"❌ {self.name}: delete error {error_code}")
                return False
            return True
        except Exception as e:
            logger.error(f"❌ {self.name}: delete exception {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Получить статистику работы группы"""
        return {
            'name': self.name,
            'group_id': self.group_id,
            'rate_limited': self.is_rate_limited(),
            'rate_limit_remaining': self.get_rate_limit_remaining(),
            'consecutive_failures': self._consecutive_failures,
            'should_switch': self.should_switch_to_user(),
            'last_server_request': self._last_server_request,
            'longpoll_initialized': bool(self._lp_server),
            'cache_size': len(self._history_cache)
        }


class GroupProxy:
    """
    Прокси-класс для совместимости с TokenHandler
    Позволяет использовать групповой токен для отправки сообщений
    """
    def __init__(self, group_handler, source_chat_id, vk):
        self.group_handler = group_handler
        self._vk = vk
        self.source_peer_id = 2000000000 + source_chat_id if source_chat_id else 0
        self.name = group_handler.name
        self.id = f"group_{group_handler.group_id}"
        self.access_token = group_handler.access_token
        self.class_type = "observer"
        self.enabled = True
        self.owner_vk_id = 0
        
    def send_to_peer(self, peer_id, text, forward_msg_id=None, reply_to_cmid=None):
        """Отправка сообщения через групповой handler"""
        return self.group_handler.send_to_peer(peer_id, text, forward_msg_id, reply_to_cmid)
        
    def get_by_id(self, message_ids):
        """Получение сообщений по ID"""
        return self.group_handler.get_by_id(message_ids)
        
    def get_history_cached(self, peer_id, count=20):
        """Получение истории с кэшем"""
        return self.group_handler.get_history_cached(peer_id, count)
        
    def invalidate_cache(self, peer_id=None):
        """Инвалидация кэша"""
        return self.group_handler.invalidate_cache(peer_id)
        
    def send_reaction_success(self, peer_id, cmid):
        """Отправка реакции"""
        return self.group_handler.send_reaction_success(peer_id, cmid)
        
    def delete_message(self, peer_id, message_id):
        """Удаление сообщения"""
        return self.group_handler.delete_message(peer_id, message_id)
        
    def get_health_info(self):
        """Информация о состоянии"""
        return {
            "id": self.id,
            "name": self.name,
            "class": "observer",
            "enabled": True,
            "captcha_paused": False,
            "captcha_until": 0,
            "needs_manual_voices": False,
            "voices": 0,
            "level": 0,
            "temp_races_count": 0,
            "successful_buffs": 0,
            "total_attempts": 0,
            "success_rate": 0.0,
            "owner_vk_id": 0,
            "races": [],
            "temp_races": [],
            "social_cd": "-",
        }
