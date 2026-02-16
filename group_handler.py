# -*- coding: utf-8 -*-
"""
GroupHandler - обработчик сообщества для Observer
"""
import logging
import threading
import time
import json
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

        # ============= ЗАЩИТА ОТ RATE LIMIT =============
        self._rate_limit_until = 0
        self._consecutive_failures = 0
        self._last_server_request = 0
        self.MIN_REQUEST_INTERVAL = 5
        self.MAX_RATE_LIMIT_WAIT = 3600
        self.MAX_CONSECUTIVE_FAILURES = 10
        # ================================================

        # Кэш сообщений
        self._history_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
        self._cache_ttl = 3
        self._cache_lock = threading.Lock()

        logger.info(f"👥 GroupHandler создан: {self.name} (ID: {self.group_id})")

    # ──────────────────────────────────────────────
    #  Валидация и LongPoll
    # ──────────────────────────────────────────────
    def is_valid(self) -> bool:
        """Проверка что GroupHandler корректно настроен"""
        return bool(self.group_id and self.access_token)

    def get_long_poll_server(self) -> bool:
        """Проверка доступности Group LongPoll"""
        try:
            params = {'group_id': self.group_id}
            ret = self._api_call('groups.getLongPollServer', params)
            if 'error' in ret:
                err = ret['error']
                logger.warning(
                    f"⚠️ [{self.name}] LongPoll недоступен: "
                    f"{err.get('error_code')} {err.get('error_msg')}"
                )
                return False
            logger.info(f"✅ [{self.name}] LongPoll доступен")
            return True
        except Exception as e:
            logger.error(f"❌ [{self.name}] LongPoll ошибка: {e}")
            return False

    def is_rate_limited(self) -> bool:
        return time.time() < self._rate_limit_until

    def get_rate_limit_remaining(self) -> int:
        remaining = self._rate_limit_until - time.time()
        return max(0, int(remaining))

    # ──────────────────────────────────────────────
    #  API вызов
    # ──────────────────────────────────────────────
    def _api_call(self, method: str, params: Dict[str, Any]) -> Dict:
        """Прямой вызов VK API с токеном группы"""
        call_params = dict(params)
        call_params['access_token'] = self.access_token
        call_params['v'] = VK_API_VERSION

        debug_params = {
            k: v for k, v in call_params.items()
            if k != 'access_token'
        }
        logger.debug(f"🔧 [{self.name}] API {method}: {debug_params}")

        try:
            ret = self._vk.call(self._vk.post(method, call_params))
            logger.debug(f"🔧 [{self.name}] API {method} ответ: {ret}")
            return ret
        except Exception as e:
            logger.error(f"❌ {self.name}: API {method} exception: {e}")
            return {'error': {'error_code': -1, 'error_msg': str(e)}}

    # ──────────────────────────────────────────────
    #  ОТПРАВКА СООБЩЕНИЯ
    # ──────────────────────────────────────────────
    def send_message(
        self,
        peer_id: int,
        text: str,
        reply_to_cmid: Optional[int] = None,
        forward_msg_id: Optional[int] = None
    ) -> Tuple[bool, Optional[Dict]]:
        """
        Отправить сообщение от имени группы.

        Групповой токен НЕ возвращает message_id (всегда 0),
        но возвращает conversation_message_id (cmid).
        Используем cmid как effective_id.
        """
        logger.info(
            f"📤 [{self.name}] Отправка в {peer_id}: "
            f"'{text[:50]}...'"
        )

        if self.is_rate_limited():
            remaining = self.get_rate_limit_remaining()
            logger.warning(
                f"⏳ Отправка отложена: rate limit, "
                f"осталось {remaining}с"
            )
            return False, None

        jitter_sleep()

        random_id = int(time.time() * 1000000)

        params = {
            'peer_ids': str(int(peer_id)),
            'message': text,
            'random_id': random_id,
            'disable_mentions': 1,
        }

        # Reply на сообщение В ТОМ ЖЕ чате
        if reply_to_cmid:
            params['forward'] = json.dumps({
                'peer_id': int(peer_id),
                'conversation_message_ids': [int(reply_to_cmid)],
                'is_reply': True
            })
            logger.info(
                f"↩️ Reply на cmid={reply_to_cmid} в чате {peer_id}"
            )
        elif forward_msg_id:
            params['forward_messages'] = str(int(forward_msg_id))
            logger.info(f"📎 Forward msg_id={forward_msg_id}")

        logger.info(
            f"🔧 params: peer_ids='{params['peer_ids']}' "
            f"(type={type(params['peer_ids']).__name__}), "
            f"has_forward={'forward' in params}"
        )

        ret = self._api_call('messages.send', params)

        logger.info(f"🔧 response: {ret}")

        if 'error' in ret:
            err = ret['error']
            code = int(err.get('error_code', 0))
            msg = str(err.get('error_msg', ''))
            logger.error(f"❌ [{self.name}] send error {code}: {msg}")

            if code == 29:
                self._consecutive_failures += 1
                wait_time = min(
                    60 * (2 ** (self._consecutive_failures - 1)),
                    3600
                )
                self._rate_limit_until = time.time() + wait_time

            return False, None

        response = ret.get('response', [])

        # peer_ids возвращает список объектов
        if isinstance(response, list) and len(response) > 0:
            result = response[0]

            if result.get('error'):
                logger.error(f"❌ Ошибка в ответе: {result['error']}")
                return False, None

            msg_id = result.get('message_id', 0)
            cmid = result.get('conversation_message_id', 0)

            # Групповой токен НЕ возвращает message_id (всегда 0)
            # Используем cmid как основной идентификатор
            effective_id = msg_id if msg_id > 0 else cmid

            logger.info(
                f"✅ [{self.name}] message_id={msg_id}, "
                f"cmid={cmid}, effective_id={effective_id}"
            )

            self._consecutive_failures = 0
            return True, {
                'message_id': effective_id,
                'cmid': cmid,
                'peer_id': peer_id,
                'is_cmid': msg_id == 0
            }

        # response=0 — peer_ids не сработал
        if isinstance(response, int):
            if response == 0:
                logger.error(f"❌ response=0! peer_ids не сработал")
            else:
                logger.info(
                    f"✅ [{self.name}] message_id={response} (fallback)"
                )
                return True, {
                    'message_id': response,
                    'cmid': 0,
                    'peer_id': peer_id,
                    'is_cmid': False
                }

        logger.warning(f"⚠️ Неожиданный ответ: {response}")
        return False, None

    # ──────────────────────────────────────────────
    #  РЕДАКТИРОВАНИЕ СООБЩЕНИЯ
    # ──────────────────────────────────────────────
    def edit_message(
        self,
        peer_id: int,
        text: str,
        message_id: int = 0,
        cmid: int = 0
    ) -> Tuple[bool, str]:
        """
        Редактировать своё сообщение.
        Для группового токена message_id=0,
        поэтому ПРИОРИТЕТ у cmid.
        """
        logger.info(
            f"✏️ [{self.name}] Редактирование в чате {peer_id}: "
            f"msg_id={message_id}, cmid={cmid}"
        )

        if self.is_rate_limited():
            return False, "RATE_LIMITED"

        jitter_sleep()

        # ── Попытка 1: через cmid (ПРИОРИТЕТ) ──
        if cmid and cmid > 0:
            params_cmid = {
                'peer_id': peer_id,
                'cmid': cmid,
                'message': text,
                'dont_parse_links': 1,
                'keep_forward_messages': 1,
                'keep_snippets': 1,
            }
            logger.info(f"✏️ Попытка через cmid={cmid}")

            ret = self._api_call('messages.edit', params_cmid)

            if 'error' not in ret and ret.get('response') == 1:
                logger.info(f"✅ Отредактировано через cmid={cmid}")
                return True, "OK"

            if 'error' in ret:
                err = ret['error']
                code = int(err.get('error_code', 0))
                logger.warning(
                    f"⚠️ cmid не сработал: {code} "
                    f"{err.get('error_msg', '')}"
                )

        # ── Попытка 2: через message_id (фолбэк) ──
        if message_id and message_id > 0:
            params = {
                'peer_id': peer_id,
                'message_id': message_id,
                'message': text,
                'dont_parse_links': 1,
                'keep_forward_messages': 1,
                'keep_snippets': 1,
            }
            logger.info(f"✏️ Попытка через message_id={message_id}")

            ret = self._api_call('messages.edit', params)

            if 'error' not in ret and ret.get('response') == 1:
                logger.info(
                    f"✅ Отредактировано через "
                    f"message_id={message_id}"
                )
                return True, "OK"

            if 'error' in ret:
                err = ret['error']
                logger.error(
                    f"❌ message_id тоже не сработал: "
                    f"{err.get('error_code')} "
                    f"{err.get('error_msg', '')}"
                )

        logger.error(
            f"❌ [{self.name}] Редактирование провалилось"
        )
        return False, "EDIT_FAILED"

    # ──────────────────────────────────────────────
    #  ИСТОРИЯ
    # ──────────────────────────────────────────────
    def get_history(
        self, peer_id: int, count: int = 20
    ) -> List[Dict[str, Any]]:
        if self.is_rate_limited():
            return []

        try:
            params = {'peer_id': peer_id, 'count': count}
            ret = self._api_call('messages.getHistory', params)

            if 'error' in ret:
                err = ret['error']
                code = int(err.get('error_code', 0))

                if code == 29:
                    self._consecutive_failures += 1
                    wait_time = min(
                        60 * (2 ** (self._consecutive_failures - 1)),
                        3600
                    )
                    self._rate_limit_until = time.time() + wait_time

                return []

            return ret.get('response', {}).get('items', []) or []

        except Exception as e:
            logger.error(
                f"❌ [{self.name}] getHistory exception: {e}"
            )
            return []

    def get_history_cached(
        self, peer_id: int, count: int = 20
    ) -> List[Dict[str, Any]]:
        cache_key = f"history_{peer_id}_{count}"
        now = time.time()

        with self._cache_lock:
            if cache_key in self._history_cache:
                cached_time, cached_data = self._history_cache[
                    cache_key
                ]
                if now - cached_time < self._cache_ttl:
                    return cached_data.copy()

        fresh_data = self.get_history(peer_id, count)

        with self._cache_lock:
            self._history_cache[cache_key] = (
                now, fresh_data.copy()
            )

        return fresh_data

    def invalidate_cache(
        self, peer_id: Optional[int] = None
    ) -> None:
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


class GroupProxy:
    """
    Прокси для GroupHandler.
    Единый интерфейс с TokenHandler.
    """

    def __init__(
        self,
        group_handler: GroupHandler,
        source_chat_id: int,
        vk
    ):
        self.group_handler = group_handler
        self._vk = vk
        self.source_chat_id = source_chat_id
        self.source_peer_id = (
            2000000000 + source_chat_id if source_chat_id else 0
        )
        self.name = group_handler.name
        self.id = f"group_{group_handler.group_id}"
        self.access_token = group_handler.access_token
        self.class_type = "observer"
        self.enabled = True
        self.owner_vk_id = 0

    def send_to_peer(
        self,
        peer_id: int,
        text: str,
        forward_msg_id: Optional[int] = None,
        reply_to_cmid: Optional[int] = None
    ) -> Tuple[bool, Optional[Dict]]:
        """Отправка — совместимый интерфейс"""
        return self.group_handler.send_message(
            peer_id=peer_id,
            text=text,
            reply_to_cmid=reply_to_cmid,
            forward_msg_id=forward_msg_id
        )

    def send_message(
        self,
        peer_id: int,
        text: str,
        reply_to: Optional[int] = None
    ) -> Tuple[bool, Optional[Dict]]:
        """Алиас send_to_peer"""
        return self.group_handler.send_message(
            peer_id=peer_id,
            text=text,
            reply_to_cmid=reply_to
        )

    def edit_message(
        self,
        peer_id: int,
        message_id: int,
        text: str,
        cmid: int = 0
    ) -> Tuple[bool, str]:
        """Редактирование"""
        return self.group_handler.edit_message(
            peer_id=peer_id,
            text=text,
            message_id=message_id,
            cmid=cmid
        )

    def get_history_cached(self, peer_id: int, count: int = 20):
        return self.group_handler.get_history_cached(peer_id, count)

    def invalidate_cache(self, peer_id=None):
        return self.group_handler.invalidate_cache(peer_id)

    def get_health_info(self):
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
