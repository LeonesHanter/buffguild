# -*- coding: utf-8 -*-
import logging
import random
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from .constants import RACE_NAMES, VK_API_VERSION
from .utils import (
    jitter_sleep,
    normalize_text,
    timestamp_to_moscow,
    format_moscow_time,
)
from .voice_prophet import VoiceProphet

logger = logging.getLogger(__name__)


class TokenHandler:
    def __init__(self, cfg: Dict[str, Any], vk, manager: "OptimizedTokenManager"):
        self.id: str = cfg["id"]
        self.name: str = cfg.get("name", self.id)
        self.class_type: str = cfg.get("class", "apostle")
        self.access_token: str = cfg.get("access_token", "")

        self._vk = vk
        self._manager = manager
        self._needs_save = False
        self._lock = threading.RLock()

        self.owner_vk_id: int = int(cfg.get("owner_vk_id", 0))
        self.source_chat_id: int = int(cfg.get("source_chat_id", 0))
        self.target_peer_id: int = int(cfg.get("target_peer_id", 0))
        self.source_peer_id: int = (
            2000000000 + self.source_chat_id if self.source_chat_id else 0
        )

        self.voices: int = int(cfg.get("voices", 0))
        self.virtual_voices: int = int(cfg.get("virtual_voices", 0))
        self.enabled: bool = bool(cfg.get("enabled", True))
        self.races: List[str] = list(cfg.get("races", []))

        self.successful_buffs: int = int(cfg.get("successful_buffs", 0))
        self.total_attempts: int = int(cfg.get("total_attempts", 0))

        self.temp_races: List[Dict[str, Any]] = []
        for tr in cfg.get("temp_races", []) or []:
            if isinstance(tr, dict) and "race" in tr and "expires" in tr:
                self.temp_races.append(
                    {"race": tr["race"], "expires": int(tr["expires"])}
                )

        self.captcha_until: int = int(cfg.get("captcha_until", 0))
        self.level: int = int(cfg.get("level", 0))
        self.needs_manual_voices: bool = bool(
            cfg.get("needs_manual_voices", False)
        )
        self.virtual_voice_grants: int = int(
            cfg.get("virtual_voice_grants", 0)
        )
        self.next_virtual_grant_ts: int = int(
            cfg.get("next_virtual_grant_ts", 0)
        )

        self._ability_cd: Dict[str, float] = {}
        self._social_cd_until: float = 0.0
        self._last_temp_race_cleanup: float = 0.0

        self._history_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
        self._cache_ttl = 3
        self._cache_lock = threading.Lock()
        
        # ============= Voice Prophet =============
        self.voice_prophet: Optional[VoiceProphet] = None
        # =========================================
        
        # ============= Safe Race Timer =============
        self.SAFETY_MARGIN = 60  # защитный зазор 60 секунд
        # ===========================================

    def mark_for_save(self) -> None:
        old_state = self._needs_save
        self._needs_save = True
        self._manager.mark_for_save()
        if not old_state:
            logger.debug(f"💾 {self.name}: помечен для сохранения")
    
    # ============= АКТИВАЦИЯ VOICE PROPHET =============
    def enable_voice_prophet(self, storage_dir: str = "data/voice_prophet") -> None:
        """Активировать предсказатель голосов для этого токена"""
        if not self.voice_prophet:
            self.voice_prophet = VoiceProphet(self, storage_dir)
            logger.info(f"🔮 {self.name}: активирован Voice Prophet")
    # ==================================================

    def fetch_owner_id_lazy(self) -> int:
        if self.owner_vk_id != 0:
            return self.owner_vk_id
        if not self.access_token:
            logger.warning(
                f"⚠️ {self.name}: cannot detect owner_vk_id - access_token empty"
            )
            return 0

        try:
            data = {"access_token": self.access_token, "v": VK_API_VERSION}
            ret = self._vk.call_with_retry("users.get", data)
            if "response" in ret and ret["response"]:
                uid = int(ret["response"][0]["id"])
                old_owner_id = self.owner_vk_id
                self.owner_vk_id = uid
                
                self.mark_for_save()
                logger.info(f"📌 {self.name}: lazy owner_vk_id={uid} (было: {old_owner_id})")
                
                return uid
        except Exception as e:
            logger.error(f"❌ {self.name}: lazy owner_vk_id failed: {e}")
        return 0

    def is_captcha_paused(self) -> bool:
        return time.time() < float(self.captcha_until)

    def set_captcha_pause(self, seconds: int = 60) -> None:
        self.captcha_until = int(time.time() + seconds)
        self.mark_for_save()
        logger.error(
            f"⛔ {self.name}: captcha pause {seconds}s (until={self.captcha_until})"
        )

    def can_use_ability(self, ability_key: str) -> Tuple[bool, float]:
        ts = self._ability_cd.get(ability_key, 0.0)
        rem = ts - time.time()
        if rem > 0:
            return False, rem
        return True, 0.0

    def set_ability_cooldown(self, ability_key: str, cooldown_seconds: int) -> None:
        sec = int(cooldown_seconds)
        if sec <= 0:
            return
        until = time.time() + sec
        cur = self._ability_cd.get(ability_key, 0.0)
        if until > cur:
            self._ability_cd[ability_key] = until

    def can_use_social(self) -> Tuple[bool, float]:
        rem = self._social_cd_until - time.time()
        if rem > 0:
            return False, rem
        return True, 0.0

    def set_social_cooldown(self, seconds: int) -> None:
        sec = int(seconds)
        if sec <= 0:
            return
        until = time.time() + sec
        if until > self._social_cd_until:
            self._social_cd_until = until

    def get_social_cooldown_info(self) -> Optional[str]:
        rem = self._social_cd_until - time.time()
        if rem <= 0:
            return None
        if rem >= 3600:
            h = int(rem // 3600)
            m = int((rem % 3600) // 60)
            return f"{h}ч{m:02d}м"
        m = int(rem // 60)
        s = int(rem % 60)
        return f"{m}м{s:02d}с"

    def increment_buff_stats(self, success: bool = True) -> None:
        self.total_attempts += 1
        if success:
            self.successful_buffs += 1
        self.mark_for_save()
    
    # ============= РАСХОД ГОЛОСА =============
    def spend_voice(self) -> bool:
        """
        Списать один голос при успешном бафе.
        
        Returns:
            bool: True если голос списан, False если голосов нет
        """
        # Проверяем, есть ли реальные голоса
        if self.voices > 0:
            old_voices = self.voices
            self.voices -= 1
            self.mark_for_save()
            
            # Записываем РАСХОД в Voice Prophet
            if self.voice_prophet:
                self.voice_prophet.record_spend(old_voices)
            
            logger.info(f"🗣️ {self.name}: списан реальный голос ({old_voices}→{self.voices})")
            return True
        
        # Если реальных нет, но есть виртуальные
        if self.virtual_voices > 0:
            old_virtual = self.virtual_voices
            self.virtual_voices -= 1
            self.mark_for_save()
            logger.info(f"🎭 {self.name}: списан виртуальный голос (осталось {self.virtual_voices})")
            return True
        
        logger.debug(f"⚠️ {self.name}: попытка списать голос, но voices={self.voices}, virtual={self.virtual_voices}")
        return False
    # ============================================================

    def clear_virtual_voices(self) -> None:
        """Очищает виртуальные голоса при получении реальных"""
        if self.virtual_voices > 0:
            old = self.virtual_voices
            self.virtual_voices = 0
            self.mark_for_save()
            logger.info(f"🧹 {self.name}: очищены виртуальные голоса ({old} шт.)")
    
    # ============= ПРИНУДИТЕЛЬНАЯ ОЧИСТКА ВИРТУАЛЬНЫХ ГОЛОСОВ =============
    def force_clear_virtual_voices(self) -> bool:
        """
        Принудительно очищает виртуальные голоса и сбрасывает флаги.
        Используется для ручного вмешательства или при проблемах.
        """
        if self.virtual_voices > 0 or self.needs_manual_voices:
            old_virtual = self.virtual_voices
            old_manual = self.needs_manual_voices
            
            self.virtual_voices = 0
            self.needs_manual_voices = False
            
            if hasattr(self._manager, 'profile_manager') and self._manager.profile_manager:
                self._manager.profile_manager.reset_virtual_attempts(self.id)
                logger.info(f"🔄 {self.name}: сброшены виртуальные попытки в ProfileManager (принудительно)")
            
            self.mark_for_save()
            logger.info(f"🧹 {self.name}: принудительно очищены виртуальные голоса (было {old_virtual}), сброшен ручной ввод (был {old_manual})")
            return True
        return False
    # ======================================================================

    # ============= ОБНОВЛЕНИЕ ГОЛОСОВ ИЗ СИСТЕМЫ =============
    def update_voices_from_system(self, new_voices: int) -> None:
        """
        Обновить голоса из системы (ответ на "Мой профиль" или лог игры).
        ПРИ ЛЮБОМ получении реальных голосов очищаем виртуальные и сбрасываем попытки.
        """
        new_voices = int(new_voices)
        if new_voices < 0:
            new_voices = 0

        # Запоминаем состояние ДО обновления
        had_virtual = self.virtual_voices > 0
        old_voices = self.voices
        old_manual = self.needs_manual_voices
        old_virtual = self.virtual_voices

        # Обновляем реальные голоса
        self.voices = new_voices
        
        # ============= КРИТИЧЕСКИ ВАЖНО: Очищаем виртуальные голоса ПРИ ЛЮБОМ получении реальных =============
        if new_voices > 0:
            # Если были виртуальные голоса - очищаем
            if self.virtual_voices > 0:
                logger.info(f"✅ {self.name}: получены реальные голоса ({new_voices}), очищаем виртуальные ({self.virtual_voices})")
                self.virtual_voices = 0
                
                # Сбрасываем счётчик попыток в ProfileManager
                if hasattr(self._manager, 'profile_manager') and self._manager.profile_manager:
                    self._manager.profile_manager.reset_virtual_attempts(self.id)
                    logger.info(f"🔄 {self.name}: сброшены виртуальные попытки в ProfileManager")
            
            # ВСЕГДА сбрасываем флаг ручного ввода, если есть реальные голоса
            if self.needs_manual_voices:
                self.needs_manual_voices = False
                logger.info(f"✅ {self.name}: сброшен флаг ручного ввода (появились реальные голоса)")
        # =====================================================================================================
        
        self.mark_for_save()
        
        # Логируем изменения
        changes = []
        if old_voices != new_voices:
            changes.append(f"голоса: {old_voices}→{new_voices}")
        if had_virtual and self.virtual_voices == 0:
            changes.append("виртуальные очищены")
        if old_manual and not self.needs_manual_voices:
            changes.append("ручной ввод сброшен")
        if old_virtual != self.virtual_voices and self.virtual_voices > 0:
            changes.append(f"виртуальные: {old_virtual}→{self.virtual_voices}")
        
        if changes:
            logger.info(f"🗣 {self.name}: обновление: {', '.join(changes)}")
        
        self.mark_real_voices_received()
        
        # Записываем ПРОВЕРКУ в Voice Prophet
        if self.voice_prophet:
            predicted = self.voice_prophet.predict_zero_at()
            self.voice_prophet.record_check(new_voices, predicted)
    # ==========================================================

    def update_voices_manual(self, new_voices: int) -> None:
        new_voices = int(new_voices)
        if new_voices < 0:
            new_voices = 0

        old = self.voices
        self.voices = new_voices
        self.needs_manual_voices = False  # Явно сбрасываем
        self.virtual_voices = 0  # Очищаем виртуальные при ручной установке
        self.virtual_voice_grants = 0
        self.next_virtual_grant_ts = 0
        self.mark_for_save()
        
        # Сбрасываем счётчик попыток в ProfileManager
        if hasattr(self._manager, 'profile_manager') and self._manager.profile_manager:
            self._manager.profile_manager.reset_virtual_attempts(self.id)
            logger.info(f"🔄 {self.name}: сброшены виртуальные попытки в ProfileManager (ручная установка)")
        
        logger.info(f"🛠 {self.name}: manual voices {old} → {new_voices}, виртуальные очищены")

    def reset_manual_voices_flag(self) -> bool:
        """Принудительно сбрасывает флаг ручного ввода"""
        if self.needs_manual_voices:
            self.needs_manual_voices = False
            self.mark_for_save()
            logger.info(f"🔄 {self.name}: принудительно сброшен флаг ручного ввода")
            return True
        return False

    def update_level(self, lvl: int) -> None:
        lvl = int(lvl)
        if lvl < 0:
            lvl = 0

        if self.level != lvl:
            old = self.level
            self.level = lvl
            self.mark_for_save()
            logger.info(f"💀 {self.name}: level {old} → {lvl}")

    # ============= МЕТОДЫ API С RETRY =============
    def get_history(self, peer_id: int, count: int = 20) -> List[Dict[str, Any]]:
        """Получает историю сообщений чата с повторными попытками"""
        try:
            data = {
                "access_token": self.access_token,
                "v": VK_API_VERSION,
                "peer_id": int(peer_id),
                "count": int(count),
            }
            ret = self._vk.call_with_retry("messages.getHistory", data)
            if "error" in ret:
                err = ret["error"]
                logger.error(f"❌ {self.name}: getHistory error {err.get('error_code')} {err.get('error_msg')}")
                return []
            return ret.get("response", {}).get("items", []) or []
        except Exception as e:
            logger.error(f"❌ {self.name}: getHistory exception {e}")
            return []

    def get_history_cached(self, peer_id: int, count: int = 20) -> List[Dict[str, Any]]:
        """Получает историю с кэшированием"""
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

    def invalidate_cache(self, peer_id: Optional[int] = None) -> None:
        """Инвалидирует кэш истории"""
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

    def get_by_id(self, message_ids: List[int]) -> List[Dict[str, Any]]:
        """Получает сообщения по их ID с повторными попытками"""
        try:
            data = {
                "access_token": self.access_token,
                "v": VK_API_VERSION,
                "message_ids": ",".join(str(int(x)) for x in message_ids),
            }
            ret = self._vk.call_with_retry("messages.getById", data)
            if "error" in ret:
                err = ret["error"]
                logger.error(f"❌ {self.name}: getById error {err.get('error_code')} {err.get('error_msg')}")
                return []
            return ret.get("response", {}).get("items", []) or []
        except Exception as e:
            logger.error(f"❌ {self.name}: getById exception {e}")
            return []

    def send_to_peer(
        self,
        peer_id: int,
        text: str,
        forward_msg_id: Optional[int] = None,
        reply_to_cmid: Optional[int] = None,
    ) -> Tuple[bool, str]:
        """Отправляет сообщение в чат с повторными попытками"""
        if not self.enabled:
            return False, "DISABLED"
        if self.is_captcha_paused():
            return False, "CAPTCHA_PAUSED"

        try:
            jitter_sleep()
            data: Dict[str, Any] = {
                "access_token": self.access_token,
                "v": VK_API_VERSION,
                "peer_id": int(peer_id),
                "message": text,
                "random_id": random.randrange(1, 2_000_000_000),
                "disable_mentions": 1,
            }
            if forward_msg_id:
                data["forward_messages"] = str(int(forward_msg_id))
            elif reply_to_cmid:
                data["reply_to"] = str(int(reply_to_cmid))

            ret = self._vk.call_with_retry("messages.send", data)
            
            if "error" in ret:
                err = ret["error"]
                code = int(err.get("error_code", 0))
                msg = str(err.get("error_msg", ""))

                if code == 14:
                    self.set_captcha_pause(60)
                    return False, "CAPTCHA"
                if code == 9:
                    return False, "FLOOD"
                if code in (4, 5):
                    return False, "AUTH"

                logger.error(f"❌ {self.name}: send error {code} {msg}")
                return False, "ERROR"

            response = ret.get("response")
            
            if isinstance(response, dict):
                message_id = response.get("id")
                if message_id:
                    logger.info(f"✅ {self.name}: сообщение отправлено, ID={message_id}")
                    return True, str(message_id)
            
            elif isinstance(response, (int, str)) and str(response).isdigit():
                message_id = int(response)
                if message_id > 0:
                    logger.info(f"✅ {self.name}: сообщение отправлено, ID={message_id}")
                    return True, str(message_id)
            
            logger.warning(f"⚠️ {self.name}: не удалось получить ID сообщения, response={response}")
            return True, "OK"

        except Exception as e:
            logger.error(f"❌ {self.name}: send exception {e}")
            return False, "ERROR"

    def edit_message(self, peer_id: int, message_id: int, text: str) -> Tuple[bool, str]:
        """
        Редактирует существующее сообщение с повторными попытками
        
        Args:
            peer_id: ID чата
            message_id: ID сообщения для редактирования
            text: новый текст сообщения
            
        Returns:
            Tuple[bool, str]: (успех, статус)
        """
        if not self.enabled:
            return False, "DISABLED"
        if self.is_captcha_paused():
            return False, "CAPTCHA_PAUSED"

        try:
            jitter_sleep()
            data = {
                "access_token": self.access_token,
                "v": VK_API_VERSION,
                "peer_id": int(peer_id),
                "message_id": int(message_id),
                "message": text,
                "dont_parse_links": 1,
            }
            
            # Для групповых токенов добавляем group_id
            if hasattr(self, 'group_id') and self.group_id:
                data["group_id"] = abs(self.group_id)
                
            ret = self._vk.call_with_retry("messages.edit", data)
            
            if "error" in ret:
                err = ret["error"]
                code = int(err.get("error_code", 0))
                msg = str(err.get("error_msg", ""))
                
                if code == 14:
                    self.set_captcha_pause(60)
                    return False, "CAPTCHA"
                if code == 9:
                    return False, "FLOOD"
                if code == 29:
                    logger.warning(f"⏳ {self.name}: rate limit при редактировании")
                    return False, "RATE_LIMITED"
                
                logger.error(f"❌ {self.name}: edit error {code} {msg}")
                return False, "ERROR"
            
            logger.info(f"✏️ {self.name}: сообщение {message_id} отредактировано")
            return True, "OK"

        except Exception as e:
            logger.error(f"❌ {self.name}: edit exception {e}")
            return False, "ERROR"

    def delete_message(self, peer_id: int, message_id: int) -> bool:
        """Удаляет сообщение с повторными попытками"""
        if not self.enabled:
            return False
        if self.is_captcha_paused():
            return False

        try:
            jitter_sleep()
            data = {
                "access_token": self.access_token,
                "v": VK_API_VERSION,
                "peer_id": int(peer_id),
                "cmids": str(int(message_id)),
                "delete_for_all": 1,
            }
            ret = self._vk.call_with_retry("messages.delete", data)
            if "error" in ret:
                err = ret["error"]
                logger.error(f"❌ {self.name}: delete error {err.get('error_code')} {err.get('error_msg')}")
                return False
            return True
        except Exception as e:
            logger.error(f"❌ {self.name}: delete exception {e}")
            return False

    def send_reaction_success(self, peer_id: int, cmid: int) -> bool:
        """Отправляет реакцию 🎉 на сообщение с повторными попытками"""
        if cmid is None:
            return False

        try:
            jitter_sleep()
            data = {
                "access_token": self.access_token,
                "v": VK_API_VERSION,
                "peer_id": int(peer_id),
                "cmid": int(cmid),
                "reaction_id": 16,
            }
            ret = self._vk.call_with_retry("messages.sendReaction", data)
            if "error" in ret:
                err = ret["error"]
                logger.error(f"❌ {self.name}: sendReaction error {err.get('error_code')} {err.get('error_msg')}")
                return False

            logger.info(f"🙂 {self.name}: реакция 🎉 поставлена (peer={peer_id} cmid={cmid})")
            return True
        except Exception as e:
            logger.error(f"❌ {self.name}: sendReaction exception {e}")
            return False

    def get_health_info(self) -> Dict[str, Any]:
        """Возвращает информацию о состоянии токена"""
        with self._lock:
            social_info = self.get_social_cooldown_info()
            return {
                "id": self.id,
                "name": self.name,
                "class": self.class_type,
                "enabled": self.enabled,
                "captcha_paused": self.is_captcha_paused(),
                "captcha_until": self.captcha_until,
                "needs_manual_voices": self.needs_manual_voices,
                "voices": self.voices,
                "virtual_voices": self.virtual_voices,
                "level": self.level,
                "temp_races_count": self.get_temp_race_count(),
                "successful_buffs": self.successful_buffs,
                "total_attempts": self.total_attempts,
                "success_rate": (
                    self.successful_buffs / self.total_attempts
                    if self.total_attempts > 0
                    else 0.0
                ),
                "owner_vk_id": self.owner_vk_id,
                "races": self.races,
                "temp_races": self.temp_races.copy(),
                "social_cd": social_info or "-",
            }
    
    # ============= МЕТОДЫ ДЛЯ РАС =============
    def _cleanup_expired_temp_races(self, force: bool = False) -> bool:
        now = time.time()
        
        if not force and (now - self._last_temp_race_cleanup < 300):
            return False

        changed = False
        with self._lock:
            before = len(self.temp_races)
            valid_races = []
            expired_races = []
            
            for tr in self.temp_races:
                expires = int(tr.get("expires", 0))
                race = tr.get("race", "unknown")
                
                if expires > now:
                    valid_races.append(tr)
                    logger.debug(f"✅ {self.name}: временная раса '{race}' активна (осталось {(expires - now)/60:.0f} мин)")
                else:
                    expired_races.append(race)
                    logger.info(f"🗑️ {self.name}: удалена просроченная временная раса '{race}'")
                    changed = True
            
            if changed:
                self.temp_races = valid_races
                self.mark_for_save()
                logger.info(f"🧹 {self.name}: очищены просроченные временные расы ({', '.join(expired_races)})")
            
            self._last_temp_race_cleanup = now

        return changed

    def cleanup_only_expired(self) -> bool:
        now = time.time()
        changed = False
        
        with self._lock:
            before = len(self.temp_races)
            valid_races = []
            expired_races = []
            
            for tr in self.temp_races:
                expires = int(tr.get("expires", 0))
                race = tr.get("race", "unknown")
                if expires > now:
                    valid_races.append(tr)
                else:
                    expired_races.append(race)
                    logger.debug(f"🗑️ {self.name}: просроченная временная раса '{race}' (истекла)")
                    changed = True
            
            if changed:
                self.temp_races = valid_races
                self.mark_for_save()
                logger.info(f"🧹 {self.name}: очищены только просроченные временные расы ({before} → {len(valid_races)})")
        
        return changed
    
    # ============= HAS RACE С ЗАЩИТНЫМ ТАЙМЕРОМ =============
    def has_race(self, race_key: str) -> bool:
        """
        Проверка наличия расы с УЧЁТОМ ЗАЩИТНОГО ТАЙМЕРА.
        Раса считается доступной ТОЛЬКО если expires > now.
        """
        if race_key in self.races:
            logger.debug(f"✅ {self.name}: найдена постоянная раса '{race_key}'")
            return True
        
        self._cleanup_expired_temp_races()
        
        for tr in self.temp_races:
            if tr.get("race") == race_key:
                expires = tr.get("expires", 0)
                if expires > time.time():
                    remaining = (expires - time.time()) / 60
                    logger.debug(f"✅ {self.name}: найдена временная раса '{race_key}' (осталось {remaining:.0f} мин)")
                    return True
                else:
                    logger.debug(f"🧹 {self.name}: удаляем просроченную расу {race_key}")
                    self.temp_races = [
                        t for t in self.temp_races 
                        if t.get("race") != race_key
                    ]
                    self.mark_for_save()
        
        logger.debug(f"❌ {self.name}: раса '{race_key}' не найдена")
        return False
    # ======================================================

    def get_temp_race_count(self) -> int:
        self._cleanup_expired_temp_races()
        return len(self.temp_races)
    
    # ============= ADD TEMPORARY RACE С ЗАЩИТНЫМ ТАЙМЕРОМ =============
    def add_temporary_race(
        self,
        race_key: str,
        duration_hours: int = 2,
        expires_at: Optional[int] = None,
    ) -> bool:
        with self._lock:
            if race_key not in RACE_NAMES:
                return False

            self._cleanup_expired_temp_races(force=False)

            if self.has_race(race_key):
                return False

            if self.get_temp_race_count() >= 1:
                return False

            if expires_at is None:
                expires_at = round(time.time() + duration_hours * 3600)
            
            safe_expires_at = expires_at - self.SAFETY_MARGIN

            self.temp_races.append({
                "race": race_key, 
                "expires": int(safe_expires_at)
            })
            self.mark_for_save()

            expires_time = format_moscow_time(
                timestamp_to_moscow(int(safe_expires_at))
            )
            real_expires_time = format_moscow_time(
                timestamp_to_moscow(int(expires_at))
            )
            
            logger.info(
                f"🎯 {self.name}: добавлена временная раса '{race_key}' "
                f"до {expires_time} "
                f"(реально до {real_expires_time}, зазор {self.SAFETY_MARGIN}с)"
            )
            
            return True
    # ====================================================================

    def update_temp_race_expiry(self, race_key: str, new_expires_at: int) -> bool:
        with self._lock:
            for tr in self.temp_races:
                if tr.get("race") == race_key:
                    tr["expires"] = int(new_expires_at)
                    self.mark_for_save()
                    expires_time = format_moscow_time(
                        timestamp_to_moscow(int(new_expires_at))
                    )
                    logger.info(
                        f"🔄 {self.name}: обновлена временная раса "
                        f"'{race_key}' до {expires_time}"
                    )
                    return True
        return False
    
    # ============= GET TEMP RACES INFO =============
    def get_temp_races_info(self) -> List[Dict]:
        result = []
        now = time.time()
        
        for tr in self.temp_races:
            expires = tr.get("expires", 0)
            remaining = expires - now
            
            if remaining > 0:
                result.append({
                    'race': tr.get('race'),
                    'expires_at': expires,
                    'remaining_seconds': int(remaining),
                    'remaining_minutes': int(remaining / 60),
                    'safe_until': format_moscow_time(timestamp_to_moscow(expires)),
                    'real_until': format_moscow_time(
                        timestamp_to_moscow(expires + self.SAFETY_MARGIN)
                    )
                })
        
        return result
    # ================================================

    def mark_real_voices_received(self) -> None:
        """Отмечает, что получены реальные голоса и сбрасывает счётчики"""
        if (
            self.needs_manual_voices
            or self.virtual_voice_grants
            or self.next_virtual_grant_ts
        ):
            self.needs_manual_voices = False
            self.virtual_voice_grants = 0
            self.next_virtual_grant_ts = 0
            self.mark_for_save()
