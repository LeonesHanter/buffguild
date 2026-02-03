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

    def mark_for_save(self) -> None:
        self._needs_save = True
        self._manager.mark_for_save()

    def fetch_owner_id_lazy(self) -> int:
        if self.owner_vk_id != 0:
            return self.owner_vk_id
        if not self.access_token:
            logging.warning(
                f"⚠️ {self.name}: cannot detect owner_vk_id - access_token empty"
            )
            return 0

        try:
            data = {"access_token": self.access_token, "v": VK_API_VERSION}
            ret = self._vk.call(self._vk.post("users.get", data))
            if "response" in ret and ret["response"]:
                uid = int(ret["response"][0]["id"])
                self.owner_vk_id = uid
                self.mark_for_save()
                logging.info(f"📌 {self.name}: lazy owner_vk_id={uid}")
                return uid
        except Exception as e:
            logging.error(f"❌ {self.name}: lazy owner_vk_id failed: {e}")
        return 0

    def is_captcha_paused(self) -> bool:
        return time.time() < float(self.captcha_until)

    def set_captcha_pause(self, seconds: int = 60) -> None:
        self.captcha_until = int(time.time() + seconds)
        self.mark_for_save()
        logging.error(
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

    def _cleanup_expired_temp_races(self, force: bool = False) -> bool:
        now = time.time()
        if not force and (now - self._last_temp_race_cleanup < 300):
            return False

        changed = False
        with self._lock:
            before = len(self.temp_races)
            self.temp_races = [
                tr for tr in self.temp_races if int(tr.get("expires", 0)) > now
            ]
            after = len(self.temp_races)

            if after != before:
                changed = True
                self.mark_for_save()
                logging.info(
                    f"🧹 {self.name}: очищены просроченные временные расы"
                )

            self._last_temp_race_cleanup = now

        return changed

    def has_race(self, race_key: str) -> bool:
        if race_key in self.races:
            return True
        self._cleanup_expired_temp_races()
        for tr in self.temp_races:
            if tr.get("race") == race_key:
                return True
        return False

    def get_temp_race_count(self) -> int:
        self._cleanup_expired_temp_races()
        return len(self.temp_races)

    def add_temporary_race(
        self,
        race_key: str,
        duration_hours: int = 2,
        expires_at: Optional[int] = None,
    ) -> bool:
        with self._lock:
            if race_key not in RACE_NAMES:
                return False

            self._cleanup_expired_temp_races(force=True)

            if self.has_race(race_key):
                return False

            if self.get_temp_race_count() >= 1:
                return False

            if expires_at is None:
                expires_at = round(time.time() + duration_hours * 3600)

            self.temp_races.append(
                {"race": race_key, "expires": int(expires_at)}
            )
            self.mark_for_save()

            expires_time = format_moscow_time(
                timestamp_to_moscow(int(expires_at))
            )
            logging.info(
                f"🎯 {self.name}: добавлена временная раса "
                f"'{race_key}' до {expires_time}"
            )
            return True

    def update_temp_race_expiry(self, race_key: str, new_expires_at: int) -> bool:
        with self._lock:
            for tr in self.temp_races:
                if tr.get("race") == race_key:
                    tr["expires"] = int(new_expires_at)
                    self.mark_for_save()
                    expires_time = format_moscow_time(
                        timestamp_to_moscow(int(new_expires_at))
                    )
                    logging.info(
                        f"🔄 {self.name}: обновлена временная раса "
                        f"'{race_key}' до {expires_time}"
                    )
                    return True
        return False

    def mark_real_voices_received(self) -> None:
        if (
            self.needs_manual_voices
            or self.virtual_voice_grants
            or self.next_virtual_grant_ts
        ):
            self.needs_manual_voices = False
            self.virtual_voice_grants = 0
            self.next_virtual_grant_ts = 0
            self.mark_for_save()

    def update_voices_from_system(self, new_voices: int) -> None:
        new_voices = int(new_voices)
        if new_voices < 0:
            new_voices = 0

        if self.voices != new_voices:
            old = self.voices
            self.voices = new_voices
            self.mark_for_save()
            logging.info(f"🗣 {self.name}: voices {old} → {new_voices}")
            self.mark_real_voices_received()

    def update_voices_manual(self, new_voices: int) -> None:
        new_voices = int(new_voices)
        if new_voices < 0:
            new_voices = 0

        old = self.voices
        self.voices = new_voices
        self.needs_manual_voices = False
        self.virtual_voice_grants = 0
        self.next_virtual_grant_ts = 0
        self.mark_for_save()
        logging.info(f"🛠 {self.name}: manual voices {old} → {new_voices}")

    def update_level(self, lvl: int) -> None:
        lvl = int(lvl)
        if lvl < 0:
            lvl = 0

        if self.level != lvl:
            old = self.level
            self.level = lvl
            self.mark_for_save()
            logging.info(f"💀 {self.name}: level {old} → {lvl}")

    async def _messages_get_history(self, peer_id: int, count: int = 20) -> Dict[str, Any]:
        data = {
            "access_token": self.access_token,
            "v": VK_API_VERSION,
            "peer_id": int(peer_id),
            "count": int(count),
        }
        return await self._vk.post("messages.getHistory", data)

    def get_history(self, peer_id: int, count: int = 20) -> List[Dict[str, Any]]:
        try:
            ret = self._vk.call(self._messages_get_history(peer_id, count))
            if "error" in ret:
                err = ret["error"]
                logging.error(
                    f"❌ {self.name}: getHistory error "
                    f"{err.get('error_code')} {err.get('error_msg')}"
                )
                return []
            return ret.get("response", {}).get("items", []) or []
        except Exception as e:
            logging.error(f"❌ {self.name}: getHistory exception {e}")
            return []

    def get_history_cached(self, peer_id: int, count: int = 20) -> List[Dict[str, Any]]:
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

    async def _messages_get_by_id(self, message_ids: List[int]) -> Dict[str, Any]:
        data = {
            "access_token": self.access_token,
            "v": VK_API_VERSION,
            "message_ids": ",".join(str(int(x)) for x in message_ids),
        }
        return await self._vk.post("messages.getById", data)

    def get_by_id(self, message_ids: List[int]) -> List[Dict[str, Any]]:
        try:
            ret = self._vk.call(self._messages_get_by_id(message_ids))
            if "error" in ret:
                err = ret["error"]
                logging.error(
                    f"❌ {self.name}: getById error "
                    f"{err.get('error_code')} {err.get('error_msg')}"
                )
                return []
            return ret.get("response", {}).get("items", []) or []
        except Exception as e:
            logging.error(f"❌ {self.name}: getById exception {e}")
            return []

    async def _messages_send(
        self,
        peer_id: int,
        text: str,
        forward_msg_id: Optional[int] = None,
        reply_to_cmid: Optional[int] = None,
    ) -> Dict[str, Any]:
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
        return await self._vk.post("messages.send", data)

    def send_to_peer(
        self,
        peer_id: int,
        text: str,
        forward_msg_id: Optional[int] = None,
        reply_to_cmid: Optional[int] = None,
    ) -> Tuple[bool, str]:
        if not self.enabled:
            return False, "DISABLED"
        if self.is_captcha_paused():
            return False, "CAPTCHA_PAUSED"

        try:
            ret = self._vk.call(
                self._messages_send(peer_id, text, forward_msg_id, reply_to_cmid)
            )
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

                logging.error(
                    f"❌ {self.name}: send error {code} {msg}"
                )
                return False, "ERROR"

            message_id = ret.get("response", 0)
            return True, f"OK:{message_id}"

        except Exception as e:
            logging.error(f"❌ {self.name}: send exception {e}")
            return False, "ERROR"

    def delete_message(self, peer_id: int, message_id: int) -> bool:
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
            ret = self._vk.call(self._vk.post("messages.delete", data))
            if "error" in ret:
                err = ret["error"]
                logging.error(
                    f"❌ {self.name}: delete error "
                    f"{err.get('error_code')} {err.get('error_msg')}"
                )
                return False
            return True
        except Exception as e:
            logging.error(f"❌ {self.name}: delete exception {e}")
            return False

    def send_reaction_success(self, peer_id: int, cmid: int) -> bool:
        if cmid is None:
            return False

        jitter_sleep()
        data = {
            "access_token": self.access_token,
            "v": VK_API_VERSION,
            "peer_id": int(peer_id),
            "cmid": int(cmid),
            "reaction_id": 16,
        }
        try:
            ret = self._vk.call(self._vk.post("messages.sendReaction", data))
            if "error" in ret:
                err = ret["error"]
                logging.error(
                    f"❌ {self.name}: sendReaction error "
                    f"{err.get('error_code')} {err.get('error_msg')}"
                )
                return False

            logging.info(
                f"🙂 {self.name}: реакция 🎉 поставлена "
                f"(peer={peer_id} cmid={cmid})"
            )
            return True
        except Exception as e:
            logging.error(f"❌ {self.name}: sendReaction exception {e}")
            return False

    def get_health_info(self) -> Dict[str, Any]:
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
