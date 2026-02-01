# -*- coding: utf-8 -*-
import asyncio
import aiohttp
import json
import logging
import os
import random
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional, Tuple

VK_API_BASE = "https://api.vk.com/method"
VK_API_VERSION = "5.131"

# ===== LOGGING =====
logger = logging.getLogger()
logger.setLevel(logging.INFO)

log_formatter = logging.Formatter(
    fmt="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

file_handler = RotatingFileHandler(
    "bot.log",
    maxBytes=5 * 1024 * 1024,
    backupCount=5,
    encoding="utf-8",
)
file_handler.setFormatter(log_formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)

logger.handlers.clear()
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# ===== GAME DATA =====
CLASS_ORDER = ["apostle", "warlock", "crusader", "light_incarnation"]

RACE_NAMES = {
    "ч": "человек",
    "г": "гоблин",
    "н": "нежить",
    "э": "эльф",
    "м": "гном",
    "д": "демон",
    "о": "орк",
}

CLASS_ABILITIES: Dict[str, Dict[str, Any]] = {
    "apostle": {
        "name": "Апостол",
        "prefix": "благословение",
        "uses_voices": True,
        "default_cooldown": 61,
        "abilities": {
            "а": "атаки",
            "з": "защиты",
            "у": "удачи",
            "ч": "человека",
            "г": "гоблина",
            "н": "нежити",
            "э": "эльфа",
            "м": "гнома",
            "д": "демона",
            "о": "орка",
        },
    },
    "warlock": {
        "name": "Проклинающий",
        "prefix": "проклятие",
        "uses_voices": True,
        "default_cooldown": 3600,
        "abilities": {
            "л": "неудачи",
            "б": "боли",
            "ю": "добычи",
        },
    },
    "crusader": {
        "name": "Паладин",
        "prefix": "",
        "uses_voices": True,
        "default_cooldown": None,
        "abilities": {
            "в": ("воскрешение", 6 * 60 * 60),
            "т": ("очищение огнем", 15 * 60 + 10),
        },
    },
    "light_incarnation": {
        "name": "Паладин",
        "prefix": "",
        "uses_voices": True,
        "default_cooldown": None,
        "abilities": {
            "и": ("очищение", 61),
            "с": ("очищение светом", 15 * 60 + 10),
        },
    },
}

# ===== RESULT REGEX =====
RE_SUCCESS = re.compile(r"(на вас наложено|на Вас наложено|наложено благословение)", re.IGNORECASE)
RE_ALREADY = re.compile(
    r"(на эту цель уже действует такое благословение|нельзя наложить благословение уже имеющейся у цели расы)",
    re.IGNORECASE,
)
RE_NO_VOICES = re.compile(r"(требуется голос|голос древних|нет голосов)", re.IGNORECASE)
RE_COOLDOWN = re.compile(
    r"(социальные эффекты можно накладывать только через определенное время|Оставшееся время:\s*\d+\s*сек)",
    re.IGNORECASE,
)
RE_REMAINING_SEC = re.compile(r"Оставшееся время:\s*(\d+)\s*сек", re.IGNORECASE)

# голоса из системных сообщений
RE_VOICES_APO = re.compile(r"Голос у Апостола:\s*(\d+)", re.IGNORECASE)
RE_VOICES_WAR = re.compile(r"Голос у проклинающего:\s*(\d+)", re.IGNORECASE)
RE_VOICES_PAL = re.compile(r"Голос у Паладина:\s*(\d+)", re.IGNORECASE)

# профиль
RE_PROFILE_VOICES = re.compile(r"👤Класс:\s*([а-яA-Za-z_]+)\s*\((\d+)\)\s*,\s*(.+)", re.IGNORECASE)
RE_PROFILE_LEVEL = re.compile(r"💀Уровень:\s*(\d+)", re.IGNORECASE)

# ===== UTILS =====
def jitter_sleep():
    time.sleep(random.uniform(0.10, 0.20))


def normalize_text(s: str) -> str:
    return (s or "").strip().lower()


def now_ts() -> int:
    return int(time.time())


# ===== TEMPORARY RACES =====
class TemporaryRace:
    def __init__(self, race_key: str, expires_at: int):
        self.race_key = race_key
        self.expires_at = expires_at  # timestamp
    
    def is_expired(self) -> bool:
        return time.time() > self.expires_at
    
    def get_remaining_time(self) -> int:
        remaining = self.expires_at - time.time()
        return max(0, int(remaining))
    
    def format_remaining(self) -> str:
        remaining = self.get_remaining_time()
        if remaining >= 3600:
            hours = remaining // 3600
            minutes = (remaining % 3600) // 60
            return f"{hours}ч{minutes}м"
        else:
            minutes = remaining // 60
            seconds = remaining % 60
            return f"{minutes}м{seconds}с"


# ===== VK ASYNC CLIENT =====
class VKAsyncClient:
    def __init__(self):
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._ready = threading.Event()
        self._session: Optional[aiohttp.ClientSession] = None
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

    async def post(self, method: str, data: Dict[str, Any]) -> Dict[str, Any]:
        if not self._session:
            raise RuntimeError("VK session not ready")
        url = f"{VK_API_BASE}/{method}"
        async with self._session.post(url, data=data) as resp:
            return await resp.json()

    async def raw_post(self, url: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Для LongPoll запросов к отдельным серверам"""
        if not self._session:
            raise RuntimeError("VK session not ready")
        async with self._session.post(url, data=data) as resp:
            return await resp.json()


# ===== DATA =====
@dataclass
class ParsedAbility:
    key: str
    text: str
    cooldown: int
    class_type: str
    uses_voices: bool
    processed: bool = False


@dataclass
class Job:
    sender_id: int
    trigger_text: str
    letters: str
    created_ts: float


# ===== TOKEN =====
class TokenHandler:
    def __init__(self, cfg: Dict[str, Any], vk: VKAsyncClient, manager: "TokenManager"):
        self.id: str = cfg["id"]
        self.name: str = cfg.get("name", self.id)
        self.class_type: str = cfg.get("class", "apostle")
        self.access_token: str = cfg.get("access_token", "")

        self._vk = vk
        self._manager = manager

        # owner_vk_id: лениво (НЕ автодетектим на старте)
        self.owner_vk_id: int = int(cfg.get("owner_vk_id", 0))

        self.source_chat_id: int = int(cfg.get("source_chat_id", 0))
        self.target_peer_id: int = int(cfg.get("target_peer_id", 0))
        self.source_peer_id: int = 2000000000 + self.source_chat_id if self.source_chat_id else 0

        self.voices: int = int(cfg.get("voices", 0))
        self.enabled: bool = bool(cfg.get("enabled", True))
        self.races: List[str] = list(cfg.get("races", []))

        # временные расы
        self.temp_races: List[Dict[str, Any]] = []
        temp_races_cfg = cfg.get("temp_races", [])
        for tr in temp_races_cfg:
            if isinstance(tr, dict) and "race" in tr and "expires" in tr:
                self.temp_races.append({
                    "race": tr["race"],
                    "expires": int(tr["expires"])
                })

        self.captcha_until: int = int(cfg.get("captcha_until", 0))
        self.level: int = int(cfg.get("level", 0))

        self.needs_manual_voices: bool = bool(cfg.get("needs_manual_voices", False))
        self.virtual_voice_grants: int = int(cfg.get("virtual_voice_grants", 0))
        self.next_virtual_grant_ts: int = int(cfg.get("next_virtual_grant_ts", 0))

        self._ability_cd: Dict[str, float] = {}
        self._last_temp_race_cleanup: float = 0.0

    def fetch_owner_id_lazy(self) -> int:
        """Лениво определяем owner_vk_id через users.get (делаем это только по необходимости)."""
        if self.owner_vk_id != 0:
            return self.owner_vk_id
        if not self.access_token:
            logging.warning(f"⚠️ {self.name}: cannot detect owner_vk_id - access_token empty")
            return 0
        try:
            data = {"access_token": self.access_token, "v": VK_API_VERSION}
            ret = self._vk.call(self._vk.post("users.get", data))
            if "response" in ret and ret["response"]:
                uid = int(ret["response"][0]["id"])
                self.owner_vk_id = uid
                self._manager.save()
                logging.info(f"📌 {self.name}: lazy owner_vk_id={uid}")
                return uid
        except Exception as e:
            logging.error(f"❌ {self.name}: lazy owner_vk_id failed: {e}")
        return 0

    def class_name(self) -> str:
        return CLASS_ABILITIES.get(self.class_type, {}).get("name", self.class_type)

    def is_captcha_paused(self) -> bool:
        return time.time() < float(self.captcha_until)

    def set_captcha_pause(self, seconds: int = 60) -> None:
        self.captcha_until = int(time.time() + seconds)
        self._manager.save()
        logging.error(f"⛔ {self.name}: captcha pause {seconds}s (until={self.captcha_until})")

    def can_use_ability(self, ability_key: str) -> Tuple[bool, float]:
        ts = self._ability_cd.get(ability_key, 0.0)
        rem = ts - time.time()
        if rem > 0:
            return False, rem
        return True, 0.0

    def set_ability_cooldown(self, ability_key: str, cooldown_seconds: int) -> None:
        self._ability_cd[ability_key] = time.time() + int(cooldown_seconds)

    def has_race(self, race_key: str) -> bool:
        """Проверяет наличие расы (включая временные)"""
        # Основные расы
        if race_key in self.races:
            return True
        
        # Временные расы
        self._cleanup_expired_temp_races()
        for tr in self.temp_races:
            if tr["race"] == race_key:
                return True
        
        return False

    def get_all_races_display(self) -> str:
        """Возвращает строку для отображения всех рас (основные + временные)"""
        self._cleanup_expired_temp_races()
        
        # Основные расы
        base_races = "/".join(sorted(self.races)) if self.races else ""
        
        # Временные расы
        temp_parts = []
        for tr in self.temp_races:
            remaining = int(tr["expires"] - time.time())
            if remaining > 0:
                if remaining >= 3600:
                    hours = remaining // 3600
                    minutes = (remaining % 3600) // 60
                    time_str = f"{hours}ч{minutes}м"
                else:
                    minutes = remaining // 60
                    seconds = remaining % 60
                    time_str = f"{minutes}м{seconds}с"
                temp_parts.append(f"{tr['race']}-({time_str})")
        
        # Комбинируем
        result_parts = []
        if base_races:
            result_parts.append(base_races)
        if temp_parts:
            result_parts.append("/".join(temp_parts))
        
        return "/".join(result_parts) if result_parts else "-"

    def add_temporary_race(self, race_key: str, duration_hours: int = 2) -> bool:
        """Добавляет временную расу на указанное количество часов"""
        if race_key not in RACE_NAMES:
            return False
        
        self._cleanup_expired_temp_races()
        
        # Проверяем уже есть ли такая раса (основная или временная)
        if self.has_race(race_key):
            return False
        
        # Добавляем временную расу
        expires_at = int(time.time() + duration_hours * 3600)
        self.temp_races.append({
            "race": race_key,
            "expires": expires_at
        })
        
        # Сохраняем в конфиг
        self._manager.save()
        
        logging.info(f"🎯 {self.name}: добавлена временная раса '{race_key}' на {duration_hours} часов (до {datetime.fromtimestamp(expires_at).strftime('%H:%M:%S')})")
        return True

    def _cleanup_expired_temp_races(self) -> None:
        """Удаляет просроченные временные расы"""
        now = time.time()
        # Делаем cleanup не чаще чем раз в 5 минут
        if now - self._last_temp_race_cleanup < 300:
            return
        
        original_count = len(self.temp_races)
        self.temp_races = [tr for tr in self.temp_races if tr["expires"] > now]
        
        if len(self.temp_races) != original_count:
            self._manager.save()
            logging.info(f"🧹 {self.name}: очищены просроченные временные расы")
        
        self._last_temp_race_cleanup = now

    def mark_real_voices_received(self) -> None:
        if self.needs_manual_voices or self.virtual_voice_grants or self.next_virtual_grant_ts:
            self.needs_manual_voices = False
            self.virtual_voice_grants = 0
            self.next_virtual_grant_ts = 0
            self._manager.save()

    def update_voices_from_system(self, new_voices: int) -> None:
        new_voices = int(new_voices)
        if new_voices < 0:
            new_voices = 0
        if self.voices != new_voices:
            old = self.voices
            self.voices = new_voices
            self._manager.save()
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
        self._manager.save()
        logging.info(f"🛠 {self.name}: manual voices {old} → {new_voices}")

    def update_level(self, lvl: int) -> None:
        lvl = int(lvl)
        if lvl < 0:
            lvl = 0
        if self.level != lvl:
            old = self.level
            self.level = lvl
            self._manager.save()
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
                logging.error(f"❌ {self.name}: getHistory error {err.get('error_code')} {err.get('error_msg')}")
                return []
            return ret.get("response", {}).get("items", []) or []
        except Exception as e:
            logging.error(f"❌ {self.name}: getHistory exception {e}")
            return []

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
                logging.error(f"❌ {self.name}: getById error {err.get('error_code')} {err.get('error_msg')}")
                return []
            return ret.get("response", {}).get("items", []) or []
        except Exception as e:
            logging.error(f"❌ {self.name}: getById exception {e}")
            return []

    async def _messages_send(self, peer_id: int, text: str, forward_msg_id: Optional[int] = None) -> Dict[str, Any]:
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
        return await self._vk.post("messages.send", data)

    def send_to_peer(self, peer_id: int, text: str, forward_msg_id: Optional[int] = None) -> Tuple[bool, str]:
        if not self.enabled:
            return False, "DISABLED"
        if self.is_captcha_paused():
            return False, "CAPTCHA_PAUSED"

        try:
            ret = self._vk.call(self._messages_send(peer_id, text, forward_msg_id))
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
                logging.error(f"❌ {self.name}: send error {code} {msg}")
                return False, "ERROR"
            return True, "OK"
        except Exception as e:
            logging.error(f"❌ {self.name}: send exception {e}")
            return False, "ERROR"

    def send_reaction_success(self, peer_id: int, cmid: int) -> bool:
        if cmid is None:
            return False
        jitter_sleep()
        data = {
            "access_token": self.access_token,
            "v": VK_API_VERSION,
            "peer_id": int(peer_id),
            "cmid": int(cmid),
            "reaction_id": 16,  # 🎉
        }
        try:
            ret = self._vk.call(self._vk.post("messages.sendReaction", data))
            if "error" in ret:
                err = ret["error"]
                logging.error(f"❌ {self.name}: sendReaction error {err.get('error_code')} {err.get('error_msg')}")
                return False
            logging.info(f"🙂 {self.name}: реакция 🎉 поставлена (peer={peer_id} cmid={cmid})")
            return True
        except Exception as e:
            logging.error(f"❌ {self.name}: sendReaction exception {e}")
            return False


# ===== TOKEN MANAGER =====
class TokenManager:
    def __init__(self, config_path: str, vk: VKAsyncClient):
        self.config_path = config_path
        self._lock = threading.Lock()
        self._vk = vk

        self.config: Dict[str, Any] = {}
        self.tokens: List[TokenHandler] = []
        self.observer_token_id: str = ""
        self.settings: Dict[str, Any] = {}

        self.load()

    def load(self) -> None:
        with open(self.config_path, "r", encoding="utf-8") as f:
            self.config = json.load(f)

        self.observer_token_id = str(self.config.get("observer_token_id", "")).strip()
        self.settings = dict(self.config.get("settings", {}))

        self.tokens = []
        for t_cfg in self.config.get("tokens", []):
            self.tokens.append(TokenHandler(t_cfg, self._vk, self))

        logging.info(f"📋 Tokens: {len(self.tokens)}")

    def save(self) -> None:
        with self._lock:
            payload_tokens = []
            for t in self.tokens:
                payload_tokens.append({
                    "id": t.id,
                    "name": t.name,
                    "class": t.class_type,
                    "access_token": t.access_token,
                    "owner_vk_id": t.owner_vk_id,
                    "source_chat_id": t.source_chat_id,
                    "target_peer_id": t.target_peer_id,
                    "voices": t.voices,
                    "enabled": t.enabled,
                    "races": t.races,
                    "temp_races": t.temp_races,  # сохраняем временные расы
                    "captcha_until": t.captcha_until,
                    "level": t.level,
                    "needs_manual_voices": t.needs_manual_voices,
                    "virtual_voice_grants": t.virtual_voice_grants,
                    "next_virtual_grant_ts": t.next_virtual_grant_ts,
                })

            self.config["observer_token_id"] = self.observer_token_id
            self.config["settings"] = self.settings
            self.config["tokens"] = payload_tokens

        with open(self.config_path, "w", encoding="utf-8") as f:
            json.dump(self.config, f, ensure_ascii=False, indent=2)

    def get_token_by_id(self, token_id: str) -> Optional[TokenHandler]:
        for t in self.tokens:
            if t.id == token_id:
                return t
        return None

    def get_token_by_name(self, name: str) -> Optional[TokenHandler]:
        name_n = normalize_text(name)
        for t in self.tokens:
            if normalize_text(t.name) == name_n:
                return t
        return None

    def get_token_by_sender_id(self, sender_id: int) -> Optional[TokenHandler]:
        """Находит токен по ID отправителя (сравнивает owner_vk_id)"""
        for t in self.tokens:
            if t.owner_vk_id == 0:
                t.fetch_owner_id_lazy()
            if t.owner_vk_id == sender_id:
                return t
        return None

    def get_observer(self) -> TokenHandler:
        if not self.observer_token_id:
            raise RuntimeError("observer_token_id is not set in config.json")
        t = self.get_token_by_id(self.observer_token_id)
        if not t:
            raise RuntimeError(f"observer_token_id='{self.observer_token_id}' not found in tokens[]")
        return t

    def all_buffers(self) -> List[TokenHandler]:
        obs = self.get_observer()
        return [t for t in self.tokens if t.id != obs.id]


# ===== ability builder =====
def build_ability_text_and_cd(class_type: str, key: str) -> Optional[Tuple[str, int, bool]]:
    c = CLASS_ABILITIES.get(class_type)
    if not c or key not in c["abilities"]:
        return None
    uses_voices = bool(c.get("uses_voices", False))
    v = c["abilities"][key]
    if isinstance(v, tuple):
        return str(v[0]), int(v[1]), uses_voices
    prefix = c.get("prefix", "")
    default_cd = int(c.get("default_cooldown", 61))
    text = f"{prefix} {v}".strip() if prefix else str(v)
    return text, default_cd, uses_voices


# ===== executor (serialized by target) =====
class AbilityExecutor:
    def __init__(self, tm: TokenManager):
        self.tm = tm
        self._target_lock: Dict[int, threading.Lock] = {}

    def _lock_for_target(self, peer_id: int) -> threading.Lock:
        if peer_id not in self._target_lock:
            self._target_lock[peer_id] = threading.Lock()
        return self._target_lock[peer_id]

    def find_trigger_in_token_source(self, token: TokenHandler, job: Job) -> Tuple[Optional[int], Optional[int]]:
        want_text = normalize_text(job.trigger_text)
        if not want_text:
            return None, None

        # ИЗМЕНЕНО: было 60, стало 30 (это 30 последних сообщений)
        msgs = token.get_history(token.source_peer_id, count=30)

        for m in msgs:
            from_id = int(m.get("from_id", 0))
            if from_id != job.sender_id:
                continue
            txt = normalize_text(m.get("text", ""))
            if txt == want_text:
                mid = int(m.get("id", 0))
                cmid = m.get("conversation_message_id")
                cmid_int = int(cmid) if isinstance(cmid, int) or (isinstance(cmid, str) and str(cmid).isdigit()) else None
                return mid, cmid_int
        return None, None

    def _parse_new_messages(self, msgs: List[Dict[str, Any]]) -> Tuple[str, Optional[int], Optional[int]]:
        remaining = None
        voices_val = None

        for m in msgs:
            text = str(m.get("text", "")).strip()

            mm = RE_REMAINING_SEC.search(text)
            if mm:
                try:
                    remaining = int(mm.group(1))
                except Exception:
                    pass

            for rex in (RE_VOICES_APO, RE_VOICES_WAR, RE_VOICES_PAL):
                vm = rex.search(text)
                if vm:
                    try:
                        voices_val = int(vm.group(1))
                    except Exception:
                        pass

        for m in msgs:
            text = str(m.get("text", "")).strip()
            if RE_SUCCESS.search(text):
                return "SUCCESS", remaining, voices_val
            if RE_ALREADY.search(text):
                return "ALREADY", remaining, voices_val
            if RE_NO_VOICES.search(text):
                return "NO_VOICES", remaining, voices_val
            if RE_COOLDOWN.search(text):
                return "COOLDOWN", remaining, voices_val

        return "UNKNOWN", remaining, voices_val

    def execute_one(self, token: TokenHandler, ability: ParsedAbility, job: Job) -> Tuple[bool, str]:
        if not token.enabled:
            return False, "DISABLED"
        if token.is_captcha_paused():
            return False, "CAPTCHA_PAUSED"
        if token.needs_manual_voices:
            return False, "NEEDS_MANUAL_VOICES"
        if ability.uses_voices and token.voices <= 0:
            return False, "NO_VOICES_LOCAL"

        can, rem = token.can_use_ability(ability.key)
        if not can:
            return False, f"LOCAL_COOLDOWN({int(rem)}s)"

        trigger_mid, trigger_cmid = self.find_trigger_in_token_source(token, job)
        if not trigger_mid:
            return False, "TRIGGER_NOT_FOUND_IN_SOURCE"

        target_lock = self._lock_for_target(token.target_peer_id)

        with target_lock:
            before = token.get_history(token.target_peer_id, count=1)
            last_id_before = before[0]["id"] if before else 0

            ok, send_status = token.send_to_peer(
                token.target_peer_id,
                ability.text,
                forward_msg_id=trigger_mid,
            )
            if not ok:
                return False, send_status

            poll_interval = float(self.tm.settings.get("poll_interval", 2.0))
            poll_count = int(self.tm.settings.get("poll_count", 20))

            for _ in range(poll_count):
                time.sleep(poll_interval)
                history = token.get_history(token.target_peer_id, count=25)
                new_msgs = [m for m in history if int(m.get("id", 0)) > last_id_before]
                if not new_msgs:
                    continue

                status, remaining, voices_val = self._parse_new_messages(list(reversed(new_msgs)))

                if voices_val is not None:
                    token.update_voices_from_system(voices_val)

                if status == "SUCCESS":
                    ability.processed = True
                    token.set_ability_cooldown(ability.key, ability.cooldown)
                    if ability.uses_voices:
                        token.update_voices_from_system(token.voices - 1)

                    if trigger_cmid is not None:
                        token.send_reaction_success(token.source_peer_id, trigger_cmid)

                    logging.info(f"✅ {token.name}({token.class_name()}): {ability.text}")
                    return True, "SUCCESS"

                if status == "ALREADY":
                    logging.info(f"ℹ️ {token.name}({token.class_name()}): {ability.text} ALREADY")
                    return True, "ALREADY"

                if status == "NO_VOICES":
                    token.update_voices_from_system(0)
                    return False, "NO_VOICES"

                if status == "COOLDOWN":
                    if remaining is not None and remaining > 0:
                        token.set_ability_cooldown(ability.key, remaining)
                        return False, f"COOLDOWN({remaining}s)"
                    token.set_ability_cooldown(ability.key, 61)
                    return False, "COOLDOWN(61s)"

            return False, "UNKNOWN"

    def refresh_profile(self, token: TokenHandler) -> bool:
        if not token.enabled or token.is_captcha_paused() or token.needs_manual_voices:
            return False

        ok, _ = token.send_to_peer(token.target_peer_id, "Мой профиль", None)
        if not ok:
            return False

        time.sleep(2.5)
        history = token.get_history(token.target_peer_id, count=25)
        if not history:
            return False

        got_voices = False

        for m in history:
            text = str(m.get("text", "")).strip()

            pm = RE_PROFILE_VOICES.search(text)
            if pm:
                try:
                    v = int(pm.group(2))
                    token.update_voices_from_system(v)
                    got_voices = (v > 0)
                except Exception:
                    pass

                races_part = (pm.group(3) or "").strip().lower()
                found = []
                for k, name in RACE_NAMES.items():
                    if name in races_part:
                        found.append(k)
                if found and token.class_type == "apostle":
                    token.races = sorted(list(set(found)))
                    token._manager.save()

            lm = RE_PROFILE_LEVEL.search(text)
            if lm and token.class_type in ("crusader", "light_incarnation"):
                try:
                    token.update_level(int(lm.group(1)))
                except Exception:
                    pass

        return got_voices


# ===== Scheduler (queue with re-filter) =====
class Scheduler:
    def __init__(self, tm: TokenManager, executor: AbilityExecutor):
        self.tm = tm
        self.executor = executor
        self._q: List[Tuple[float, Job, str]] = []  # (run_at_ts, job, letter)
        self._lock = threading.Lock()
        self._thr = threading.Thread(target=self._run_loop, daemon=True)
        self._thr.start()

    def enqueue_letters(self, job: Job, letters: str) -> None:
        letters = letters[:4]
        now = time.time()
        with self._lock:
            for ch in letters:
                self._q.append((now, job, ch))

    def _pop_ready(self) -> Optional[Tuple[float, Job, str]]:
        now = time.time()
        with self._lock:
            self._q.sort(key=lambda x: x[0])
            if not self._q:
                return None
            if self._q[0][0] > now:
                return None
            return self._q.pop(0)

    def _reschedule(self, when_ts: float, job: Job, letter: str) -> None:
        with self._lock:
            self._q.append((when_ts, job, letter))

    def _build_ability(self, letter: str) -> Optional[ParsedAbility]:
        for cls in CLASS_ORDER:
            info = build_ability_text_and_cd(cls, letter)
            if info:
                txt, cd, uses_voices = info
                return ParsedAbility(letter, txt, cd, cls, uses_voices)
        return None

    def _candidates_for_ability(self, ability: ParsedAbility) -> List[TokenHandler]:
        out: List[TokenHandler] = []
        for t in self.tm.all_buffers():
            if not t.enabled:
                continue
            if t.is_captcha_paused():
                continue
            if t.needs_manual_voices:
                continue

            class_data = CLASS_ABILITIES.get(t.class_type)
            if not class_data:
                continue
            if ability.key not in class_data["abilities"]:
                continue

            if t.class_type == "apostle" and ability.key in RACE_NAMES:
                if not t.has_race(ability.key):
                    # Попробуем добавить временную расу если это апостол и отправитель команды !баф
                    continue

            if ability.uses_voices and t.voices <= 0:
                continue

            can, _ = t.can_use_ability(ability.key)
            if not can:
                continue

            out.append(t)

        random.shuffle(out)
        return out

    def _run_loop(self):
        while True:
            item = self._pop_ready()
            if not item:
                time.sleep(0.2)
                continue

            _, job, letter = item
            ability = self._build_ability(letter)
            if not ability:
                logging.warning(f"⚠️ Unknown letter '{letter}'")
                continue

            candidates = self._candidates_for_ability(ability)
            if not candidates:
                self._reschedule(time.time() + 5.0, job, letter)
                continue

            processed = False
            last_status = "UNKNOWN"

            for token in candidates:
                ok, status = self.executor.execute_one(token, ability, job)
                last_status = status

                if status in ("SUCCESS", "ALREADY"):
                    processed = True
                    break

                if status == "TRIGGER_NOT_FOUND_IN_SOURCE":
                    continue

            if not processed:
                delay = 10.0
                m = re.search(r"\((\d+)s\)", last_status)
                if m:
                    try:
                        sec = int(m.group(1))
                        delay = max(10.0, float(sec))
                    except Exception:
                        pass
                self._reschedule(time.time() + delay, job, letter)


# ===== Auto Voices Restorer =====
class AutoVoicesRestorer:
    def __init__(self, tm: TokenManager, executor: AbilityExecutor):
        self.tm = tm
        self.executor = executor
        self._thr = threading.Thread(target=self._loop, daemon=True)
        self._thr.start()

    def _loop(self):
        while True:
            try:
                now = now_ts()
                for t in self.tm.all_buffers():
                    if not t.enabled:
                        continue
                    if t.is_captcha_paused():
                        continue
                    if t.needs_manual_voices:
                        continue

                    if t.voices > 0:
                        if t.virtual_voice_grants or t.next_virtual_grant_ts:
                            t.virtual_voice_grants = 0
                            t.next_virtual_grant_ts = 0
                            t._manager.save()
                        continue

                    if t.next_virtual_grant_ts and now < t.next_virtual_grant_ts:
                        continue

                    if t.virtual_voice_grants >= 4:
                        t.needs_manual_voices = True
                        t._manager.save()
                        logging.warning(f"🛑 {t.name}: needs_manual_voices=True (auto attempts exhausted)")
                        continue

                    if t.class_type == "apostle" or t.class_type in ("crusader", "light_incarnation"):
                        ok = self.executor.refresh_profile(t)
                        t.virtual_voice_grants += 1
                        t.next_virtual_grant_ts = now + 2 * 60 * 60
                        t._manager.save()

                        if ok and t.voices > 0:
                            logging.info(f"✅ {t.name}: voices restored via profile ({t.voices})")
                        else:
                            logging.info(f"⏳ {t.name}: profile check done, voices still 0 (attempt {t.virtual_voice_grants}/4)")
                    else:
                        t.virtual_voice_grants += 1
                        t.next_virtual_grant_ts = now + 6 * 60 * 60
                        t.voices = 1
                        t._manager.save()
                        logging.info(f"🧪 {t.name}: virtual voices grant +1 (attempt {t.virtual_voice_grants}/4)")

                time.sleep(30)
            except Exception as e:
                logging.error(f"❌ AutoVoicesRestorer error: {e}")
                time.sleep(5)


# ===== Observer Bot (LONG POLL) =====
class ObserverBot:
    def __init__(self, tm: TokenManager, executor: AbilityExecutor):
        self.tm = tm
        self.executor = executor
        self.scheduler = Scheduler(tm, executor)

        self.observer = tm.get_observer()
        if not self.observer.access_token:
            raise RuntimeError("Observer token has empty access_token")
        if not self.observer.source_peer_id:
            raise RuntimeError("Observer source_chat_id is missing")

        # Эти настройки остаются для target polling
        self.poll_interval = float(tm.settings.get("poll_interval", 2.0))
        self.poll_count = int(tm.settings.get("poll_count", 20))

        logging.info("🤖 MultiTokenBot STARTED (Observer=LongPoll)")
        logging.info(f"📋 Tokens: {len(tm.tokens)}")
        logging.info(f"🛰️ Target poll: interval={self.poll_interval}s, count={self.poll_count}")

        AutoVoicesRestorer(tm, executor)

        # LongPoll state
        self._lp_server: str = ""
        self._lp_key: str = ""
        self._lp_ts: str = ""

    def _parse_baf_letters(self, text: str) -> str:
        text_n = normalize_text(text)
        if not text_n.startswith("!баф"):
            return ""
        s = text_n[4:].strip()
        if not s:
            return ""
        s = s[:4]

        allowed = set()
        for cls in CLASS_ABILITIES.values():
            allowed.update(cls["abilities"].keys())

        out = "".join([ch for ch in s if ch in allowed])
        return out[:4]

    def _is_apo_cmd(self, text: str) -> bool:
        return normalize_text(text).startswith("!апо")

    def _parse_doprasa_cmd(self, text: str) -> Optional[Tuple[str, str]]:
        """Парсит команду /допраса"""
        t = (text or "").strip()
        if not normalize_text(t).startswith("/допраса"):
            return None
        
        # Убираем команду и разделяем
        parts = t.split()
        if len(parts) != 2:
            return None
        
        race = parts[1].strip().lower()
        if race not in RACE_NAMES:
            return None
        
        return race, text  # возвращаем расу и оригинальный текст

    def _format_apo_status(self) -> str:
        apostles = [t for t in self.tm.all_buffers() if t.class_type == "apostle"]
        warlocks = [t for t in self.tm.all_buffers() if t.class_type == "warlock"]
        paladins = [t for t in self.tm.all_buffers() if t.class_type in ("crusader", "light_incarnation")]

        lines: List[str] = []
        if apostles:
            lines.append("Апостолы:")
            for t in apostles:
                races = t.get_all_races_display()
                extra = " (manual)" if t.needs_manual_voices else ""
                lines.append(f"{t.name}({races}) 🗣Голосов: {t.voices}{extra}")
            lines.append("")

        if warlocks:
            lines.append("Прокли:")
            for t in warlocks:
                extra = " (manual)" if t.needs_manual_voices else ""
                lines.append(f"{t.name} 🗣Голосов: {t.voices}{extra}")
            lines.append("")

        if paladins:
            lines.append("Паладины:")
            for t in paladins:
                extra = " (manual)" if t.needs_manual_voices else ""
                lines.append(f"{t.name} (lvl {t.level}) 🗣Голосов: {t.voices}{extra}")
            lines.append("")

        if not lines:
            return "Нет баферов в конфиге."
        return "\n".join(lines).strip()

    def _parse_golosa_cmd(self, text: str) -> Optional[Tuple[str, int]]:
        t = (text or "").strip()
        if not normalize_text(t).startswith("!голоса"):
            return None
        parts = t.split()
        if len(parts) != 3:
            return None
        name = parts[1].strip()
        try:
            n = int(parts[2].strip())
        except Exception:
            return None
        if not name:
            return None
        return name, max(0, n)

    def _apply_manual_voices_by_name(self, name: str, n: int) -> str:
        token = self.tm.get_token_by_name(name)
        if not token:
            return f"❌ Токен с именем '{name}' не найден."
        token.update_voices_manual(n)
        return f"✅ {token.name}: голоса выставлены = {n}"

    def _apply_doprasa_by_sender(self, sender_id: int, race_key: str, original_text: str) -> str:
        """Добавляет временную расу для апостола по ID отправителя"""
        # Находим токен по sender_id
        token = self.tm.get_token_by_sender_id(sender_id)
        if not token:
            return f"❌ Апостол с вашим ID ({sender_id}) не найден в системе."
        
        if token.class_type != "apostle":
            return f"❌ Токен {token.name} не является апостолом."
        
        # Проверяем что такая раса существует
        if race_key not in RACE_NAMES:
            return f"❌ Неизвестная раса '{race_key}'. Доступные: {', '.join(RACE_NAMES.keys())}"
        
        # Проверяем что такой расы еще нет
        if token.has_race(race_key):
            race_name = RACE_NAMES.get(race_key, race_key)
            return f"⚠️ У {token.name} уже есть раса '{race_name}'."
        
        # Добавляем временную расу на 2 часа
        success = token.add_temporary_race(race_key, duration_hours=2)
        if success:
            race_name = RACE_NAMES.get(race_key, race_key)
            expires_at = token.temp_races[-1]["expires"]
            expires_time = datetime.fromtimestamp(expires_at).strftime('%H:%M')
            return f"✅ {token.name}: добавлена временная раса '{race_name}' до {expires_time}"
        else:
            return f"❌ Не удалось добавить расу для {token.name}."

    def _lp_get_server(self) -> bool:
        data = {
            "access_token": self.observer.access_token,
            "v": VK_API_VERSION,
            "lp_version": 3,
        }
        ret = self.observer._vk.call(self.observer._vk.post("messages.getLongPollServer", data))
        if "error" in ret:
            err = ret["error"]
            logging.error(f"❌ LongPollServer error {err.get('error_code')} {err.get('error_msg')}")
            return False
        resp = ret.get("response", {})
        self._lp_server = str(resp.get("server", "")).strip()
        self._lp_key = str(resp.get("key", "")).strip()
        self._lp_ts = str(resp.get("ts", "")).strip()
        if not self._lp_server or not self._lp_key or not self._lp_ts:
            logging.error("❌ LongPollServer: missing server/key/ts")
            return False
        logging.info(f"✅ LongPoll initialized: server={self._lp_server}, ts={self._lp_ts}")
        return True

    def _lp_check(self) -> Optional[Dict[str, Any]]:
        server = self._lp_server
        if not server.startswith("http"):
            server = "https://" + server.lstrip("/")

        # Используем raw_post для LongPoll запросов
        data = {
            "act": "a_check",
            "key": self._lp_key,
            "ts": self._lp_ts,
            "wait": 25,
            "mode": 2,
            "version": 3,
        }
        try:
            # Используем raw_post вместо post
            ret = self.observer._vk.call(self.observer._vk.raw_post(server, data))
            return ret
        except Exception as e:
            logging.error(f"❌ LongPoll a_check exception: {e}", exc_info=True)
            return None

    def _handle_new_message(self, msg_item: Dict[str, Any]) -> None:
        text = (msg_item.get("text") or "").strip()
        from_id = int(msg_item.get("from_id", 0))
        peer_id = int(msg_item.get("peer_id", 0))
        mid = int(msg_item.get("id", 0))

        if peer_id != self.observer.source_peer_id:
            return
        if from_id <= 0:
            return
        if not text:
            return

        # !апо
        if self._is_apo_cmd(text):
            status = self._format_apo_status()
            self.observer.send_to_peer(self.observer.source_peer_id, status, None)
            return

        # /допраса <раса> - добавление временной расы
        doprasa = self._parse_doprasa_cmd(text)
        if doprasa is not None:
            race_key, original_text = doprasa
            response = self._apply_doprasa_by_sender(from_id, race_key, original_text)
            self.observer.send_to_peer(self.observer.source_peer_id, response, None)
            return

        # !голоса <name> <N>
        parsed = self._parse_golosa_cmd(text)
        if parsed is not None:
            name, n = parsed
            token = self.tm.get_token_by_name(name)
            if not token:
                self.observer.send_to_peer(self.observer.source_peer_id, f"❌ Токен с именем '{name}' не найден.", None)
                return

            # ленивый автодетект owner_vk_id
            if token.owner_vk_id == 0:
                token.fetch_owner_id_lazy()

            if token.owner_vk_id != 0 and token.owner_vk_id != from_id:
                logging.warning(
                    f"⚠️ Unauthorized !голоса by {from_id} for token {token.name} (owner={token.owner_vk_id})"
                )
                return

            reply = self._apply_manual_voices_by_name(name, n)
            self.observer.send_to_peer(self.observer.source_peer_id, reply, None)
            return

        # !баф
        letters = self._parse_baf_letters(text)
        if letters:
            # Проверяем, не нужно ли добавить временные расы для расовых баффов
            for letter in letters:
                if letter in RACE_NAMES:
                    # Находим апостола по ID отправителя
                    token = self.tm.get_token_by_sender_id(from_id)
                    if token and token.class_type == "apostle":
                        # Если у апостола нет такой расы, добавляем временную
                        if not token.has_race(letter):
                            token.add_temporary_race(letter, duration_hours=2)
                            race_name = RACE_NAMES.get(letter, letter)
                            logging.info(f"🎯 Автоматически добавлена временная раса '{race_name}' для {token.name} по команде !баф")
            
            job = Job(
                sender_id=from_id,
                trigger_text=text,
                letters=letters,
                created_ts=time.time(),
            )
            logging.info(f"🎯 !баф from {from_id}: {letters} [observer={self.observer.name}]")
            self.scheduler.enqueue_letters(job, letters)

    def run(self):
        if not self._lp_get_server():
            raise RuntimeError("Failed to init long poll server")

        logging.info(f"✅ LongPoll готов к работе. Начинаю слушать чат {self.observer.source_peer_id}")

        while True:
            try:
                lp = self._lp_check()
                if not lp:
                    logging.warning("⚠️ LongPoll check returned None, retrying in 2 seconds...")
                    time.sleep(2)
                    continue

                # возможные ответы: {"ts": "...", "updates": [...]}
                # или {"failed": 1/2/3, ...}
                if "failed" in lp:
                    failed = int(lp.get("failed", 0))
                    if failed in (1, 2):
                        # 1: ts out of date, 2: key expired
                        logging.warning(f"⚠️ LongPoll failed={failed}, re-init server")
                        self._lp_get_server()
                        continue
                    if failed == 3:
                        logging.warning("⚠️ LongPoll failed=3, full re-init")
                        self._lp_get_server()
                        continue
                    logging.warning(f"⚠️ LongPoll unknown failed={failed}, re-init")
                    self._lp_get_server()
                    continue

                new_ts = lp.get("ts")
                if new_ts is not None:
                    self._lp_ts = str(new_ts)

                updates = lp.get("updates", []) or []
                if not updates:
                    continue

                # LP v3: new message event is type=4
                # update: [4, msg_id, flags, peer_id, ts, text, ...]
                msg_ids: List[int] = []
                for u in updates:
                    if not isinstance(u, list) or not u:
                        continue
                    if int(u[0]) != 4:
                        continue
                    try:
                        msg_id = int(u[1])
                        peer_id = int(u[3])
                    except Exception:
                        continue

                    if peer_id == self.observer.source_peer_id:
                        msg_ids.append(msg_id)

                if not msg_ids:
                    continue

                # ВАЖНО: LP не даёт from_id, поэтому берём точные данные через getById
                items = self.observer.get_by_id(msg_ids)
                for it in items:
                    self._handle_new_message(it)

            except Exception as e:
                logging.error(f"❌ Observer longpoll loop error: {e}", exc_info=True)
                time.sleep(2)


# ===== main =====
def main():
    try:
        vk = VKAsyncClient()
        tm = TokenManager("config.json", vk)
        executor = AbilityExecutor(tm)
        ObserverBot(tm, executor).run()

    except (FileNotFoundError, json.JSONDecodeError):
        logging.error("❌ config.json не найден или содержит ошибку. Пожалуйста, создайте/исправьте файл.")
        if not os.path.exists("config.json"):
            with open("config.json", "w", encoding="utf-8") as f:
                template = {
                    "observer_token_id": "main_observer",
                    "settings": {
                        "poll_interval": 2.0,
                        "poll_count": 20
                    },
                    "tokens": [
                        {
                            "id": "main_observer",
                            "name": "Observer",
                            "class": "observer",
                            "access_token": "vk1.a.YOUR_TOKEN",
                            "owner_vk_id": 0,
                            "source_chat_id": 12345,
                            "target_peer_id": -183040898,
                            "voices": 0,
                            "enabled": True,
                            "races": [],
                            "temp_races": [],
                            "captcha_until": 0
                        }
                    ]
                }
                json.dump(template, f, ensure_ascii=False, indent=2)
            logging.info("📄 Создан шаблон config.json. Заполните токены и observer_token_id.")

    except Exception as e:
        logging.critical(f"💥 Критическая ошибка при запуске: {e}", exc_info=True)


if __name__ == "__main__":
    main()
