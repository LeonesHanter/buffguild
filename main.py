# -*- coding: utf-8 -*-
import asyncio
import aiohttp
import json
import logging
import random
import threading
import time
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
from logging.handlers import RotatingFileHandler
from concurrent.futures import TimeoutError as FuturesTimeoutError

# =========================
# LOGGING
# =========================
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

VK_API_BASE = "https://api.vk.com/method"
VK_API_VERSION = "5.131"

# =========================
# REACTIONS
# =========================
REACTION_FAIL = 7   # 😢
REACTION_OK = 16    # 🎉

# =========================
# BOT CONSTANTS
# =========================
MAX_BUFF_LETTERS = 4
COLLECT_WINDOW = 1.2
REQUEST_TTL = 12.0

# Poll result from target:
POLL_TRIES = 4
POLL_SLEEP_SECONDS = 10.0  # <--- requested: 10 sec between polls

# Captcha ban:
CAPTCHA_BAN_SECONDS = 60.0  # <--- requested: 1 minute pause

# Jitter:
SEND_JITTER_MIN = 0.10
SEND_JITTER_MAX = 0.20

RACE_KEYS = {"ч", "г", "н", "э", "м", "д", "о"}

CLASS_ORDER = ["apostle", "warlock", "crusader", "light_incarnation"]

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
        "name": "Чернокнижник",
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
        "name": "Крестоносец",
        "prefix": "",
        "uses_voices": False,
        "default_cooldown": None,
        "abilities": {
            "в": ("воскрешение", 6 * 60 * 60),
            "т": ("очищение огнем", 15 * 60 + 10),
        },
    },
    "light_incarnation": {
        "name": "Воплощение света",
        "prefix": "",
        "uses_voices": False,
        "default_cooldown": None,
        "abilities": {
            "и": ("очищение", 61),
            "с": ("очищение светом", 15 * 60 + 10),
        },
    },
}

# =========================
# HELPERS
# =========================
def now_ts() -> float:
    return time.time()

def clamp(x: float, a: float, b: float) -> float:
    return max(a, min(b, x))

def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default

def jitter_sleep():
    time.sleep(random.uniform(SEND_JITTER_MIN, SEND_JITTER_MAX))

# =========================
# VK Async Client
# =========================
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
        timeout = aiohttp.ClientTimeout(total=12)
        connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300)
        self._session = aiohttp.ClientSession(timeout=timeout, connector=connector)

    def call(self, coro):
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            return fut.result(timeout=25)
        except FuturesTimeoutError:
            fut.cancel()
            raise

    async def post(self, method: str, data: Dict[str, Any]) -> Dict[str, Any]:
        if not self._session:
            raise RuntimeError("VK session not ready")
        url = f"{VK_API_BASE}/{method}"
        async with self._session.post(url, data=data) as resp:
            return await resp.json()

    def close(self) -> None:
        async def _close():
            try:
                if self._session and not self._session.closed:
                    await self._session.close()
            except Exception:
                pass

        try:
            self.call(_close())
        except Exception:
            pass
        try:
            self._loop.call_soon_threadsafe(self._loop.stop)
        except Exception:
            pass

# =========================
# Rate limiter (send)
# =========================
class SimpleRateLimiter:
    def __init__(self, max_per_minute: int = 40):
        self.max_per_minute = max_per_minute
        self._lock = threading.Lock()
        self._counters: Dict[str, Tuple[int, float]] = {}

    def allow(self, token_id: str) -> bool:
        now = now_ts()
        with self._lock:
            count, start = self._counters.get(token_id, (0, now))
            if now - start >= 60:
                self._counters[token_id] = (1, now)
                return True
            if count < self.max_per_minute:
                self._counters[token_id] = (count + 1, start)
                return True
            return False

# =========================
# Parsed Ability
# =========================
@dataclass
class ParsedAbility:
    key: str
    text: str
    cooldown: int
    class_type: str
    uses_voices: bool
    processed: bool = False

# =========================
# Token Handler
# =========================
class TokenHandler:
    def __init__(self, cfg: Dict[str, Any], vk: VKAsyncClient, rate_limiter: SimpleRateLimiter):
        self.id: str = cfg["id"]
        self.name: str = cfg.get("name", self.id)
        self.class_type: str = cfg.get("class", "apostle")
        self.access_token: str = cfg["access_token"]

        self.source_chat_id: int = int(cfg["source_chat_id"])
        self.source_peer_id: int = 2000000000 + self.source_chat_id
        self.target_peer_id: int = int(cfg["target_peer_id"])

        self.voices: int = int(cfg.get("voices", 5))
        self.enabled: bool = bool(cfg.get("enabled", True))

        # captcha ban
        self.captcha_until: float = float(cfg.get("captcha_until", 0) or 0)

        raw_races = cfg.get("races", [])
        if isinstance(raw_races, list):
            self.races: List[str] = [str(x).strip().lower() for x in raw_races if str(x).strip()]
        else:
            self.races = []
        self.races = [r for r in self.races if r in RACE_KEYS]
        self.races = list(dict.fromkeys(self.races))

        self._vk = vk
        self._rate_limiter = rate_limiter

        self._ability_cd: Dict[str, float] = {}
        self.next_history_ts: float = 0.0
        self._io_lock = threading.Lock()

    def class_name(self) -> str:
        return CLASS_ABILITIES.get(self.class_type, {}).get("name", self.class_type)

    def is_available(self) -> bool:
        if not self.enabled:
            return False
        if now_ts() < self.captcha_until:
            return False
        return True

    def captcha_ban(self, seconds: float = CAPTCHA_BAN_SECONDS, context: str = "") -> None:
        until = now_ts() + float(seconds)
        self.captcha_until = until
        if context:
            logging.warning(f"🧩 {self.name}: captcha ban {int(seconds)}s ({context})")
        else:
            logging.warning(f"🧩 {self.name}: captcha ban {int(seconds)}s")

    def can_use_ability(self, ability_key: str) -> Tuple[bool, float]:
        ts = self._ability_cd.get(ability_key, 0.0)
        rem = ts - now_ts()
        if rem > 0:
            return False, rem
        return True, 0.0

    def set_ability_cooldown(self, ability_key: str, seconds: int) -> None:
        self._ability_cd[ability_key] = now_ts() + int(max(1, seconds))

    async def _messages_send(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return await self._vk.post("messages.send", data)

    async def _messages_send_reaction(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return await self._vk.post("messages.sendReaction", data)

    def send_reaction(self, peer_id: int, cmid: int, reaction_id: int) -> bool:
        if cmid is None or int(cmid) <= 0:
            return False
        if not self.is_available():
            return False

        # jitter before reaction (requested)
        jitter_sleep()

        async def _send():
            data = {
                "access_token": self.access_token,
                "v": VK_API_VERSION,
                "peer_id": int(peer_id),
                "cmid": int(cmid),
                "reaction_id": int(reaction_id),
            }
            return await self._messages_send_reaction(data)

        with self._io_lock:
            try:
                ret = self._vk.call(_send())
            except Exception as e:
                logging.error(f"❌ {self.name}: sendReaction exception: {e}")
                return False

        if "error" in ret:
            err = ret["error"]
            code = err.get("error_code")
            msg = err.get("error_msg")
            if safe_int(code) == 14:
                self.captcha_ban(context="sendReaction")
            logging.warning(f"⚠️ {self.name}: sendReaction error {code} {msg} (peer={peer_id} cmid={cmid})")
            return False

        emoji = {REACTION_FAIL: "😢", REACTION_OK: "🎉"}.get(reaction_id, str(reaction_id))
        logging.info(f"🙂 {self.name}: реакция {emoji} поставлена (peer={peer_id} cmid={cmid})")
        return True

    def send_text(
        self,
        peer_id: int,
        text: str,
        reply_to: Optional[int] = None,
        forward_peer_id: Optional[int] = None,
        forward_conversation_message_id: Optional[int] = None,
    ) -> Tuple[bool, Optional[int], Optional[int], Optional[str]]:
        if not self.is_available():
            return False, None, -100, "token_not_available"
        if not self._rate_limiter.allow(self.id):
            return False, None, -1, "rate_limited"

        # jitter before send (requested)
        jitter_sleep()

        async def _send():
            data: Dict[str, Any] = {
                "access_token": self.access_token,
                "v": VK_API_VERSION,
                "peer_id": int(peer_id),
                "message": text,
                "random_id": random.randrange(1, 2_000_000_000),
                "disable_mentions": 1,
            }
            if reply_to is not None:
                data["reply_to"] = int(reply_to)

            if forward_peer_id is not None and forward_conversation_message_id is not None:
                data["forward"] = json.dumps(
                    {
                        "peer_id": int(forward_peer_id),
                        "conversation_message_ids": [int(forward_conversation_message_id)],
                        "is_reply": 0,
                    },
                    ensure_ascii=False,
                )

            ret = await self._messages_send(data)
            return ret

        with self._io_lock:
            try:
                ret = self._vk.call(_send())
            except Exception as e:
                return False, None, -2, f"exception: {e}"

        if "error" in ret:
            err = ret["error"]
            code = safe_int(err.get("error_code"), -3)
            msg = str(err.get("error_msg"))

            # Captcha -> ban 60s, NOT disable
            if code == 14:
                self.captcha_ban(context="messages.send")
            return False, None, code, msg

        msg_id = None
        try:
            msg_id = int(ret.get("response"))
        except Exception:
            msg_id = None

        return True, msg_id, None, None

    def get_history(self, peer_id: int, count: int = 30) -> Tuple[List[Dict[str, Any]], Optional[int], Optional[str]]:
        if not self.is_available():
            return [], -100, "token_not_available"

        now = now_ts()
        if now < self.next_history_ts:
            return [], 9, "backoff"

        async def _get():
            data = {
                "access_token": self.access_token,
                "v": VK_API_VERSION,
                "peer_id": int(peer_id),
                "count": int(count),
            }
            return await self._vk.post("messages.getHistory", data)

        with self._io_lock:
            try:
                ret = self._vk.call(_get())
            except Exception as e:
                return [], -2, f"exception: {e}"

        if "error" in ret:
            err = ret["error"]
            code = safe_int(err.get("error_code"), -3)
            msg = str(err.get("error_msg"))
            logging.error(f"❌ {self.name}: getHistory error {code} {msg}")

            if code == 9:
                self.next_history_ts = now_ts() + random.randint(10, 20)
                logging.warning(f"🧊 {self.name}: history backoff {int(self.next_history_ts - now_ts())}s")

            if code == 14:
                self.captcha_ban(context="getHistory")

            if code == 5:
                # invalid token: still disabling (this is not captcha)
                self.enabled = False
                logging.error(f"⛔ {self.name}: disabled (invalid access_token)")

            return [], code, msg

        items = ret.get("response", {}).get("items", [])
        return items, None, None

# =========================
# Request session collector
# =========================
@dataclass
class RequestSession:
    key: str
    created_ts: float
    sender_id: int
    abilities: List[ParsedAbility]
    sightings: Dict[str, Tuple[int, int]] = field(default_factory=dict)  # token_id -> (source_peer_id, cmid)
    finalized: bool = False

# =========================
# Ability Executor
# =========================
class AbilityExecutor:
    def execute_one(
        self,
        token: TokenHandler,
        ability: ParsedAbility,
        source_peer_id: int,
        source_conv_msg_id: int,
    ) -> Tuple[bool, str]:
        can, rem = token.can_use_ability(ability.key)
        if not can:
            token.send_reaction(source_peer_id, source_conv_msg_id, REACTION_FAIL)
            return False, f"COOLDOWN_LOCAL({rem:.1f}s)"

        if token.class_type == "apostle" and ability.key in RACE_KEYS:
            if ability.key not in token.races:
                return False, "RACE_NOT_ALLOWED"

        baseline_id = self._last_msg_id(token, token.target_peer_id)

        # 1) forward trigger to target
        ok, fwd_msg_id, ecode, emsg = token.send_text(
            peer_id=token.target_peer_id,
            text=" ",
            forward_peer_id=source_peer_id,
            forward_conversation_message_id=source_conv_msg_id,
        )
        if not ok or not fwd_msg_id:
            token.send_reaction(source_peer_id, source_conv_msg_id, REACTION_FAIL)
            if ecode == 14:
                return False, "CAPTCHA"
            if ecode == 5:
                return False, "INVALID"
            if ecode == 9:
                return False, "FLOOD"
            return False, f"SEND_ERROR_FORWARD({ecode} {emsg})"

        # 2) send ability reply_to forwarded msg_id
        ok2, _mid2, e2, m2 = token.send_text(
            peer_id=token.target_peer_id,
            text=ability.text,
            reply_to=fwd_msg_id,
        )
        if not ok2:
            token.send_reaction(source_peer_id, source_conv_msg_id, REACTION_FAIL)
            if e2 == 14:
                return False, "CAPTCHA"
            if e2 == 5:
                return False, "INVALID"
            if e2 == 9:
                token.set_ability_cooldown(ability.key, 20)
                return False, "FLOOD"
            return False, f"SEND_ERROR({e2} {m2})"

        # 3) poll result (10s between polls)
        status, cd = self._poll_result(token, token.target_peer_id, baseline_id)

        if status == "SUCCESS":
            ability.processed = True
            token.set_ability_cooldown(ability.key, ability.cooldown)
            if ability.uses_voices:
                token.voices = max(0, token.voices - 1)
            token.send_reaction(source_peer_id, source_conv_msg_id, REACTION_OK)
            return True, "SUCCESS"

        if status == "ALREADY":
            ability.processed = True
            token.send_reaction(source_peer_id, source_conv_msg_id, REACTION_FAIL)
            return True, "ALREADY"

        if status == "NO_VOICES":
            token.voices = 0
            token.send_reaction(source_peer_id, source_conv_msg_id, REACTION_FAIL)
            return False, "NO_VOICES"

        if status == "COOLDOWN":
            sec = int(max(10, cd or 30))
            token.set_ability_cooldown(ability.key, sec)
            token.send_reaction(source_peer_id, source_conv_msg_id, REACTION_FAIL)
            return False, f"COOLDOWN({sec}s)"

        token.send_reaction(source_peer_id, source_conv_msg_id, REACTION_FAIL)
        return False, status

    def _last_msg_id(self, token: TokenHandler, peer_id: int) -> int:
        items, _c, _m = token.get_history(peer_id, count=1)
        if items:
            return safe_int(items[0].get("id"), 0)
        return 0

    def _poll_result(self, token: TokenHandler, peer_id: int, baseline_id: int) -> Tuple[str, int]:
        for i in range(1, POLL_TRIES + 1):
            time.sleep(POLL_SLEEP_SECONDS)

            items, _c, _m = token.get_history(peer_id, count=80)
            new_msgs = [m for m in items if safe_int(m.get("id"), 0) > baseline_id]
            status, cd = self._parse_result(new_msgs)
            if status != "UNKNOWN":
                return status, cd
            logging.info(f"🕵️ [{token.name}] no result yet (poll {i}/{POLL_TRIES})")

        return "UNKNOWN", 0

    def _parse_result(self, msgs: List[Dict[str, Any]]) -> Tuple[str, int]:
        for m in msgs:
            text = (m.get("text", "") or "").lower()

            if "на вас наложено" in text or ("наложено" in text and ("благослов" in text or "проклят" in text)):
                return "SUCCESS", 0

            if (
                ("на эту цель" in text and "уже действует" in text)
                or ("уже действует" in text and ("благослов" in text or "проклят" in text))
                or ("нельзя наложить" in text and ("уже име" in text or "уже есть" in text))
                or ("уже наложено" in text and ("благослов" in text or "проклят" in text))
            ):
                return "ALREADY", 0

            if "требуется голос" in text or "нет голос" in text or "голос древних" in text:
                return "NO_VOICES", 0

            if (
                ("слишком часто" in text) or ("подождите" in text) or ("доступно через" in text)
                or ("попробуйте позже" in text) or ("социальные эффекты" in text and "определенное время" in text)
            ):
                return "COOLDOWN", self._extract_cd_seconds(text)

        return "UNKNOWN", 0

    def _extract_cd_seconds(self, text: str) -> int:
        minutes = 0
        seconds = 0
        m = re.search(r"(\d+)\s*(минут|минута|минуты|мин)\b", text)
        if m:
            minutes = int(m.group(1))
        s = re.search(r"(\d+)\s*(секунд|секунда|секунды|сек)\b", text)
        if s:
            seconds = int(s.group(1))
        if minutes or seconds:
            return minutes * 60 + seconds
        n = re.search(r"\b(\d+)\b", text)
        if n:
            return int(n.group(1))
        return 30

# =========================
# MultiToken Bot
# =========================
class MultiTokenBot:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self._lock = threading.Lock()

        self.vk = VKAsyncClient()
        self.rate_limiter = SimpleRateLimiter(max_per_minute=40)
        self.executor = AbilityExecutor()

        self.config: Dict[str, Any] = {}
        self.tokens: List[TokenHandler] = []

        self.poll_interval = 2.0
        self.poll_count = 20

        self.sources: Dict[int, TokenHandler] = {}
        self.last_msg_ids: Dict[str, int] = {}

        self.sessions: Dict[str, RequestSession] = {}
        self.sessions_lock = threading.Lock()

        # round-robin poll state
        self._source_peer_ids: List[int] = []
        self._rr_idx = 0

        self._running = False

        self.load()
        self.last_msg_ids = self._load_last_msg_ids()

        logging.info("🤖 MultiTokenBot STARTED")
        logging.info(f"📋 Tokens: {len(self.tokens)}")
        logging.info(f"📁 Source chats: {len(self.sources)}")
        logging.info(f"🛰️ Poll interval: {self.poll_interval:.1f}s, poll_count={self.poll_count}")

    def load(self) -> None:
        with open(self.config_path, "r", encoding="utf-8") as f:
            self.config = json.load(f)

        settings = self.config.get("settings", {}) if isinstance(self.config, dict) else {}
        self.poll_interval = float(settings.get("poll_interval", 2.0))
        self.poll_count = int(settings.get("poll_count", 20))

        self.tokens = []
        self.sources = {}

        for t_cfg in self.config.get("tokens", []):
            t = TokenHandler(t_cfg, self.vk, self.rate_limiter)
            self.tokens.append(t)
            self.sources[t.source_peer_id] = t

        self._source_peer_ids = list(self.sources.keys())
        self._source_peer_ids.sort()
        self._rr_idx = 0

    def save_config(self) -> None:
        with self._lock:
            tokens_payload = []
            for t in self.tokens:
                orig = None
                for x in self.config.get("tokens", []):
                    if x.get("id") == t.id:
                        orig = dict(x)
                        break
                if orig is None:
                    orig = {"id": t.id}

                orig["name"] = t.name
                orig["class"] = t.class_type
                orig["access_token"] = t.access_token
                orig["source_chat_id"] = t.source_chat_id
                orig["target_peer_id"] = t.target_peer_id
                orig["voices"] = t.voices
                orig["enabled"] = t.enabled
                orig["races"] = t.races

                # persist captcha ban
                orig["captcha_until"] = int(t.captcha_until) if t.captcha_until else 0

                tokens_payload.append(orig)

            self.config["tokens"] = tokens_payload

            with open(self.config_path, "w", encoding="utf-8") as f:
                json.dump(self.config, f, ensure_ascii=False, indent=2)

    def _load_last_msg_ids(self) -> Dict[str, int]:
        try:
            with open("last_msg_ids.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return {str(k): int(v) for k, v in data.items()}
        except Exception:
            pass
        return {}

    def _save_last_msg_ids(self) -> None:
        try:
            with open("last_msg_ids.json", "w", encoding="utf-8") as f:
                json.dump(self.last_msg_ids, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.error(f"❌ Failed to save last_msg_ids: {e}")

    def parse_command_text(self, text: str) -> List[ParsedAbility]:
        text = (text or "").strip().lower()
        if not text.startswith("!баф"):
            return []
        cmd = text[4:].strip()
        if not cmd:
            return []
        cmd = "".join(ch for ch in cmd if ch.isalpha())[:MAX_BUFF_LETTERS]
        if not cmd:
            return []

        abilities: List[ParsedAbility] = []
        for ch in cmd:
            for class_type in CLASS_ORDER:
                info = self._build_ability_text_and_cd(class_type, ch)
                if info:
                    txt, cd, uses_voices = info
                    abilities.append(ParsedAbility(ch, txt, cd, class_type, uses_voices))
                    break
        return abilities

    def _build_ability_text_and_cd(self, class_type: str, key: str) -> Optional[Tuple[str, int, bool]]:
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

    def _make_session_key(self, sender_id: int, abilities: List[ParsedAbility]) -> str:
        keys = "".join(a.key for a in abilities)
        bucket = int(now_ts() // 2)
        return f"{sender_id}:{keys}:{bucket}"

    # ===== main loop (reduced getHistory) =====
    def run(self):
        self._running = True
        try:
            while self._running:
                updated_any = False
                self._cleanup_sessions()

                # round-robin: poll only ONE source chat per tick
                if self._source_peer_ids:
                    source_peer_id = self._source_peer_ids[self._rr_idx % len(self._source_peer_ids)]
                    self._rr_idx += 1

                    token = self.sources.get(source_peer_id)
                    if token and token.is_available():
                        items, code, _msg = token.get_history(source_peer_id, count=self.poll_count)
                        if code is None or code == 9:
                            last_id = int(self.last_msg_ids.get(str(source_peer_id), 0) or 0)
                            for m in reversed(items):
                                mid = safe_int(m.get("id"), 0)
                                if mid <= last_id:
                                    continue

                                last_id = mid
                                updated_any = True

                                text = (m.get("text", "") or "").strip()
                                sender_id = safe_int(m.get("from_id"), 0)
                                conv_msg_id = safe_int(m.get("conversation_message_id"), 0)

                                if sender_id <= 0 or conv_msg_id <= 0:
                                    continue

                                abilities = self.parse_command_text(text)
                                if not abilities:
                                    continue

                                session_key = self._make_session_key(sender_id, abilities)

                                logging.info(
                                    f"🎯 !баф from {sender_id}: "
                                    f"{''.join(a.key for a in abilities)} ({len(abilities)} abilities) "
                                    f"[token={token.name} source={source_peer_id} target={token.target_peer_id}]"
                                )

                                self._register_sighting(session_key, sender_id, abilities, token, source_peer_id, conv_msg_id)

                            self.last_msg_ids[str(source_peer_id)] = last_id

                if updated_any:
                    self._save_last_msg_ids()

                self._finalize_ready_sessions()

                # persist captcha_until changes relatively quickly
                self.save_config()

                time.sleep(self.poll_interval)

        except KeyboardInterrupt:
            logging.info("⏹️ Stopping...")
        finally:
            self._running = False
            self.save_config()
            self.vk.close()

    def _cleanup_sessions(self):
        with self.sessions_lock:
            now = now_ts()
            for k in list(self.sessions.keys()):
                s = self.sessions[k]
                if now - s.created_ts > REQUEST_TTL:
                    self.sessions.pop(k, None)

    def _register_sighting(
        self,
        session_key: str,
        sender_id: int,
        abilities: List[ParsedAbility],
        token: TokenHandler,
        source_peer_id: int,
        conv_msg_id: int,
    ):
        with self.sessions_lock:
            s = self.sessions.get(session_key)
            if not s:
                s = RequestSession(
                    key=session_key,
                    created_ts=now_ts(),
                    sender_id=sender_id,
                    abilities=abilities,
                    sightings={},
                    finalized=False,
                )
                self.sessions[session_key] = s
            s.sightings[token.id] = (source_peer_id, conv_msg_id)

    def _finalize_ready_sessions(self):
        to_finalize: List[RequestSession] = []
        with self.sessions_lock:
            now = now_ts()
            for s in self.sessions.values():
                if s.finalized:
                    continue
                if now - s.created_ts >= COLLECT_WINDOW:
                    s.finalized = True
                    to_finalize.append(s)

        for s in to_finalize:
            threading.Thread(target=self._process_session, args=(s,), daemon=True).start()

    def _process_session(self, sess: RequestSession):
        abilities = sess.abilities[:]
        assigned: Dict[str, str] = {}

        tokens_with_sighting: List[TokenHandler] = []
        for t in self.tokens:
            if t.id in sess.sightings and t.is_available():
                tokens_with_sighting.append(t)

        if not tokens_with_sighting:
            logging.error(f"❌ Session {sess.key}: no tokens with sighting, skipping")
            return

        used_tokens: set[str] = set()

        for ab in abilities:
            cand = self._candidates_for_ability(tokens_with_sighting, ab)
            if not cand:
                logging.warning(f"⚠️ Session {sess.key}: no candidates for '{ab.key}'")
                continue

            chosen = None
            for t in cand:
                if t.id not in used_tokens:
                    chosen = t
                    break
            if not chosen:
                chosen = cand[0]

            assigned[ab.key] = chosen.id
            used_tokens.add(chosen.id)

        for ab in abilities:
            if ab.key not in assigned:
                continue

            first_token_id = assigned[ab.key]
            order = self._candidates_for_ability(tokens_with_sighting, ab)

            if order:
                idx = next((i for i, t in enumerate(order) if t.id == first_token_id), 0)
                order = order[idx:] + order[:idx]

            done = False
            for attempt_idx, token in enumerate(order, start=1):
                if token.id not in sess.sightings:
                    continue

                source_peer_id, conv_msg_id = sess.sightings[token.id]
                done2, status = self.executor.execute_one(
                    token=token,
                    ability=ab,
                    source_peer_id=source_peer_id,
                    source_conv_msg_id=conv_msg_id,
                )

                if status == "RACE_NOT_ALLOWED":
                    continue

                if done2:
                    if status == "SUCCESS":
                        logging.info(f"✅ {token.name}({token.class_name()}): {ab.text}")
                    else:
                        logging.info(f"ℹ️ {token.name}({token.class_name()}): {ab.text} {status}")
                    done = True
                    break

                logging.warning(f"⚠️ [{attempt_idx}] {token.name}({token.class_name()}): {ab.text} -> {status}")

            if not done:
                logging.error(f"❌ Session {sess.key}: ability '{ab.key}' not processed")

        self._log_stats(sess.sender_id, abilities)

    def _candidates_for_ability(self, tokens: List[TokenHandler], ab: ParsedAbility) -> List[TokenHandler]:
        out: List[TokenHandler] = []
        for t in tokens:
            if not t.is_available():
                continue

            class_data = CLASS_ABILITIES.get(t.class_type)
            if not class_data or ab.key not in class_data["abilities"]:
                continue

            can, _rem = t.can_use_ability(ab.key)
            if not can:
                continue

            if ab.uses_voices and t.voices <= 0:
                continue

            if t.class_type == "apostle" and ab.key in RACE_KEYS:
                if ab.key not in t.races:
                    continue

            out.append(t)

        # Prefer more voices, then random
        out.sort(key=lambda x: (x.voices, random.random()), reverse=True)
        return out

    def _log_stats(self, sender_id: int, abilities: List[ParsedAbility]) -> None:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        keys = "".join(a.key for a in abilities)
        success_count = sum(1 for a in abilities if a.processed)
        fail_count = len(abilities) - success_count
        details = ",".join(f"{a.key}:{'ok' if a.processed else 'fail'}" for a in abilities)
        line = f"{ts};sender={sender_id};abilities={keys};success={success_count};fail={fail_count};details={details}\n"
        try:
            with open("buff_stats.log", "a", encoding="utf-8") as f:
                f.write(line)
        except Exception as e:
            logging.error(f"❌ Failed to write buff_stats.log: {e}")

# =========================
# main
# =========================
if __name__ == "__main__":
    bot = MultiTokenBot("config.json")
    bot.run()
