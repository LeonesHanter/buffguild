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
# SETTINGS / CONSTANTS
# =========================
MAX_BUFF_LETTERS = 4

# Сколько времени "копим" одинаковую команду (если нужно) — тут Observer один, но оставим на будущее
COLLECT_WINDOW = 0.6

# Poll результата в target:
POLL_TRIES = 4
POLL_SLEEP_SECONDS = 10.0  # как просил: 10 секунд

# Captcha ban:
CAPTCHA_BAN_SECONDS = 60.0

# Jitter:
SEND_JITTER_MIN = 0.10
SEND_JITTER_MAX = 0.20

# Rate limit на send (мягкий, чтобы не лупить):
SEND_MAX_PER_MINUTE = 35

# Maintenance
PROFILE_REFRESH_EVERY = 2 * 60 * 60         # 2 часа
VIRTUAL_VOICE_EVERY = 6 * 60 * 60           # 6 часов
VIRTUAL_VOICE_ATTEMPTS_MAX = 4

# Reaction: only success
REACTION_OK = 16  # 🎉

# Allowed race keys for apostle race buffs
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
        "uses_voices": True,   # важно: у вас есть "Голос у Паладина", значит голоса есть
        "default_cooldown": None,
        "abilities": {
            "в": ("воскрешение", 6 * 60 * 60),
            "т": ("очищение огнем", 15 * 60 + 10),
        },
    },
    "light_incarnation": {
        "name": "Паладин",     # по голосам/строке — также паладин
        "prefix": "",
        "uses_voices": True,
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


def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def jitter_sleep():
    time.sleep(random.uniform(SEND_JITTER_MIN, SEND_JITTER_MAX))


def clamp(n: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, n))


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
        connector = aiohttp.TCPConnector(limit=80, ttl_dns_cache=300)
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
    def __init__(self, max_per_minute: int = 35):
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
# Parsed Ability / Task
# =========================
@dataclass
class ParsedAbility:
    key: str
    text: str
    cooldown: int
    class_type: str
    uses_voices: bool
    processed: bool = False


@dataclass(order=True)
class BuffTask:
    sort_ts: float
    task_id: str = field(compare=False)
    sender_id: int = field(compare=False)
    source_peer_id: int = field(compare=False)
    source_cmid: int = field(compare=False)
    target_peer_id: int = field(compare=False)
    ability: ParsedAbility = field(compare=False)

    attempts: int = field(default=0, compare=False)
    last_error: str = field(default="", compare=False)

    def bump(self, delay_sec: float, reason: str):
        self.sort_ts = now_ts() + max(0.2, float(delay_sec))
        self.last_error = reason
        self.attempts += 1


# =========================
# Token Handler
# =========================
class TokenHandler:
    def __init__(self, cfg: Dict[str, Any], vk: VKAsyncClient, rate_limiter: SimpleRateLimiter):
        self.id: str = cfg["id"]
        self.name: str = cfg.get("name", self.id)
        self.class_type: str = cfg.get("class", "apostle")
        self.access_token: str = cfg["access_token"]

        self.source_chat_id: int = int(cfg.get("source_chat_id", 0))
        self.source_peer_id: int = (2000000000 + self.source_chat_id) if self.source_chat_id else 0
        self.target_peer_id: int = int(cfg.get("target_peer_id", 0))

        self.voices: int = int(cfg.get("voices", 5))
        self.enabled: bool = bool(cfg.get("enabled", True))

        self.captcha_until: float = float(cfg.get("captcha_until", 0) or 0)

        raw_races = cfg.get("races", [])
        if isinstance(raw_races, list):
            self.races: List[str] = [str(x).strip().lower() for x in raw_races if str(x).strip()]
        else:
            self.races = []
        self.races = [r for r in self.races if r in RACE_KEYS]
        self.races = list(dict.fromkeys(self.races))

        # голоса-рекавери режим (для не-апостолов)
        self.voice_recover_attempts: int = int(cfg.get("voice_recover_attempts", 0) or 0)
        self.next_virtual_voice_ts: float = float(cfg.get("next_virtual_voice_ts", 0) or 0)
        self.needs_manual_voices: bool = bool(cfg.get("needs_manual_voices", False))

        # апостол: обновление профиля
        self.next_profile_refresh_ts: float = float(cfg.get("next_profile_refresh_ts", 0) or 0)

        self._vk = vk
        self._rate_limiter = rate_limiter

        # локальный кд на букву
        self._ability_cd: Dict[str, float] = {}

        # backoff на getHistory
        self.next_history_ts: float = 0.0

        # лок на IO (чтобы один токен не делал 2 запроса одновременно)
        self._io_lock = threading.Lock()

        # dirty для сохранения конфига
        self.dirty: bool = False

    def mark_dirty(self) -> None:
        self.dirty = True

    def class_name(self) -> str:
        return CLASS_ABILITIES.get(self.class_type, {}).get("name", self.class_type)

    def is_available(self) -> bool:
        if not self.enabled:
            return False
        if self.needs_manual_voices:
            return False
        if now_ts() < self.captcha_until:
            return False
        return True

    def captcha_ban(self, seconds: float = CAPTCHA_BAN_SECONDS, context: str = "") -> None:
        self.captcha_until = now_ts() + float(seconds)
        self.mark_dirty()
        if context:
            logging.warning(f"🧩 {self.name}: captcha pause {int(seconds)}s ({context})")
        else:
            logging.warning(f"🧩 {self.name}: captcha pause {int(seconds)}s")

    def update_voices(self, new_voices: int) -> None:
        new_voices = int(max(0, new_voices))
        if new_voices != self.voices:
            old = self.voices
            self.voices = new_voices
            self.mark_dirty()
            logging.info(f"🗣️ {self.name}: voices {old} → {new_voices}")

        # если после парса голоса появились — снимаем нужду ручного вмешательства
        if self.voices > 0 and self.needs_manual_voices:
            self.needs_manual_voices = False
            self.voice_recover_attempts = 0
            self.next_virtual_voice_ts = 0
            self.mark_dirty()
            logging.info(f"✅ {self.name}: manual-voices flag cleared (voices restored)")

    def set_manual_voices(self, n: int) -> None:
        n = int(max(0, n))
        self.voices = n
        self.needs_manual_voices = False
        self.voice_recover_attempts = 0
        self.next_virtual_voice_ts = 0
        self.mark_dirty()
        logging.info(f"🛠️ {self.name}: manual voices set to {n}")

    def can_use_ability(self, ability_key: str) -> Tuple[bool, float]:
        ts = self._ability_cd.get(ability_key, 0.0)
        rem = ts - now_ts()
        if rem > 0:
            return False, rem
        return True, 0.0

    def set_ability_cooldown(self, ability_key: str, seconds: int) -> None:
        self._ability_cd[ability_key] = now_ts() + int(max(1, seconds))

    def get_ability_cd_rem(self, ability_key: str) -> float:
        ts = self._ability_cd.get(ability_key, 0.0)
        return max(0.0, ts - now_ts())

    async def _messages_send(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return await self._vk.post("messages.send", data)

    async def _messages_send_reaction(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return await self._vk.post("messages.sendReaction", data)

    def send_reaction_ok(self, peer_id: int, cmid: int) -> bool:
        """Ставим ТОЛЬКО 🎉 на успех."""
        if cmid is None or int(cmid) <= 0:
            return False
        if not self.is_available():
            return False

        jitter_sleep()

        async def _send():
            data = {
                "access_token": self.access_token,
                "v": VK_API_VERSION,
                "peer_id": int(peer_id),
                "cmid": int(cmid),
                "reaction_id": int(REACTION_OK),
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
            code = safe_int(err.get("error_code"), -1)
            msg = str(err.get("error_msg"))
            if code == 14:
                self.captcha_ban(context="sendReaction")
            logging.warning(f"⚠️ {self.name}: sendReaction error {code} {msg} (peer={peer_id} cmid={cmid})")
            return False

        logging.info(f"🙂 {self.name}: реакция 🎉 поставлена (peer={peer_id} cmid={cmid})")
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

            return await self._messages_send(data)

        with self._io_lock:
            try:
                ret = self._vk.call(_send())
            except Exception as e:
                return False, None, -2, f"exception: {e}"

        if "error" in ret:
            err = ret["error"]
            code = safe_int(err.get("error_code"), -3)
            msg = str(err.get("error_msg"))

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
                self.enabled = False
                self.mark_dirty()
                logging.error(f"⛔ {self.name}: disabled (invalid access_token)")

            return [], code, msg

        items = ret.get("response", {}).get("items", [])
        return items, None, None


# =========================
# Ability Executor
# =========================
class AbilityExecutor:
    # Универсальный парс голосов по классам:
    VOICES_ANY_RE = re.compile(
        r"голос\s+у\s+(апостола|проклинающего|паладина)\s*:\s*(\d+)",
        re.IGNORECASE
    )

    # Апостольский профиль:
    # "👤Класс: апостол (29), гном-гоблин"
    PROFILE_RE = re.compile(
        r"класс\s*:\s*(апостол)\s*\((\d+)\)\s*,\s*([а-яё]+)\s*-\s*([а-яё]+)",
        re.IGNORECASE
    )

    def execute_one(
        self,
        worker: TokenHandler,
        task: BuffTask,
    ) -> Tuple[bool, str]:
        """
        Возвращает:
          ok=True  -> задача закрыта (SUCCESS или ALREADY)
          ok=False -> задача не закрыта, надо отложить/передать дальше
        """
        ab = task.ability

        # локальный кд проверяем ДО
        can, rem = worker.can_use_ability(ab.key)
        if not can:
            return False, f"COOLDOWN_LOCAL({int(rem)}s)"

        # фильтр по расе для апостола на расовые бафы
        if worker.class_type == "apostle" and ab.key in RACE_KEYS:
            if ab.key not in worker.races:
                return False, "RACE_NOT_ALLOWED"

        # фильтр по голосам
        if ab.uses_voices and worker.voices <= 0:
            return False, "NO_VOICES_LOCAL"

        # baseline для poll в target
        baseline_id = self._last_msg_id(worker, task.target_peer_id)

        # 1) forward триггер в target
        ok, fwd_msg_id, ecode, emsg = worker.send_text(
            peer_id=task.target_peer_id,
            text=" ",
            forward_peer_id=task.source_peer_id,
            forward_conversation_message_id=task.source_cmid,
        )
        if not ok or not fwd_msg_id:
            if ecode == 14:
                return False, "CAPTCHA"
            if ecode == 9:
                return False, "FLOOD"
            if ecode == 5:
                return False, "INVALID"
            return False, f"FORWARD_ERROR({ecode} {emsg})"

        # 2) send ability reply_to forwarded message (в target)
        ok2, _mid2, e2, m2 = worker.send_text(
            peer_id=task.target_peer_id,
            text=ab.text,
            reply_to=fwd_msg_id,
        )
        if not ok2:
            if e2 == 14:
                return False, "CAPTCHA"
            if e2 == 9:
                return False, "FLOOD"
            if e2 == 5:
                return False, "INVALID"
            return False, f"SEND_ERROR({e2} {m2})"

        # 3) poll result (+ парсим голоса по пути)
        status, cd = self._poll_result(worker, task.target_peer_id, baseline_id)

        if status == "SUCCESS":
            ab.processed = True

            # локальный кд ставится ТОЛЬКО на успех, как ты просил
            worker.set_ability_cooldown(ab.key, ab.cooldown)

            # реакция ТОЛЬКО на успех
            worker.send_reaction_ok(task.source_peer_id, task.source_cmid)

            return True, "SUCCESS"

        if status == "ALREADY":
            ab.processed = True
            return True, "ALREADY"

        if status == "NO_VOICES":
            worker.update_voices(0)
            return False, "NO_VOICES"

        if status == "COOLDOWN":
            sec = int(max(10, cd or 30))
            # подстраиваем локальный кд под реальный (важно!)
            worker.set_ability_cooldown(ab.key, sec)
            return False, f"COOLDOWN({sec}s)"

        return False, status

    def refresh_profile_if_possible(self, worker: TokenHandler) -> Tuple[bool, str]:
        """
        Апостол с 0 голосов: отправляем "Мой профиль" в target и парсим:
          - голоса (скобки)
          - расы (гном-гоблин -> м,г)
        """
        if worker.class_type != "apostle":
            return False, "NOT_APOSTLE"
        if worker.voices > 0:
            return False, "VOICES_OK"
        if worker.target_peer_id == 0:
            return False, "NO_TARGET"
        if now_ts() < worker.next_profile_refresh_ts:
            return False, "TOO_EARLY"

        baseline_id = self._last_msg_id(worker, worker.target_peer_id)

        ok, _mid, ecode, emsg = worker.send_text(peer_id=worker.target_peer_id, text="Мой профиль")
        worker.next_profile_refresh_ts = now_ts() + PROFILE_REFRESH_EVERY
        worker.mark_dirty()

        if not ok:
            if ecode == 14:
                return False, "CAPTCHA"
            return False, f"SEND_PROFILE_ERROR({ecode} {emsg})"

        status, _cd = self._poll_profile(worker, worker.target_peer_id, baseline_id)
        return (status == "PROFILE_OK"), status

    def _last_msg_id(self, token: TokenHandler, peer_id: int) -> int:
        items, _c, _m = token.get_history(peer_id, count=1)
        if items:
            return safe_int(items[0].get("id"), 0)
        return 0

    def _poll_result(self, token: TokenHandler, peer_id: int, baseline_id: int) -> Tuple[str, int]:
        for i in range(1, POLL_TRIES + 1):
            time.sleep(POLL_SLEEP_SECONDS)

            items, _c, _m = token.get_history(peer_id, count=160)
            new_msgs = [m for m in items if safe_int(m.get("id"), 0) > baseline_id]

            # парс голосов по любому классу
            self._parse_voices_any(token, new_msgs)

            status, cd = self._parse_result(new_msgs)
            if status != "UNKNOWN":
                return status, cd

            logging.info(f"🕵️ [{token.name}] no result yet (poll {i}/{POLL_TRIES})")

        return "UNKNOWN", 0

    def _poll_profile(self, token: TokenHandler, peer_id: int, baseline_id: int) -> Tuple[str, int]:
        for i in range(1, POLL_TRIES + 1):
            time.sleep(POLL_SLEEP_SECONDS)

            items, _c, _m = token.get_history(peer_id, count=160)
            new_msgs = [m for m in items if safe_int(m.get("id"), 0) > baseline_id]

            # голоса тоже могут быть в ответе
            self._parse_voices_any(token, new_msgs)

            ok = self._parse_profile_apostle(token, new_msgs)
            if ok:
                return "PROFILE_OK", 0

            logging.info(f"🕵️ [{token.name}] profile not found yet (poll {i}/{POLL_TRIES})")

        return "PROFILE_UNKNOWN", 0

    def _parse_voices_any(self, token: TokenHandler, msgs: List[Dict[str, Any]]) -> None:
        for m in msgs:
            text = (m.get("text", "") or "").strip()
            if not text:
                continue
            mm = self.VOICES_ANY_RE.search(text)
            if mm:
                new_voices = safe_int(mm.group(2), token.voices)
                token.update_voices(new_voices)

    def _parse_profile_apostle(self, token: TokenHandler, msgs: List[Dict[str, Any]]) -> bool:
        for m in msgs:
            text = (m.get("text", "") or "").strip()
            if not text:
                continue
            mm = self.PROFILE_RE.search(text)
            if not mm:
                continue

            voices = safe_int(mm.group(2), token.voices)
            race1 = (mm.group(3) or "").lower()
            race2 = (mm.group(4) or "").lower()

            # пытаемся сопоставить русские слова -> ключи рас
            # (не идеально, но для твоих рас работает)
            rmap = {
                "человек": "ч",
                "гоблин": "г",
                "нежить": "н",
                "эльф": "э",
                "гном": "м",
                "демон": "д",
                "орк": "о",
            }
            new_races: List[str] = []
            if race1 in rmap:
                new_races.append(rmap[race1])
            if race2 in rmap:
                new_races.append(rmap[race2])
            new_races = [r for r in new_races if r in RACE_KEYS]
            new_races = list(dict.fromkeys(new_races))

            token.update_voices(voices)
            if new_races and new_races != token.races:
                token.races = new_races
                token.mark_dirty()
                logging.info(f"🧬 {token.name}: races updated via profile -> {token.races}")

            return True
        return False

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

            # системный КД
            if (
                ("слишком часто" in text)
                or ("подождите" in text)
                or ("доступно через" in text)
                or ("попробуйте позже" in text)
                or ("социальные эффекты" in text and "определенное время" in text)
                or ("оставшееся время" in text)
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
# Main Bot (Observer + Dispatcher + Queue)
# =========================
class MultiTokenBot:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self._lock = threading.Lock()

        self.vk = VKAsyncClient()
        self.rate_limiter = SimpleRateLimiter(max_per_minute=SEND_MAX_PER_MINUTE)
        self.executor = AbilityExecutor()

        self.config: Dict[str, Any] = {}
        self.tokens: List[TokenHandler] = []
        self.tokens_by_id: Dict[str, TokenHandler] = {}
        self.sources_by_peer: Dict[int, TokenHandler] = {}

        self.observer_token_id: str = ""
        self.observer: Optional[TokenHandler] = None

        # polling
        self.poll_interval = 2.0
        self.poll_count = 30

        # last msg ids per source peer
        self.last_msg_ids: Dict[str, int] = {}

        # очередь задач
        self.queue_lock = threading.Lock()
        self.queue: List[BuffTask] = []  # хранится отсортированной по sort_ts

        self._running = False

        self.load()
        self.last_msg_ids = self._load_last_msg_ids()

        if not self.observer:
            raise RuntimeError("observer_token_id is not set or not found among tokens")

        # источники: берём из токенов (как ты и используешь сейчас)
        self.source_peer_ids: List[int] = sorted([t.source_peer_id for t in self.tokens if t.source_peer_id])

        logging.info("🤖 MultiTokenBot STARTED")
        logging.info(f"👁️ Observer: {self.observer.name} ({self.observer.id})")
        logging.info(f"📋 Tokens(total): {len(self.tokens)}")
        logging.info(f"📁 Source chats: {len(self.source_peer_ids)}")
        logging.info(f"🛰️ Scan interval: {self.poll_interval:.1f}s, scan_count={self.poll_count}")
        logging.info(f"🕵️ Target poll: tries={POLL_TRIES}, sleep={POLL_SLEEP_SECONDS:.0f}s")

    # -----------------
    # CONFIG
    # -----------------
    def load(self) -> None:
        with open(self.config_path, "r", encoding="utf-8") as f:
            self.config = json.load(f)

        settings = self.config.get("settings", {}) if isinstance(self.config, dict) else {}
        self.poll_interval = float(settings.get("poll_interval", 2.0))
        self.poll_count = int(settings.get("poll_count", 30))

        self.observer_token_id = str(self.config.get("observer_token_id", "") or "").strip()

        self.tokens = []
        self.tokens_by_id = {}
        self.sources_by_peer = {}

        for t_cfg in self.config.get("tokens", []):
            t = TokenHandler(t_cfg, self.vk, self.rate_limiter)
            self.tokens.append(t)
            self.tokens_by_id[t.id] = t
            if t.source_peer_id:
                self.sources_by_peer[t.source_peer_id] = t

        self.observer = self.tokens_by_id.get(self.observer_token_id)

    def save_config(self, force: bool = False) -> None:
        any_dirty = force or any(t.dirty for t in self.tokens)
        if not any_dirty:
            return

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

                # captcha pause
                orig["captcha_until"] = int(t.captcha_until) if t.captcha_until else 0

                # voice recovery fields
                orig["voice_recover_attempts"] = int(t.voice_recover_attempts)
                orig["next_virtual_voice_ts"] = int(t.next_virtual_voice_ts) if t.next_virtual_voice_ts else 0
                orig["needs_manual_voices"] = bool(t.needs_manual_voices)

                # profile refresh
                orig["next_profile_refresh_ts"] = int(t.next_profile_refresh_ts) if t.next_profile_refresh_ts else 0

                tokens_payload.append(orig)
                t.dirty = False

            self.config["tokens"] = tokens_payload

            # сохраняем observer_token_id
            self.config["observer_token_id"] = self.observer_token_id

            with open(self.config_path, "w", encoding="utf-8") as f:
                json.dump(self.config, f, ensure_ascii=False, indent=2)

    # -----------------
    # LAST MSG IDS
    # -----------------
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

    # -----------------
    # COMMAND PARSER
    # -----------------
    def parse_buff_command(self, text: str) -> List[ParsedAbility]:
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
        default_cd = int(c.get("default_cooldown", 61) or 61)
        text = f"{prefix} {v}".strip() if prefix else str(v)
        return text, default_cd, uses_voices

    # /голоса N (в source-чате токена)
    def parse_manual_voices(self, text: str) -> Optional[int]:
        text = (text or "").strip().lower()
        # принимаем: "/голоса 10" или "!голоса 10"
        m = re.match(r"^[!/](голоса)\s+(\d+)\s*$", text)
        if not m:
            return None
        return safe_int(m.group(2), -1)

    # -----------------
    # QUEUE
    # -----------------
    def _queue_push(self, task: BuffTask) -> None:
        with self.queue_lock:
            self.queue.append(task)
            self.queue.sort(key=lambda t: t.sort_ts)

    def _queue_peek(self) -> Optional[BuffTask]:
        with self.queue_lock:
            if not self.queue:
                return None
            return self.queue[0]

    def _queue_pop_ready(self) -> Optional[BuffTask]:
        with self.queue_lock:
            if not self.queue:
                return None
            if self.queue[0].sort_ts <= now_ts():
                return self.queue.pop(0)
            return None

    # -----------------
    # FILTERS / CANDIDATES
    # -----------------
    def _candidates_for_task(self, task: BuffTask, exclude_ids: Optional[set] = None) -> List[TokenHandler]:
        ab = task.ability
        exclude_ids = exclude_ids or set()

        out: List[TokenHandler] = []
        for t in self.tokens:
            if t.id == self.observer_token_id:
                continue
            if t.id in exclude_ids:
                continue
            if not t.is_available():
                continue

            # должен иметь target
            if t.target_peer_id != task.target_peer_id:
                continue

            # класс должен поддерживать букву
            class_data = CLASS_ABILITIES.get(t.class_type)
            if not class_data or ab.key not in class_data["abilities"]:
                continue

            # голоса > 0 если нужно
            if ab.uses_voices and t.voices <= 0:
                continue

            # расовый фильтр для апостола
            if t.class_type == "apostle" and ab.key in RACE_KEYS:
                if ab.key not in t.races:
                    continue

            # локальный кд
            can, _rem = t.can_use_ability(ab.key)
            if not can:
                continue

            out.append(t)

        # финальная сортировка: рандом, но предпочтём больше голосов
        out.sort(key=lambda x: (x.voices, random.random()), reverse=True)
        return out

    def _next_available_delay_for_task(self, task: BuffTask) -> float:
        """
        Если сейчас не нашлось кандидатов, пытаемся вычислить минимальную задержку,
        когда кто-то может стать доступным по локальному кд (или по капче/режиму).
        """
        ab = task.ability
        best = None

        for t in self.tokens:
            if t.id == self.observer_token_id:
                continue
            if not t.enabled:
                continue
            if t.needs_manual_voices:
                continue
            if t.target_peer_id != task.target_peer_id:
                continue
            class_data = CLASS_ABILITIES.get(t.class_type)
            if not class_data or ab.key not in class_data["abilities"]:
                continue
            if ab.uses_voices and t.voices <= 0:
                continue
            if t.class_type == "apostle" and ab.key in RACE_KEYS and ab.key not in t.races:
                continue

            rem = t.get_ability_cd_rem(ab.key)
            # учтём капчу тоже
            cap_rem = max(0.0, t.captcha_until - now_ts())
            rem = max(rem, cap_rem)

            if best is None or rem < best:
                best = rem

        if best is None:
            # никого вообще нет -> отложим на 60 сек, пусть что-то поменяется
            return 60.0

        # небольшой джиттер, чтобы не синхронно все просыпались
        return float(best) + random.uniform(0.5, 1.5)

    # -----------------
    # OBSERVER SCAN
    # -----------------
    def _scan_sources_with_observer(self) -> bool:
        """
        Observer сканирует все source-чаты. Он:
          - находит !баф
          - создаёт задачи на буквы
          - обрабатывает /голоса N (ручное восстановление)
        """
        updated_any = False
        obs = self.observer
        assert obs is not None

        for source_peer_id in self.source_peer_ids:
            items, code, _msg = obs.get_history(source_peer_id, count=self.poll_count)
            if code is not None and code not in (9,):
                continue

            last_id = int(self.last_msg_ids.get(str(source_peer_id), 0) or 0)

            for m in reversed(items):
                mid = safe_int(m.get("id"), 0)
                if mid <= last_id:
                    continue
                last_id = mid
                updated_any = True

                text = (m.get("text", "") or "").strip()
                sender_id = safe_int(m.get("from_id"), 0)
                cmid = safe_int(m.get("conversation_message_id"), 0)

                if sender_id <= 0 or cmid <= 0:
                    continue

                # 1) ручная команда /голоса N в source-чате данного токена
                manual_n = self.parse_manual_voices(text)
                if manual_n is not None and manual_n >= 0:
                    target_token = self.sources_by_peer.get(source_peer_id)
                    if not target_token:
                        # если не нашли, просто сообщим
                        obs.send_text(peer_id=source_peer_id, text="⚠️ Не нашёл токен для этого чата.")
                        continue

                    target_token.set_manual_voices(manual_n)
                    self.save_config(force=False)

                    # подтверждение (через Observer)
                    obs.send_text(peer_id=source_peer_id, text=f"✅ Голоса для {target_token.name}: {manual_n}")
                    continue

                # 2) баф команда
                abilities = self.parse_buff_command(text)
                if not abilities:
                    continue

                # создаём задачи: по одной на каждую букву
                keys = "".join(a.key for a in abilities)
                logging.info(f"🎯 !баф from {sender_id}: {keys} ({len(abilities)} abilities) [source={source_peer_id}]")

                # target_peer_id берём из "чьего" это source_peer_id токена (у всех одинаковый target)
                owner_token = self.sources_by_peer.get(source_peer_id)
                if not owner_token:
                    logging.error(f"❌ source_peer_id={source_peer_id}: no owner token mapping")
                    continue
                target_peer_id = owner_token.target_peer_id

                for idx, ab in enumerate(abilities, start=1):
                    tid = f"{sender_id}:{keys}:{mid}:{idx}"
                    task = BuffTask(
                        sort_ts=now_ts(),  # сразу готова
                        task_id=tid,
                        sender_id=sender_id,
                        source_peer_id=source_peer_id,
                        source_cmid=cmid,
                        target_peer_id=target_peer_id,
                        ability=ab,
                    )
                    self._queue_push(task)

            self.last_msg_ids[str(source_peer_id)] = last_id

        return updated_any

    # -----------------
    # DISPATCH LOOP
    # -----------------
    def _dispatch_ready_tasks(self) -> None:
        """
        Очень важная часть:
          - берём готовые задачи из очереди
          - фильтруем токены (класс/кд/голоса/расы)
          - выбираем "сколько букв" = столько активных воркеров
          - если воркеров меньше -> оставшиеся задачи откладываем до ближайшего кд и повторяем позже
        """
        # Возьмём "пачку" задач, которые готовы сейчас, но относятcя к одному и тому же триггеру (source_peer+cmid)
        # Чтобы при !баф уч не задействовать 4 токена, а только 2 — делаем группировку.
        ready: List[BuffTask] = []
        first = self._queue_peek()
        if not first:
            return
        if first.sort_ts > now_ts():
            return

        # соберём группу по source_peer_id+source_cmid+sender_id (один триггер)
        key = (first.source_peer_id, first.source_cmid, first.sender_id, first.target_peer_id)

        with self.queue_lock:
            i = 0
            while i < len(self.queue):
                t = self.queue[i]
                k2 = (t.source_peer_id, t.source_cmid, t.sender_id, t.target_peer_id)
                if t.sort_ts <= now_ts() and k2 == key:
                    ready.append(t)
                    self.queue.pop(i)
                    continue
                i += 1

        if not ready:
            return

        # Сколько букв? = столько активных токенов нам нужно (макс 4)
        needed = len(ready)

        # Для каждой буквы попробуем подобрать токен. Если букв 2 — не трогаем лишние токены.
        chosen: Dict[str, TokenHandler] = {}  # task_id -> token
        used_token_ids: set = set()

        # 1) первая волна: пытаемся назначить по одному токену на задачу
        for task in ready:
            candidates = self._candidates_for_task(task, exclude_ids=used_token_ids)
            if not candidates:
                continue
            token = random.choice(candidates[: min(3, len(candidates))])  # небольшая рандомизация среди топов
            chosen[task.task_id] = token
            used_token_ids.add(token.id)

            if len(used_token_ids) >= needed:
                break

        # 2) исполняем назначенные
        not_done: List[BuffTask] = []
        for task in ready:
            worker = chosen.get(task.task_id)
            if not worker:
                not_done.append(task)
                continue

            ok, status = self.executor.execute_one(worker, task)

            if ok:
                # лог успеха/алреди
                if status == "SUCCESS":
                    logging.info(f"✅ {worker.name}({worker.class_name()}): {task.ability.text}")
                else:
                    logging.info(f"ℹ️ {worker.name}({worker.class_name()}): {task.ability.text} {status}")
                continue

            # не выполнено — откладываем (очень важно: планировщик/очередь)
            delay = self._next_available_delay_for_task(task)

            # captcha -> пауза токена и задача подождёт
            if status == "CAPTCHA":
                worker.captcha_ban(CAPTCHA_BAN_SECONDS, context="send/forward")
                delay = max(delay, CAPTCHA_BAN_SECONDS)

            task.bump(delay, status)
            logging.warning(f"⚠️ Task delay: {worker.name} {task.ability.text} -> {status}, retry in ~{int(delay)}s")
            not_done.append(task)

        # 3) то, что не назначили (не хватило токенов) — тоже в очередь по кд
        for task in not_done:
            if task.ability.processed:
                continue
            delay = self._next_available_delay_for_task(task)
            task.bump(delay, task.last_error or "NO_WORKERS_AVAILABLE")
            self._queue_push(task)

    # -----------------
    # MAINTENANCE (voices/profile)
    # -----------------
    def _maintenance(self) -> None:
        """
        - Апостол с 0 голосов: раз в 2 часа шлём "Мой профиль" в target и парсим.
        - Не-апостолы с 0 голосов: раз в 6 часов делаем "виртуальный голос" (voices=1),
          но только чтобы токен вернулся в пул. Реальные голоса узнаем потом по системке.
          После 4 таких попыток -> needs_manual_voices = True (НЕ выключаем токен).
        """
        for t in self.tokens:
            if t.id == self.observer_token_id:
                continue

            if not t.enabled:
                continue

            # если токен в капче-паузе — подождём
            if now_ts() < t.captcha_until:
                continue

            # если просит ручных голосов — ничего не делаем
            if t.needs_manual_voices:
                continue

            # апостол: профиль
            if t.class_type == "apostle" and t.voices == 0:
                ok, status = self.executor.refresh_profile_if_possible(t)
                if ok:
                    logging.info(f"🧾 {t.name}: profile refresh OK (voices={t.voices}, races={t.races})")
                elif status not in ("TOO_EARLY", "VOICES_OK", "NOT_APOSTLE"):
                    logging.info(f"🧾 {t.name}: profile refresh -> {status}")

                continue

            # не-апостол (или паладины/проклинающие): виртуальные голоса, если 0
            if t.class_type != "apostle" and t.voices == 0:
                if t.next_virtual_voice_ts and now_ts() < t.next_virtual_voice_ts:
                    continue

                # если уже 4 попытки — помечаем и выходим
                if t.voice_recover_attempts >= VIRTUAL_VOICE_ATTEMPTS_MAX:
                    t.needs_manual_voices = True
                    t.mark_dirty()
                    logging.warning(f"🛑 {t.name}: needs_manual_voices=True (virtual attempts exhausted)")
                    continue

                # выдаём "виртуальный" голос, чтобы токен вернулся в пул
                t.voices = 1
                t.voice_recover_attempts += 1
                t.next_virtual_voice_ts = now_ts() + VIRTUAL_VOICE_EVERY
                t.mark_dirty()

                logging.info(
                    f"🔧 {t.name}: virtual voice granted (attempt {t.voice_recover_attempts}/{VIRTUAL_VOICE_ATTEMPTS_MAX}), "
                    f"next in {int(VIRTUAL_VOICE_EVERY/3600)}h"
                )

    # -----------------
    # RUN
    # -----------------
    def run(self):
        self._running = True
        try:
            while self._running:
                updated_any = self._scan_sources_with_observer()
                if updated_any:
                    self._save_last_msg_ids()

                # обслуживание голосов/профилей (без флуда)
                self._maintenance()

                # диспетчер: выполняем только то, что готово
                self._dispatch_ready_tasks()

                # сохраняем config при изменениях
                self.save_config(force=False)

                time.sleep(self.poll_interval)

        except KeyboardInterrupt:
            logging.info("⏹️ Stopping...")
        finally:
            self._running = False
            self.save_config(force=True)
            self.vk.close()


# =========================
# main
# =========================
if __name__ == "__main__":
    bot = MultiTokenBot("config.json")
    bot.run()
