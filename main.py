# -*- coding: utf-8 -*-
import asyncio
import aiohttp
import json
import logging
import random
import re
import threading
import time
from concurrent.futures import TimeoutError as FuturesTimeoutError
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
from logging.handlers import RotatingFileHandler

# === ЛОГИРОВАНИЕ С РОТАЦИЕЙ ===
logger = logging.getLogger()
logger.setLevel(logging.INFO)

log_formatter = logging.Formatter(
    fmt="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

file_handler = RotatingFileHandler(
    "bot.log",
    maxBytes=5 * 1024 * 1024,  # 5 MB
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

# ====== КЛАССЫ И СПОСОБНОСТИ ======
# 1.1 FIX: apostol -> apostle
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
            "р": "расы",
            "с": "судьбы",
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

RACE_NAMES = {
    "ч": "человек",
    "г": "гоблин",
    "н": "нежить",
    "э": "эльф",
    "м": "гном",
    "д": "демон",
    "о": "орк",
}

# ====== АДАПТИВНЫЙ ТАЙМИНГ ======
class AdaptiveTiming:
    def __init__(self, initial_wait: float = 3.0, min_wait: float = 1.0, max_wait: float = 5.0):
        self._lock = threading.Lock()
        self._samples: List[float] = []
        self._wait = initial_wait
        self._min = min_wait
        self._max = max_wait

    def get_wait_time(self) -> float:
        with self._lock:
            return self._wait

    def record_response_time(self, elapsed: float) -> None:
        with self._lock:
            self._samples.append(float(elapsed))
            if len(self._samples) > 50:
                self._samples.pop(0)
            if len(self._samples) < 10:
                return
            s = sorted(self._samples)
            idx = int(len(s) * 0.95)
            idx = min(max(idx, 0), len(s) - 1)
            p95 = s[idx]
            new_wait = p95 * 1.1
            old_wait = self._wait
            self._wait = max(self._min, min(self._max, new_wait))
            if abs(old_wait - self._wait) > 0.1:
                logging.info(f"⏱️ Timing updated: {old_wait:.2f}s → {self._wait:.2f}s")

# ====== КЭШ СООБЩЕНИЙ ======
class MessageCache:
    def __init__(self, ttl: int = 8):
        self.ttl = ttl
        self._lock = threading.Lock()
        self._cache: Dict[int, Tuple[float, List[Dict[str, Any]]]] = {}

    def get(self, peer_id: int) -> Optional[List[Dict[str, Any]]]:
        now = time.time()
        with self._lock:
            item = self._cache.get(peer_id)
            if not item:
                return None
            ts, messages = item
            if now - ts > self.ttl:
                return None
            return messages[:]

    def set(self, peer_id: int, messages: List[Dict[str, Any]]) -> None:
        with self._lock:
            self._cache[peer_id] = (time.time(), messages)

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()

# ====== СИСТЕМА ВЕСОВ ======
class TokenWeightManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._fails: Dict[str, int] = {}
        self._weights: Dict[str, float] = {}

    def _calc_weight(self, fails: int) -> float:
        return max(0.1, 1.0 - 0.1 * fails)

    def get_weight(self, token_id: str) -> float:
        with self._lock:
            f = self._fails.get(token_id, 0)
            w = self._weights.get(token_id)
            if w is None:
                w = self._calc_weight(f)
                self._weights[token_id] = w
            return w

    def record_failure(self, token_id: str, failure_type: str = "no_voices") -> None:
        with self._lock:
            self._fails[token_id] = self._fails.get(token_id, 0) + 1
            f = self._fails[token_id]
            old = self._weights.get(token_id, 1.0)
            new = self._calc_weight(f)
            self._weights[token_id] = new
        logging.info(f"📉 {token_id}: fail#{f} ({failure_type}) weight {old:.1f}→{new:.1f}")

    def record_success(self, token_id: str) -> None:
        with self._lock:
            old_f = self._fails.get(token_id, 0)
            old_w = self._weights.get(token_id, 1.0)
            self._fails[token_id] = 0
            self._weights[token_id] = min(1.0, old_w + 0.2)
            new_w = self._weights[token_id]
        if old_f > 0 or old_w < 1.0:
            logging.info(f"📈 {token_id}: success weight {old_w:.1f}→{new_w:.1f}, fails reset {old_f}")

# ====== ПРОСТОЙ RATE LIMITER ДЛЯ TOKENS ======
class SimpleRateLimiter:
    def __init__(self, max_per_minute: int = 60):
        self.max_per_minute = max_per_minute
        self._lock = threading.Lock()
        self._counters: Dict[str, Tuple[int, float]] = {}  # token_id -> (count, window_start_ts)

    def allow(self, token_id: str) -> bool:
        now = time.time()
        with self._lock:
            count, start = self._counters.get(token_id, (0, now))
            if now - start >= 60:
                self._counters[token_id] = (1, now)
                return True
            if count < self.max_per_minute:
                self._counters[token_id] = (count + 1, start)
                return True
            return False

# ====== VK ASYNC CLIENT ======
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
            return fut.result(timeout=20)
        except FuturesTimeoutError:  # 1.4 FIX
            fut.cancel()
            raise
        except Exception:
            # на всякий — чтобы не оставлять dangling future
            try:
                fut.cancel()
            except Exception:
                pass
            raise

    async def post(self, method: str, data: Dict[str, Any]) -> Dict[str, Any]:
        if not self._session:
            raise RuntimeError("VK session not ready")
        url = f"{VK_API_BASE}/{method}"
        async with self._session.post(url, data=data) as resp:
            return await resp.json()

    # 4.2 FIX: корректное закрытие aiohttp + остановка loop
    def close(self) -> None:
        async def _close():
            if self._session and not self._session.closed:
                await self._session.close()

        try:
            asyncio.run_coroutine_threadsafe(_close(), self._loop).result(timeout=10)
        except Exception as e:
            logging.warning(f"⚠️ VK client close warning: {e}")

        try:
            self._loop.call_soon_threadsafe(self._loop.stop)
        except Exception:
            pass

# ====== DATA ======
@dataclass
class ParsedAbility:
    key: str
    text: str
    cooldown: int
    class_type: str
    uses_voices: bool
    processed: bool = False  # успешно выдан

# ====== TOKEN HANDLER ======
class TokenHandler:
    def __init__(self, cfg: Dict[str, Any], vk: VKAsyncClient, msg_cache: MessageCache,
                 manager: "SimpleTokenManager", rate_limiter: SimpleRateLimiter):
        self.id: str = cfg["id"]
        self.name: str = cfg.get("name", self.id)
        self.class_type: str = cfg.get("class", "apostle")  # 1.1 FIX
        self.access_token: str = cfg["access_token"]
        self.user_id: int = cfg.get("user_id", 0)
        self.source_chat_id: int = int(cfg["source_chat_id"])
        self.target_peer_id: int = int(cfg["target_peer_id"])
        self.source_peer_id: int = 2000000000 + self.source_chat_id
        self.voices: int = int(cfg.get("voices", 5))
        self.enabled: bool = bool(cfg.get("enabled", True))
        self.last_check: int = int(cfg.get("last_check", 0))
        self._ability_cd: Dict[str, float] = {}
        self._vk = vk
        self._cache = msg_cache
        self._manager = manager
        self._rate_limiter = rate_limiter

        # 4.1: лок, чтобы один токен не пытался бафать параллельно две буквы
        self._use_lock = threading.Lock()

    def class_name(self) -> str:
        return CLASS_ABILITIES.get(self.class_type, {}).get("name", "Неизвестный")

    def can_use_ability(self, ability_key: str) -> Tuple[bool, float]:
        ts = self._ability_cd.get(ability_key, 0.0)
        rem = ts - time.time()
        if rem > 0:
            return False, rem
        return True, 0.0

    def set_ability_cooldown(self, ability_key: str, cooldown_seconds: int) -> None:
        self._ability_cd[ability_key] = time.time() + int(cooldown_seconds)

    def update_voices(self, new_voices: int) -> None:
        if self.voices != new_voices:
            old = self.voices
            self.voices = new_voices
            self._manager.save()
            logging.info(f"🔊 {self.name}: voices {old} → {new_voices}")

    # 2.1: возвращаем id отправленного сообщения, чтобы проверить ответ
    async def _messages_send(self, text: str, reply_to: int) -> Optional[int]:
        if not self._rate_limiter.allow(self.id):
            logging.warning(f"🚦 Rate limit for token {self.id}, skipping send")
            return None

        data = {
            "access_token": self.access_token,
            "v": VK_API_VERSION,
            "peer_id": self.target_peer_id,
            "message": text,
            "random_id": random.randrange(1, 2_000_000_000),
            "disable_mentions": 1,
            "reply_to": int(reply_to),
        }
        ret = await self._vk.post("messages.send", data)
        if "error" in ret:
            err = ret["error"]
            code = err.get("error_code")
            msg = err.get("error_msg")
            logging.error(f"❌ {self.name}: send error {code} {msg}")
            return None

        # VK обычно возвращает int в "response"
        sent_id = ret.get("response")
        if isinstance(sent_id, int):
            return sent_id
        return None

    def send_command_reply(self, text: str, reply_to_message_id: int) -> Optional[int]:
        try:
            return self._vk.call(self._messages_send(text, reply_to_message_id))
        except Exception as e:
            logging.error(f"❌ {self.name}: send_command_reply exception {e}")
            return None

    def get_history(self, peer_id: int, count: int = 20) -> List[Dict[str, Any]]:
        cached = self._cache.get(peer_id)
        if cached is not None:
            return cached[:count]
        data = {
            "access_token": self.access_token,
            "v": VK_API_VERSION,
            "peer_id": int(peer_id),
            "count": int(count),
        }

        async def _get():
            try:
                ret = await self._vk.post("messages.getHistory", data)
                if "error" in ret:
                    err = ret["error"]
                    code = err.get("error_code")
                    msg = err.get("error_msg")
                    logging.error(f"❌ {self.name}: getHistory error {code} {msg}")
                    return []
                if "response" in ret and "items" in ret["response"]:
                    items = ret["response"]["items"]
                    self._cache.set(peer_id, items)
                    return items
            except Exception as e:
                logging.error(f"getHistory error: {e}")
            return []

        try:
            return self._vk.call(_get())
        except Exception as e:
            logging.error(f"❌ {self.name}: getHistory exception {e}")
            return []

    def get_history_fresh(self, peer_id: int, count: int = 20) -> List[Dict[str, Any]]:
        """История без кэша — для проверки результата бафа."""
        data = {
            "access_token": self.access_token,
            "v": VK_API_VERSION,
            "peer_id": int(peer_id),
            "count": int(count),
        }

        async def _get():
            try:
                ret = await self._vk.post("messages.getHistory", data)
                if "error" in ret:
                    err = ret["error"]
                    code = err.get("error_code")
                    msg = err.get("error_msg")
                    logging.error(f"❌ {self.name}: getHistory_fresh error {code} {msg}")
                    return []
                if "response" in ret and "items" in ret["response"]:
                    return ret["response"]["items"]
            except Exception as e:
                logging.error(f"getHistory_fresh error: {e}")
            return []

        try:
            return self._vk.call(_get())
        except Exception as e:
            logging.error(f"❌ {self.name}: getHistory_fresh exception {e}")
            return []

# ====== TOKEN MANAGER ======
class SimpleTokenManager:
    def __init__(self, config_path: str, vk: VKAsyncClient):
        self.config_path = config_path
        self._lock = threading.Lock()
        self._vk = vk
        self.msg_cache = MessageCache(ttl=8)
        self.weight = TokenWeightManager()
        self.rate_limiter = SimpleRateLimiter(max_per_minute=60)
        self.tokens: List[TokenHandler] = []
        self.config: Dict = {}
        self.load()

    def load(self) -> None:
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                self.config = json.load(f)
        except FileNotFoundError:
            logging.warning(f"⚠️ {self.config_path} not found, creating empty")
            self.config = {"tokens": [], "settings": {"delay": 2}, "race_pools": {}}
            self.save()
        except json.JSONDecodeError as e:
            logging.error(f"❌ Invalid JSON in {self.config_path}: {e}")
            raise

        with self._lock:
            self.tokens = []
            for t_cfg in self.config.get("tokens", []):
                target = int(t_cfg.get("target_peer_id", 0))
                if 0 < target < 2000000000:
                    logging.warning(f"⚠️ Suspicious target_peer_id={target} for {t_cfg.get('id')}")
                sc = int(t_cfg.get("source_chat_id", 0))
                if sc > 2000000000:
                    logging.warning(f"⚠️ source_chat_id looks like peer_id={sc} for {t_cfg.get('id')}")
                self.tokens.append(
                    TokenHandler(t_cfg, self._vk, self.msg_cache, self, self.rate_limiter)
                )
        logging.info(f"📋 Loaded {len(self.tokens)} tokens")

    def reload(self) -> None:
        self.msg_cache.clear()
        self.load()

    def save(self) -> None:
        with self._lock:
            tokens_payload = []
            for t in self.tokens:
                tokens_payload.append({
                    "id": t.id,
                    "name": t.name,
                    "class": t.class_type,
                    "access_token": t.access_token,
                    "user_id": t.user_id,
                    "source_chat_id": t.source_chat_id,
                    "target_peer_id": t.target_peer_id,
                    "voices": t.voices,
                    "enabled": t.enabled,
                    "last_check": t.last_check,
                })
            self.config["tokens"] = tokens_payload
        try:
            with open(self.config_path, "w", encoding="utf-8") as f:
                json.dump(self.config, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.error(f"❌ Failed to save config: {e}")

    def tokens_for_ability(self, ability_key: str) -> List[TokenHandler]:
        tokens = []
        for token in self.tokens:
            if not token.enabled:
                continue
            class_data = CLASS_ABILITIES.get(token.class_type)
            if not class_data or ability_key not in class_data["abilities"]:
                continue
            ok, _ = token.can_use_ability(ability_key)
            if ok:
                tokens.append(token)
        return tokens

# ====== EXECUTOR ======
class AbilityExecutor:
    def __init__(self, timing: AdaptiveTiming, tm: SimpleTokenManager):
        self.timing = timing
        self.tm = tm

    def execute(self, token: TokenHandler, ability: ParsedAbility,
                trigger_msg_id: int, sender_id: int, attempt_idx: int) -> None:
        start_time = time.time()

        # 4.1: один токен — одна активная попытка за раз
        with token._use_lock:
            try:
                sent_msg_id = token.send_command_reply(ability.text, trigger_msg_id)
                if not sent_msg_id:
                    self.tm.weight.record_failure(token.id, "send_error_or_rate_limit")
                    logging.warning(
                        f"❌ [{attempt_idx}] {token.name}({token.class_name()}): "
                        f"{ability.text} SEND_ERROR_OR_RATE_LIMIT ({time.time()-start_time:.2f}s)"
                    )
                    return

                # ждём ответ + несколько коротких попыток прочитать свежую историю
                time.sleep(self.timing.get_wait_time())

                result = "UNKNOWN"
                for _ in range(3):
                    history = token.get_history_fresh(token.target_peer_id, count=20)
                    result = self._parse_result(history, sent_msg_id, trigger_msg_id)
                    if result != "UNKNOWN":
                        break
                    time.sleep(0.6)

                if result == "SUCCESS":
                    ability.processed = True
                    token.set_ability_cooldown(ability.key, ability.cooldown)
                    if ability.uses_voices:
                        token.update_voices(max(0, token.voices - 1))
                    self.tm.weight.record_success(token.id)
                    self.timing.record_response_time(time.time() - start_time)
                    logging.info(
                        f"✅ [{attempt_idx}] {token.name}({token.class_name()}): "
                        f"{ability.text} ({time.time()-start_time:.2f}s)"
                    )
                elif result == "NO_VOICES":
                    token.update_voices(0)
                    self.tm.weight.record_failure(token.id, "no_voices")
                    logging.warning(
                        f"🔇 [{attempt_idx}] {token.name}({token.class_name()}): "
                        f"{ability.text} NO_VOICES ({time.time()-start_time:.2f}s)"
                    )
                else:
                    self.tm.weight.record_failure(token.id, result)
                    logging.warning(
                        f"⚠️ [{attempt_idx}] {token.name}({token.class_name()}): "
                        f"{ability.text} {result} ({time.time()-start_time:.2f}s)"
                    )
            except Exception as e:
                logging.error(f"❌ Attempt error idx={attempt_idx}: {e}")
                self.tm.weight.record_failure(token.id, "exception")

    # 2.1 FIX: привязка ответа к конкретной отправке (reply на sent_msg_id / trigger_msg_id)
    def _parse_result(self, history: List[Dict], sent_msg_id: int, trigger_msg_id: int) -> str:
        for msg in history:
            # VK может класть объект reply_message
            reply = msg.get("reply_message") or {}
            reply_id = reply.get("id")

            # интересуют только сообщения-ответы на нашу отправку (или как fallback на триггер)
            if reply_id not in (sent_msg_id, trigger_msg_id):
                continue

            text = (msg.get("text", "") or "").lower()

            if "на вас наложено" in text or "наложено" in text:
                return "SUCCESS"
            if "требуется голос" in text or "требуются голоса" in text or "недостаточно голос" in text:
                return "NO_VOICES"

        return "UNKNOWN"

# ====== БОТ ======
class MultiTokenBot:
    def __init__(self, config_path: str):
        self._vk_client = VKAsyncClient()
        self.tm = SimpleTokenManager(config_path, self._vk_client)
        self.timing = AdaptiveTiming()
        self.executor = AbilityExecutor(self.timing, self.tm)
        self.main_token = self.tm.tokens[0] if self.tm.tokens else None
        if not self.main_token:
            raise RuntimeError("No tokens in config.json")
        self.source_peer_id = self.main_token.source_peer_id
        self._running = False
        self.last_msg_id = self._load_last_msg_id()

        # 4.1: пул потоков для параллельной обработки букв (макс 4)
        self._pool = ThreadPoolExecutor(max_workers=4)

        logging.info("🤖 MultiTokenBot STARTED")
        logging.info(f"📋 Tokens: {len(self.tm.tokens)}")
        logging.info(f"📁 Source chat: {self.source_peer_id}")
        logging.info(f"⏱️ Initial wait time: {self.timing.get_wait_time():.2f}s")

    # ===== вспомогательные =====
    def _load_last_msg_id(self) -> int:
        try:
            with open("last_msg_id.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                return int(data.get("last_msg_id", 0))
        except Exception:
            return 0

    def _save_last_msg_id(self, last_msg_id: int) -> None:
        try:
            with open("last_msg_id.json", "w", encoding="utf-8") as f:
                json.dump({"last_msg_id": int(last_msg_id)}, f)
        except Exception as e:
            logging.error(f"❌ Failed to save last_msg_id: {e}")

    # ===== командный парсер (!баф, максимум 4 буквы) =====
    # 1.2 FIX: после !баф берём только 4 буквы, пробелы игнорируем: "!баф зуча" => "зуча"
    def parse_command_text(self, text: str, sender_id: int, trigger_msg_id: int) -> List[ParsedAbility]:
        text = (text or "").strip().lower()
        if not text.startswith("!баф"):
            return []

        rest = text[4:]  # 1.2 FIX: правильно отрезаем "!баф"
        rest = rest.strip()
        if not rest:
            return []

        # игнорируем пробелы/табуляции между буквами
        rest = re.sub(r"\s+", "", rest)

        # строго максимум 4 буквы
        cmd_text = rest[:4]
        if not cmd_text:
            return []

        abilities: List[ParsedAbility] = []
        for ch in cmd_text:
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

    def _send_reaction(self, trigger_msg_id: int, text: str, sender_id: int) -> None:
        if self.main_token:
            self.main_token.send_command_reply(text, trigger_msg_id)

    # ===== основной цикл =====
    def run(self):
        self._running = True
        try:
            while self._running:
                msgs = self.main_token.get_history(self.source_peer_id, count=30)
                updated = False

                for msg in reversed(msgs):
                    msg_id = msg.get("id", 0)
                    if msg_id <= self.last_msg_id:
                        continue
                    self.last_msg_id = msg_id
                    updated = True

                    text = (msg.get("text", "") or "").strip()
                    sender_id = msg.get("from_id", 0)
                    if sender_id <= 0:
                        continue

                    abilities = self.parse_command_text(text, sender_id, msg_id)
                    if abilities:
                        logging.info(
                            f"🎯 !баф from {sender_id}: "
                            f"{''.join(a.key for a in abilities)} ({len(abilities)} abilities)"
                        )
                        self._process_abilities(abilities, sender_id, msg_id)

                if updated:
                    self._save_last_msg_id(self.last_msg_id)

                time.sleep(1.0)
        except KeyboardInterrupt:
            logging.info("⏹️ Stopping...")
        finally:
            self.stop()

    # 4.2 FIX: корректная остановка
    def stop(self) -> None:
        if not self._running:
            return
        self._running = False

        try:
            self._pool.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass

        try:
            self._vk_client.close()
        except Exception as e:
            logging.warning(f"⚠️ stop() warning: {e}")

        logging.info("🧹 Bot stopped cleanly")

    # ===== обработка способностей =====
    # 4.1: параллельная обработка букв (до 4)
    def _process_abilities(self, abilities: List[ParsedAbility], sender_id: int, trigger_msg_id: int) -> None:
        futures = []
        for ability in abilities:
            futures.append(self._pool.submit(self._process_single_ability, ability, sender_id, trigger_msg_id))

        for _ in as_completed(futures):
            pass

        success_count = len([a for a in abilities if a.processed])
        fail_count = len(abilities) - success_count

        if success_count == len(abilities):
            logging.info(f"🎉 All {len(abilities)} abilities used for {sender_id}")
        elif success_count > 0:
            logging.warning(f"⚠️ Partial success: {success_count}/{len(abilities)} for {sender_id}")
        else:
            logging.error(f"❌ Failed all {len(abilities)} for {sender_id}")

        self._log_success_stats(sender_id, abilities)

    def _process_single_ability(self, ability: ParsedAbility, sender_id: int, trigger_msg_id: int) -> None:
        candidates = self.tm.tokens_for_ability(ability.key)
        if not candidates:
            logging.warning(f"⚠️ No tokens for ability '{ability.key}' (class {ability.class_type})")
            return

        candidates.sort(key=lambda t: self.tm.weight.get_weight(t.id), reverse=True)

        for idx, token in enumerate(candidates, start=1):
            try:
                can, _ = token.can_use_ability(ability.key)
                if not can:
                    continue
                self.executor.execute(token, ability, trigger_msg_id, sender_id, idx)
                if ability.processed:
                    break  # строго 1 баф на букву
            except Exception as e:
                logging.error(
                    f"❌ Error while processing ability '{ability.key}' "
                    f"with token {token.id}: {e}"
                )

    # ===== статистика успешных бафов =====
    def _log_success_stats(self, sender_id: int, abilities: List[ParsedAbility]) -> None:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        keys = "".join(a.key for a in abilities)
        success_flags = []
        success_count = 0
        fail_count = 0
        for a in abilities:
            if a.processed:
                success_flags.append(f"{a.key}:ok")
                success_count += 1
            else:
                success_flags.append(f"{a.key}:fail")
                fail_count += 1
        line = (
            f"{ts};sender={sender_id};abilities={keys};"
            f"success={success_count};fail={fail_count};"
            f"details={','.join(success_flags)}\n"
        )
        try:
            with open("buff_stats.log", "a", encoding="utf-8") as f:
                f.write(line)
        except Exception as e:
            logging.error(f"❌ Failed to write buff_stats.log: {e}")

# ===== main =====
if __name__ == "__main__":
    bot = MultiTokenBot("config.json")
    try:
        bot.run()
    except KeyboardInterrupt:
        bot.stop()
