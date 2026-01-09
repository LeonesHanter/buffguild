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
from typing import Dict, List, Optional, Tuple, Any


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("bot.log", encoding="utf-8"), logging.StreamHandler()],
)

VK_API_BASE = "https://api.vk.com/method"
VK_API_VERSION = "5.131"


# ====== Классы / способности ======
# apostle: "благословение X"
# warlock: "проклятие X"
# crusader/light_incarnation: без префикса, с индивидуальными кд
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
        "default_cooldown": 61,
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
            "в": ("воскрешение", 6 * 60 * 60),   # 6 часов
            "т": ("очищение огнем", 15 * 60 + 10),  # 15м10с
        },
    },
    "light_incarnation": {
        "name": "Воплощение света",
        "prefix": "",
        "uses_voices": False,
        "default_cooldown": None,
        "abilities": {
            "и": ("очищение", 61),
            "с": ("очищение светом", 15 * 60 + 10),  # 15м10с
        },
    },
}


# ====== Адаптивный тайминг ======
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
            self._wait = max(self._min, min(self._max, new_wait))

    def get_stats(self) -> Optional[Dict[str, float]]:
        with self._lock:
            if not self._samples:
                return None
            avg = sum(self._samples) / len(self._samples)
            return {
                "current_wait": self._wait,
                "avg_response": avg,
                "min_response": min(self._samples),
                "max_response": max(self._samples),
                "samples": float(len(self._samples)),
            }


# ====== Кэш сообщений ======
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
            return messages

    def set(self, peer_id: int, messages: List[Dict[str, Any]]) -> None:
        with self._lock:
            self._cache[peer_id] = (time.time(), messages)

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()


# ====== Веса токенов: 1.0, после 1 провала 0.9, после 2 — 0.8 ... минимум 0.1 ======
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

    def choose_weighted_unique(self, tokens: List["TokenHandler"], count: int) -> List["TokenHandler"]:
        if not tokens:
            return []
        if count >= len(tokens):
            return tokens[:]

        pool = tokens[:]
        selected: List[TokenHandler] = []
        for _ in range(count):
            weights = [self.get_weight(t.id) for t in pool]
            chosen = random.choices(pool, weights=weights, k=1)[0]
            selected.append(chosen)
            pool.remove(chosen)
            if not pool:
                break
        return selected

    def get_stats(self) -> List[Dict[str, Any]]:
        with self._lock:
            out = []
            for token_id in set(list(self._weights.keys()) + list(self._fails.keys())):
                out.append(
                    {"token_id": token_id, "weight": self._weights.get(token_id, 1.0), "consecutive_failures": self._fails.get(token_id, 0)}
                )
            out.sort(key=lambda x: x["weight"], reverse=True)
            return out

    def reset_all(self) -> None:
        with self._lock:
            self._fails.clear()
            self._weights.clear()
        logging.info("♻️ weights reset")


# ====== VK Async клиент (одна сессия на процесс) ======
class VKAsyncClient:
    def __init__(self):
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._ready = threading.Event()
        self._session: Optional[aiohttp.ClientSession] = None
        self._thread.start()
        self._ready.wait(timeout=10)

    def _run_loop(self):
        asyncio.set_event_loop(self._loop)
        self._loop.create_task(self._init())
        self._ready.set()
        self._loop.run_forever()

    async def _init(self):
        timeout = aiohttp.ClientTimeout(total=12)
        connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300)
        self._session = aiohttp.ClientSession(timeout=timeout, connector=connector)

    def call(self, coro):
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return fut.result(timeout=20)

    async def post(self, method: str, data: Dict[str, Any]) -> Dict[str, Any]:
        if not self._session:
            raise RuntimeError("VK session not ready")
        url = f"{VK_API_BASE}/{method}"
        async with self._session.post(url, data=data) as resp:
            return await resp.json()

    def close(self):
        if not self._session:
            return
        async def _close():
            await self._session.close()
        self.call(_close())
        self._loop.call_soon_threadsafe(self._loop.stop)


# ====== Token ======
class TokenHandler:
    def __init__(self, cfg: Dict[str, Any], vk: VKAsyncClient, msg_cache: MessageCache):
        self.id: str = cfg["id"]
        self.name: str = cfg.get("name", self.id)
        self.class_type: str = cfg.get("class", "apostle")
        self.access_token: str = cfg["access_token"]
        self.user_id: int = cfg.get("user_id", 0)

        self.source_chat_id: int = int(cfg["source_chat_id"])
        self.target_peer_id: int = int(cfg["target_peer_id"])
        self.source_peer_id: int = 2000000000 + self.source_chat_id

        self.voices: int = int(cfg.get("voices", 5))
        self.enabled: bool = bool(cfg.get("enabled", True))
        self.last_check: int = int(cfg.get("last_check", 0))

        self._ability_cd: Dict[str, float] = {}  # key -> timestamp
        self._vk = vk
        self._cache = msg_cache

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

    # ---- VK API ----
    async def _messages_send(self, text: str, reply_to: int) -> bool:
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
            logging.error(f"❌ {self.name}: send error {ret['error']}")
            return False
        return True

    def send_command_reply(self, text: str, reply_to_message_id: int) -> bool:
        return self._vk.call(self._messages_send(text, reply_to_message_id))

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
        # getHistory оставим через requests? — но нам нужен speed: делаем aiohttp тоже
        async def _get():
            ret = await self._vk.post("messages.getHistory", data)
            if "response" in ret and "items" in ret["response"]:
                items = ret["response"]["items"]
                self._cache.set(peer_id, items)
                return items
            return []

        try:
            return self._vk.call(_get())
        except Exception as e:
            logging.error(f"❌ {self.name}: getHistory error {e}")
            return []


# ====== Token manager ======
class SimpleTokenManager:
    def __init__(self, config_path: str, vk: VKAsyncClient):
        self.config_path = config_path
        self._lock = threading.Lock()
        self._vk = vk
        self.msg_cache = MessageCache(ttl=8)
        self.weight = TokenWeightManager()
        self.tokens: List[TokenHandler] = []
        self.load()

    def load(self) -> None:
        with open(self.config_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)

        with self._lock:
            self.tokens = [TokenHandler(t, self._vk, self.msg_cache) for t in cfg.get("tokens", [])]
        logging.info(f"📋 tokens loaded: {len(self.tokens)}")

    def reload(self) -> None:
        self.msg_cache.clear()
        self.load()

    def save(self) -> None:
        with self._lock:
            tokens_payload = []
            for t in self.tokens:
                tokens_payload.append(
                    {
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
                    }
                )

        payload = {"tokens": tokens_payload, "settings": {"delay": 2}}
        with open(self.config_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    def get_all_tokens_info(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [
                {
                    "id": t.id,
                    "name": t.name,
                    "class": t.class_type,
                    "enabled": t.enabled,
                    "voices": t.voices,
                    "user_id": t.user_id,
                    "source_chat_id": t.source_chat_id,
                    "target_peer_id": t.target_peer_id,
                }
                for t in self.tokens
            ]

    def record_token_result(self, token_id: str, success: bool, failure_type: str = "other") -> None:
        if success:
            self.weight.record_success(token_id)
        else:
            self.weight.record_failure(token_id, failure_type)

    def get_weight_stats(self) -> List[Dict[str, Any]]:
        return self.weight.get_stats()

    def tokens_for_ability(self, ability_key: str) -> List[TokenHandler]:
        with self._lock:
            out = []
            for t in self.tokens:
                if not t.enabled:
                    continue
                class_data = CLASS_ABILITIES.get(t.class_type)
                if not class_data:
                    continue
                if ability_key not in class_data["abilities"]:
                    continue
                ok, _rem = t.can_use_ability(ability_key)
                if not ok:
                    continue
                out.append(t)
            return out


# ====== Команда /баф parsing ======
@dataclass
class ParsedAbility:
    key: str
    text: str
    cooldown: int
    class_type: str
    uses_voices: bool


def build_ability_text_and_cd(class_type: str, key: str) -> Optional[Tuple[str, int, bool]]:
    c = CLASS_ABILITIES.get(class_type)
    if not c:
        return None
    abilities = c["abilities"]
    if key not in abilities:
        return None

    uses_voices = bool(c.get("uses_voices", False))
    v = abilities[key]
    if isinstance(v, tuple):
        name, cd = v
        return str(name), int(cd), uses_voices

    prefix = c.get("prefix", "")
    default_cd = int(c.get("default_cooldown", 61) or 61)
    text = f"{prefix} {v}".strip() if prefix else str(v)
    return text, default_cd, uses_voices


def parse_command_to_abilities(text: str) -> Optional[List[ParsedAbility]]:
    m = re.match(r"^/баф\s+([^\s]+)$", text.strip().lower())
    if not m:
        return None
    letters = list(m.group(1))

    # Каждую букву мапим на ПЕРВЫЙ класс, где она есть (как в твоей логике)
    out: List[ParsedAbility] = []
    for ch in letters:
        found = False
        for class_type in CLASS_ABILITIES.keys():
            r = build_ability_text_and_cd(class_type, ch)
            if not r:
                continue
            ability_text, cd, uses_voices = r
            out.append(ParsedAbility(key=ch, text=ability_text, cooldown=cd, class_type=class_type, uses_voices=uses_voices))
            found = True
            break
        if not found:
            logging.warning(f"⚠️ unknown ability key: {ch}")
    return out if out else None


# ====== MultiTokenBot ======
class MultiTokenBot:
    def __init__(self, config_path: str):
        self.vk = VKAsyncClient()
        self.tm = SimpleTokenManager(config_path, self.vk)

        if not self.tm.tokens:
            raise RuntimeError("No tokens in config.json")

        # основной токен для чтения исходного чата
        self.main_token = self.tm.tokens[0]
        self.source_peer_id = self.main_token.source_peer_id

        self.delay_time = 2
        self.timing = AdaptiveTiming()

        self.last_processed_source_id = 0

        # паттерны ответов NPC/системы
        self.confirm_pat = re.compile(r"наложено\s+(благословение|проклятие)|на\s+вас\s+наложено", re.IGNORECASE)
        self.no_voices_pat = re.compile(r"требуется\s+Голос", re.IGNORECASE)
        self.already_pat = re.compile(r"уже действует", re.IGNORECASE)
        self.cooldown_pat = re.compile(r"только через определенное время|оставшееся время", re.IGNORECASE)
        self.voices_pat = re.compile(r"Голос у Апостола:\s*(\d+)", re.IGNORECASE)

    # ---- реакции (не обязательно, но оставляем) ----
    def send_reaction(self, peer_id: int, cmid: Optional[int], reaction_id: int) -> bool:
        if not cmid:
            return False
        data = {
            "access_token": self.main_token.access_token,
            "v": VK_API_VERSION,
            "peer_id": int(peer_id),
            "cmid": int(cmid),
            "reaction_id": int(reaction_id),
        }
        async def _call():
            return await self.vk.post("messages.sendReaction", data)

        try:
            ret = self.vk.call(_call())
            return "error" not in ret
        except Exception as e:
            logging.error(f"sendReaction error: {e}")
            return False

    # ---- парсинг ответа конкретному токену ----
    def _parse_recent_result(self, token: TokenHandler) -> str:
        msgs = token.get_history(token.target_peer_id, count=8)
        for m in reversed(msgs):
            text = (m.get("text") or "").strip()
            from_id = m.get("from_id", 0)
            if from_id > 0:
                continue  # пользовательские пропускаем

            if self.confirm_pat.search(text):
                # если есть инфа про голоса — обновляем
                vm = self.voices_pat.search(text)
                if vm:
                    try:
                        token.voices = int(vm.group(1))
                    except Exception:
                        pass
                return "success"

            if self.no_voices_pat.search(text):
                return "no_voices"
            if self.already_pat.search(text):
                return "already_has"
            if self.cooldown_pat.search(text):
                return "cooldown"

        return "unknown"

    def _send_one_thread(self, token: TokenHandler, ability: ParsedAbility, reply_to_mid: int, idx: int, results: Dict[int, str]) -> None:
        try:
            start = time.time()
            ok = token.send_command_reply(ability.text, reply_to_mid)
            if not ok:
                results[idx] = "send_error"
                self.tm.record_token_result(token.id, False, "send_error")
                return

            time.sleep(self.timing.get_wait_time())

            res = self._parse_recent_result(token)
            elapsed = time.time() - start
            self.timing.record_response_time(elapsed)

            results[idx] = res

            if res == "success":
                self.tm.record_token_result(token.id, True)
                token.set_ability_cooldown(ability.key, ability.cooldown)
                logging.info(f"✅ [{idx+1}] {token.name}({token.class_name()}): {ability.text} ({elapsed:.2f}s)")
            elif res == "no_voices":
                self.tm.record_token_result(token.id, False, "no_voices")
                logging.warning(f"🔇 [{idx+1}] {token.name}({token.class_name()}): {ability.text} NO_VOICES ({elapsed:.2f}s)")
            else:
                self.tm.record_token_result(token.id, False, res)
                logging.warning(f"⚠️ [{idx+1}] {token.name}({token.class_name()}): {ability.text} => {res} ({elapsed:.2f}s)")
        except Exception as e:
            results[idx] = "exception"
            self.tm.record_token_result(token.id, False, "exception")
            logging.error(f"thread error idx={idx}: {e}")

    def _process_command(self, from_id: int, message_id: int, cmid: Optional[int], abilities: List[ParsedAbility]) -> None:
        remaining = abilities[:]
        success = 0
        max_iter = 5
        it = 0

        while remaining and it < max_iter:
            it += 1
            logging.info(f"🔄 iter {it}: remaining={len(remaining)}")

            # для каждой способности подбираем токен (учёт кд способности + веса)
            pairs: List[Tuple[TokenHandler, ParsedAbility]] = []
            for ab in remaining:
                candidates = self.tm.tokens_for_ability(ab.key)
                if not candidates:
                    continue
                chosen = self.tm.weight.choose_weighted_unique(candidates, 1)
                if chosen:
                    pairs.append((chosen[0], ab))

            if not pairs:
                break

            threads = []
            results: Dict[int, str] = {}
            for i, (tok, ab) in enumerate(pairs):
                t = threading.Thread(target=self._send_one_thread, args=(tok, ab, message_id, i, results), daemon=True)
                threads.append(t)
                t.start()

            for t in threads:
                t.join()

            new_remaining: List[ParsedAbility] = []
            for i, (tok, ab) in enumerate(pairs):
                r = results.get(i, "unknown")
                if r == "success":
                    success += 1
                else:
                    new_remaining.append(ab)

            remaining = new_remaining
            if remaining:
                time.sleep(0.4)

        if success == len(abilities):
            self.send_reaction(self.source_peer_id, cmid, 16)
        elif success > 0:
            self.send_reaction(self.source_peer_id, cmid, 16)
        else:
            self.send_reaction(self.source_peer_id, cmid, 7)

    def _handle_source_message(self, m: Dict[str, Any]) -> None:
        text = (m.get("text") or "").strip()
        if not text:
            return

        if not text.lower().startswith("/баф"):
            return

        abilities = parse_command_to_abilities(text)
        if not abilities:
            return

        from_id = int(m.get("from_id", 0))
        if from_id < 0:
            return  # сообщения от сообществ пропускаем

        mid = int(m["id"])
        cmid = m.get("conversation_message_id")

        logging.info(f"🎯 /баф from id{from_id}: {''.join([a.key for a in abilities])} ({len(abilities)})")

        threading.Thread(target=self._process_command, args=(from_id, mid, cmid, abilities), daemon=True).start()

    def _init_last_message_id(self) -> None:
        msgs = self.main_token.get_history(self.source_peer_id, count=1)
        if msgs:
            self.last_processed_source_id = int(msgs[0]["id"])
            logging.info(f"📌 start from message_id={self.last_processed_source_id}")

    def run(self) -> None:
        self._init_last_message_id()
        logging.info("🤖 MultiTokenBot started")

        while True:
            try:
                msgs = self.main_token.get_history(self.source_peer_id, count=20)
                for m in reversed(msgs):
                    mid = int(m["id"])
                    if mid <= self.last_processed_source_id:
                        continue
                    self.last_processed_source_id = mid
                    self._handle_source_message(m)

                time.sleep(self.delay_time)
            except Exception as e:
                logging.error(f"main loop error: {e}")
                time.sleep(3)


def main():
    config_path = "config.json"
    if not os.path.exists(config_path):
        logging.error("config.json not found")
        return
    bot = MultiTokenBot(config_path)
    bot.run()


if __name__ == "__main__":
    main()
