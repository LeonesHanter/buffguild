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
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("bot.log", encoding="utf-8"), logging.StreamHandler()],
)

VK_API_BASE = "https://api.vk.com/method"
VK_API_VERSION = "5.131"

# ====== КЛАССЫ И СПОСОБНОСТИ (ОБНОВЛЕНО) ======
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
        "default_cooldown": 3600,  # ← 1 ЧАС
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
    "о": "орк"
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

    def choose_weighted_unique(self, tokens: List["TokenHandler"], count: int) -> List["TokenHandler"]:
        if not tokens:
            return []
        if count >= len(tokens):
            return tokens[:]
        pool = tokens[:]
        selected: List[TokenHandler] = []
        for _ in range(count):
            if not pool:
                break
            weights = [self.get_weight(t.id) for t in pool]
            chosen = random.choices(pool, weights=weights, k=1)[0]
            selected.append(chosen)
            pool.remove(chosen)
        return selected

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
        except TimeoutError:
            fut.cancel()
            raise

    async def post(self, method: str, data: Dict[str, Any]) -> Dict[str, Any]:
        if not self._session:
            raise RuntimeError("VK session not ready")
        url = f"{VK_API_BASE}/{method}"
        async with self._session.post(url, data=data) as resp:
            return await resp.json()

# ====== TOKEN HANDLER ======
@dataclass
class ParsedAbility:
    key: str
    text: str
    cooldown: int
    class_type: str
    uses_voices: bool

class TokenHandler:
    def __init__(self, cfg: Dict[str, Any], vk: VKAsyncClient, msg_cache: MessageCache, manager: "SimpleTokenManager"):
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
        self._ability_cd: Dict[str, float] = {}
        self._vk = vk
        self._cache = msg_cache
        self._manager = manager

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
        async def _get():
            try:
                ret = await self._vk.post("messages.getHistory", data)
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

# ====== TOKEN MANAGER ======
class SimpleTokenManager:
    def __init__(self, config_path: str, vk: VKAsyncClient):
        self.config_path = config_path
        self._lock = threading.Lock()
        self._vk = vk
        self.msg_cache = MessageCache(ttl=8)
        self.weight = TokenWeightManager()
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
                self.tokens.append(TokenHandler(t_cfg, self._vk, self.msg_cache, self))
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
        # Основной пул
        for token in self.tokens:
            if not token.enabled:
                continue
            class_data = CLASS_ABILITIES.get(token.class_type)
            if not class_data or ability_key not in class_data["abilities"]:
                continue
            ok, _ = token.can_use_ability(ability_key)
            if ok:
                tokens.append(token)
        
        # ДОПОЛНИТЕЛЬНЫЙ ПУЛ для рас (только Апостолы)
        race_pools = self.config.get("race_pools", {})
        if ability_key in RACE_NAMES:
            pool = race_pools.get(ability_key, {})
            expires = pool.get("expires", 0)
            if time.time() < expires:
                for token_id in pool.get("enabled_apostles", []):
                    for token in self.tokens:
                        if (token.id == token_id and 
                            token.class_type == "apostle" and 
                            token.enabled):
                            ok, _ = token.can_use_ability(ability_key)
                            if ok and token not in tokens:  # Избегаем дублей
                                tokens.append(token)
        return tokens

# ====== ОСНОВНОЙ БОТ ======
class MultiTokenBot:
    MAX_PARALLEL = 4
    MAX_ITERATIONS = 5

    def __init__(self, config_path: str):
        self.tm = SimpleTokenManager(config_path, VKAsyncClient())
        self.timing = AdaptiveTiming()
        self.main_token = self.tm.tokens[0] if self.tm.tokens else None
        if not self.main_token:
            raise RuntimeError("No tokens in config.json")
        self.source_peer_id = self.main_token.source_peer_id
        self._running = False
        logging.info("🤖 MultiTokenBot STARTED")
        logging.info(f"📋 Tokens: {len(self.tm.tokens)}")
        logging.info(f"📁 Source chat: {self.source_peer_id}")
        logging.info(f"⏱️ Initial wait time: {self.timing.get_wait_time():.2f}s")

    def parse_command_text(self, text: str, sender_id: int, trigger_msg_id: int) -> List[ParsedAbility]:
        text = text.strip().lower()
        
        # Новые команды
        if self._handle_special_commands(text, sender_id, trigger_msg_id):
            return []
        
        # Обычный парсинг /баф
        if not text.startswith('/баф'):
            return []
        
        abilities = []
        cmd_text = text[4:].strip()
        
        for char in cmd_text:
            for class_type in CLASS_ORDER:
                ability_info = self._build_ability_text_and_cd(class_type, char)
                if ability_info:
                    text, cooldown, uses_voices = ability_info
                    abilities.append(ParsedAbility(char, text, cooldown, class_type, uses_voices))
                    break
        return abilities

    def _handle_special_commands(self, text: str, sender_id: int, trigger_msg_id: int) -> bool:
        if text.startswith('/апо'):
            self._cmd_apo(trigger_msg_id, sender_id)
            return True
        if '/мои голос' in text:
            self._cmd_restore_voices(text, trigger_msg_id, sender_id)
            return True
        if text.startswith('/допраса'):
            self._cmd_doprasa(text, trigger_msg_id, sender_id)
            return True
        return False

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

    def _cmd_apo(self, trigger_msg_id: int, sender_id: int) -> None:
        """📋 Команда /апо"""
        apostles_info = []
        race_pools = self.tm.config.get("race_pools", {})
        
        for token in self.tm.tokens:
            if token.class_type != "apostle":
                continue
            
            voices_str = f"🔊 **{token.voices} голосов**"
            status = "✅" if token.enabled else "❌"
            
            races_list = []
            for race_key, pool in race_pools.items():
                if token.id in pool.get("enabled_apostles", []):
                    expires = pool.get("expires", 0)
                    remaining = max(0, expires - time.time())
                    if remaining > 0:
                        hours = int(remaining // 3600)
                        mins = int((remaining % 3600) // 60)
                        race_name = RACE_NAMES.get(race_key, race_key)
                        time_str = f"({hours}ч{mins}м)" if hours > 0 else f"({mins}м)"
                        races_list.append(f"{race_name} {time_str}")
            
            races_str = "🏺 **" + ", ".join(races_list) + "**" if races_list else "🏺 **-**"
            apostles_info.append(
                f"**{token.name}**  {status}\n"
                f"{voices_str}\n"
                f"{races_str}"
            )
        
        status_text = "📋 **Апостолы:**\n\n" + "\n\n".join([f"**{i+1}.** {info}" for i, info in enumerate(apostles_info)])
        self._send_reaction(trigger_msg_id, status_text[:4000], sender_id)

    def _cmd_restore_voices(self, text: str, trigger_msg_id: int, sender_id: int) -> None:
        """🔊 Команда /мои голоса N"""
        match = re.match(r'/мои\s+голос[аы]\s+(\d+)', text, re.IGNORECASE)
        if not match:
            self._send_reaction(trigger_msg_id, "❌ Формат: /мои голоса 5", sender_id)
            return
        
        voices_count = int(match.group(1))
        sender_token = None
        for token in self.tm.tokens:
            if token.user_id == sender_id and token.class_type == "apostle":
                sender_token = token
                break
        
        if not sender_token:
            self._send_reaction(trigger_msg_id, "❌ Апостол не найден", sender_id)
            return
        
        old_voices = sender_token.voices
        sender_token.update_voices(voices_count)
        self.tm.weight.record_success(sender_token.id)
        self._send_reaction(trigger_msg_id, f"✅ {sender_token.name}: голоса {old_voices}→{voices_count}, weight=1.0", sender_id)

    def _cmd_doprasa(self, text: str, trigger_msg_id: int, sender_id: int) -> None:
        """🌟 Команда /допраса д"""
        match = re.match(r'/допраса\s+([чгнэмдо])', text.lower())
        if not match:
            self._send_reaction(trigger_msg_id, "❌ Формат: /допраса д", sender_id)
            return
        
        race_key = match.group(1)
        expires = time.time() + 2 * 3600  # 2 часа
        
        apostle_token = None
        for token in self.tm.tokens:
            if (token.class_type == "apostle" and 
                token.enabled and 
                token.user_id == sender_id and 
                token.voices > 0):
                apostle_token = token
                break
        
        if not apostle_token:
            self._send_reaction(trigger_msg_id, "❌ Нет доступных апостолов с голосами", sender_id)
            return
        
        race_pools = self.tm.config.get("race_pools", {})
        race_pools[race_key] = race_pools.get(race_key, {})
        if "enabled_apostles" not in race_pools[race_key]:
            race_pools[race_key]["enabled_apostles"] = []
        if apostle_token.id not in race_pools[race_key]["enabled_apostles"]:
            race_pools[race_key]["enabled_apostles"].append(apostle_token.id)
        race_pools[race_key]["expires"] = expires
        
        self.tm.config["race_pools"] = race_pools
        self.tm.save()
        
        race_name = RACE_NAMES.get(race_key, race_key)
        self._send_reaction(trigger_msg_id, f"✅ {apostle_token.name} в пуле {race_name} (2ч)", sender_id)

    def _send_reaction(self, trigger_msg_id: int, text: str, sender_id: int) -> None:
        """Отправить реакцию на сообщение отправителя"""
        if self.main_token:
            self.main_token.send_command_reply(text, trigger_msg_id)

    def run(self):
        self._running = True
        try:
            last_msg_id = 0
            while self._running:
                msgs = self.main_token.get_history(self.source_peer_id, count=30)
                for msg in reversed(msgs):
                    msg_id = msg.get("id", 0)
                    if msg_id <= last_msg_id:
                        continue
                    last_msg_id = max(last_msg_id, msg_id)
                    
                    text = msg.get("text", "").strip()
                    sender_id = msg.get("from_id", 0)
                    
                    if sender_id <= 0:
                        continue
                    
                    abilities = self.parse_command_text(text, sender_id, msg_id)
                    if abilities:
                        logging.info(f"🎯 /баф from {sender_id}: {''.join([a.key for a in abilities])} ({len(abilities)} abilities)")
                        self._process_abilities(abilities, sender_id, msg_id)
        except KeyboardInterrupt:
            logging.info("⏹️ Stopping...")
        finally:
            self._running = False

    def _process_abilities(self, abilities: List[ParsedAbility], sender_id: int, trigger_msg_id: int) -> None:
        remaining_abilities = abilities[:]
        for iteration in range(self.MAX_ITERATIONS):
            if not remaining_abilities:
                break
                
            logging.info(f"🔄 Iteration {iteration+1}/{self.MAX_ITERATIONS}: remaining={len(remaining_abilities)}")
            
            # Группируем по классам
            ability_groups: Dict[str, List[ParsedAbility]] = {}
            for ability in remaining_abilities:
                if ability.class_type not in ability_groups:
                    ability_groups[ability.class_type] = []
                ability_groups[ability.class_type].append(ability)
            
            threads = []
            for class_type, class_abilities in ability_groups.items():
                available_tokens = self.tm.tokens_for_ability(class_abilities[0].key)
                selected_tokens = self.tm.weight.choose_weighted_unique(available_tokens, min(len(class_abilities), self.MAX_PARALLEL))
                
                for i, (token, ability) in enumerate(zip(selected_tokens, class_abilities)):
                    if not token.can_use_ability(ability.key)[0]:
                        continue
                    
                    thread = threading.Thread(target=self._execute_ability, 
                                            args=(token, ability, trigger_msg_id, sender_id, i+1))
                    threads.append(thread)
                    thread.start()
                    if len(threads) >= self.MAX_PARALLEL:
                        break
            
            # Ждём завершения
            for thread in threads:
                thread.join()
            
            wait_time = self.timing.get_wait_time()
            time.sleep(wait_time)
            
            # Фильтруем успешно выполненные
            remaining_abilities = [a for a in remaining_abilities if not a.processed]
        
        success_count = len(abilities) - len(remaining_abilities)
        if success_count == len(abilities):
            logging.info(f"🎉 All {len(abilities)} abilities used for {sender_id}")
        elif success_count > 0:
            logging.warning(f"⚠️ Partial success: {success_count}/{len(abilities)} for {sender_id}")
        else:
            logging.error(f"❌ Failed all abilities for {sender_id}")

    def _execute_ability(self, token: TokenHandler, ability: ParsedAbility, 
                        trigger_msg_id: int, sender_id: int, thread_idx: int) -> None:
        start_time = time.time()
        ability.processed = True
        
        try:
            success = token.send_command_reply(ability.text, trigger_msg_id)
            if not success:
                self.tm.record_token_result(token.id, False, "send_error")
                logging.warning(f"❌ [{thread_idx}] {token.name}({token.class_name()}): {ability.text} SEND_ERROR ({time.time()-start_time:.2f}s)")
                return
            
            time.sleep(self.timing.get_wait_time())
            
            # Проверяем результат
            history = token.get_history(token.target_peer_id, count=10)
            result = self._parse_result(history, ability.text)
            
            if result == "SUCCESS":
                token.set_ability_cooldown(ability.key, ability.cooldown)
                if ability.uses_voices:
                    token.update_voices(token.voices - 1)
                self.tm.record_token_result(token.id, True)
                self.timing.record_response_time(time.time() - start_time)
                logging.info(f"✅ [{thread_idx}] {token.name}({token.class_name()}): {ability.text} ({time.time()-start_time:.2f}s)")
            elif result == "NO_VOICES":
                token.update_voices(0)
                self.tm.record_token_result(token.id, False, "no_voices")
                logging.warning(f"🔇 [{thread_idx}] {token.name}({token.class_name()}): {ability.text} NO_VOICES ({time.time()-start_time:.2f}s)")
            else:
                self.tm.record_token_result(token.id, False, result)
                logging.warning(f"⚠️ [{thread_idx}] {token.name}({token.class_name()}): {ability.text} {result} ({time.time()-start_time:.2f}s)")
                
        except Exception as e:
            logging.error(f"❌ Thread error idx={thread_idx}: {e}")
            self.tm.record_token_result(token.id, False, "exception")

    def _parse_result(self, history: List[Dict], sent_text: str) -> str:
        for msg in history:
            text = msg.get("text", "").lower()
            if "на вас наложено" in text or "наложено" in text:
                return "SUCCESS"
            if "требуется голос" in text or "голоса" in text:
                return "NO_VOICES"
        return "UNKNOWN"

if __name__ == "__main__":
    bot = MultiTokenBot("config.json")
    try:
        bot.run()
    except KeyboardInterrupt:
        pass
