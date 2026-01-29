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


# ====== КЛАССЫ И СПОСОБНОСТИ ======
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


# ====== АДАПТИВНЫЙ ТАЙМИНГ ======
class AdaptiveTiming:
    """
    Автоматически подстраивает время ожидания ответа от VK API
    на основе реальных измерений (95-й перцентиль).
    """
    
    def __init__(self, initial_wait: float = 3.0, min_wait: float = 1.0, max_wait: float = 5.0):
        self._lock = threading.Lock()
        self._samples: List[float] = []
        self._wait = initial_wait
        self._min = min_wait
        self._max = max_wait

    def get_wait_time(self) -> float:
        """Получить текущее оптимальное время ожидания"""
        with self._lock:
            return self._wait

    def record_response_time(self, elapsed: float) -> None:
        """Записать время ответа для обучения"""
        with self._lock:
            self._samples.append(float(elapsed))
            if len(self._samples) > 50:
                self._samples.pop(0)
            if len(self._samples) < 10:
                return

            # 95-й перцентиль
            s = sorted(self._samples)
            idx = int(len(s) * 0.95)
            idx = min(max(idx, 0), len(s) - 1)
            p95 = s[idx]
            
            # +10% запаса
            new_wait = p95 * 1.1
            old_wait = self._wait
            self._wait = max(self._min, min(self._max, new_wait))
            
            if abs(old_wait - self._wait) > 0.1:
                logging.info(f"⏱️ Timing updated: {old_wait:.2f}s → {self._wait:.2f}s")

    def get_stats(self) -> Optional[Dict[str, float]]:
        """Получить статистику для мониторинга"""
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


# ====== КЭШ СООБЩЕНИЙ ======
class MessageCache:
    """
    Кэширует результаты messages.getHistory для снижения нагрузки на API.
    TTL по умолчанию 8 секунд.
    """
    
    def __init__(self, ttl: int = 8):
        self.ttl = ttl
        self._lock = threading.Lock()
        self._cache: Dict[int, Tuple[float, List[Dict[str, Any]]]] = {}

    def get(self, peer_id: int) -> Optional[List[Dict[str, Any]]]:
        """Получить из кэша (thread-safe копия)"""
        now = time.time()
        with self._lock:
            item = self._cache.get(peer_id)
            if not item:
                return None
            ts, messages = item
            if now - ts > self.ttl:
                return None
            return messages[:]  # ✅ Возвращаем копию

    def set(self, peer_id: int, messages: List[Dict[str, Any]]) -> None:
        """Сохранить в кэш"""
        with self._lock:
            self._cache[peer_id] = (time.time(), messages)

    def clear(self) -> None:
        """Очистить весь кэш"""
        with self._lock:
            self._cache.clear()
            logging.info("💾 Message cache cleared")


# ====== СИСТЕМА ВЕСОВ ТОКЕНОВ ======
class TokenWeightManager:
    """
    Управляет весами токенов:
    - Начальный вес: 1.0
    - После 1 провала: 0.9
    - После 2 провалов: 0.8
    - ...
    - Минимум: 0.1
    - При успехе: +0.2 (до макс 1.0)
    """
    
    def __init__(self):
        self._lock = threading.Lock()
        self._fails: Dict[str, int] = {}
        self._weights: Dict[str, float] = {}

    def _calc_weight(self, fails: int) -> float:
        """Расчёт веса по количеству провалов"""
        return max(0.1, 1.0 - 0.1 * fails)

    def get_weight(self, token_id: str) -> float:
        """Получить текущий вес токена"""
        with self._lock:
            f = self._fails.get(token_id, 0)
            w = self._weights.get(token_id)
            if w is None:
                w = self._calc_weight(f)
                self._weights[token_id] = w
            return w

    def record_failure(self, token_id: str, failure_type: str = "no_voices") -> None:
        """Записать провал (снижение веса)"""
        with self._lock:
            self._fails[token_id] = self._fails.get(token_id, 0) + 1
            f = self._fails[token_id]
            old = self._weights.get(token_id, 1.0)
            new = self._calc_weight(f)
            self._weights[token_id] = new
        logging.info(f"📉 {token_id}: fail#{f} ({failure_type}) weight {old:.1f}→{new:.1f}")

    def record_success(self, token_id: str) -> None:
        """Записать успех (повышение веса + сброс провалов)"""
        with self._lock:
            old_f = self._fails.get(token_id, 0)
            old_w = self._weights.get(token_id, 1.0)
            self._fails[token_id] = 0
            self._weights[token_id] = min(1.0, old_w + 0.2)
            new_w = self._weights[token_id]
        if old_f > 0 or old_w < 1.0:
            logging.info(f"📈 {token_id}: success weight {old_w:.1f}→{new_w:.1f}, fails reset {old_f}")

    def choose_weighted_unique(self, tokens: List["TokenHandler"], count: int) -> List["TokenHandler"]:
        """
        Выбрать N токенов с учётом весов (без повторений).
        Токены с большим весом имеют больший шанс быть выбранными.
        """
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

    def get_stats(self) -> List[Dict[str, Any]]:
        """Статистика для Telegram админки"""
        with self._lock:
            out = []
            for token_id in set(list(self._weights.keys()) + list(self._fails.keys())):
                out.append({
                    "token_id": token_id,
                    "weight": self._weights.get(token_id, 1.0),
                    "consecutive_failures": self._fails.get(token_id, 0),
                })
            out.sort(key=lambda x: x["weight"], reverse=True)
            return out

    def reset_all(self) -> None:
        """Сброс всех весов (для восстановления после сбоя)"""
        with self._lock:
            self._fails.clear()
            self._weights.clear()
        logging.info("♻️ All weights reset to 1.0")


# ====== VK ASYNC CLIENT ======
class VKAsyncClient:
    """
    Асинхронный клиент VK API с переиспользуемой сессией.
    
    ИСПРАВЛЕНО:
    - Race condition при инициализации
    - Утечка event loop при timeout
    - Переиспользование одной сессии для всех запросов
    """
    
    def __init__(self):
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._ready = threading.Event()
        self._session: Optional[aiohttp.ClientSession] = None
        self._thread.start()
        if not self._ready.wait(timeout=10):
            raise RuntimeError("VK client init timeout")

    def _run_loop(self):
        """Event loop в отдельном потоке"""
        asyncio.set_event_loop(self._loop)
        # ✅ ИСПРАВЛЕНИЕ: Ждём завершения _init ПЕРЕД set()
        self._loop.run_until_complete(self._init())
        self._ready.set()
        self._loop.run_forever()

    async def _init(self):
        """Инициализация aiohttp сессии"""
        timeout = aiohttp.ClientTimeout(total=12)
        connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300)
        self._session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        logging.info("🌐 VK async session created")

    def call(self, coro):
        """
        Выполнить корутину в event loop и получить результат.
        
        ИСПРАВЛЕНО: Отмена задачи при timeout
        """
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            return fut.result(timeout=20)
        except TimeoutError:
            fut.cancel()  # ✅ Отменяем задачу
            raise

    async def post(self, method: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """POST запрос к VK API"""
        if not self._session:
            raise RuntimeError("VK session not ready")
        url = f"{VK_API_BASE}/{method}"
        async with self._session.post(url, data=data) as resp:
            return await resp.json()

    def close(self):
        """Закрыть сессию и остановить event loop"""
        if not self._session:
            return
        async def _close():
            await self._session.close()
        try:
            self.call(_close())
        except:
            pass
        self._loop.call_soon_threadsafe(self._loop.stop)


# ====== TOKEN HANDLER ======
class TokenHandler:
    """
    Обработчик одного VK токена.
    
    ИСПРАВЛЕНО:
    - Автоматическое сохранение voices в config.json
    - Явная установка voices=0 при провале
    """
    
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
        self._manager = manager  # ✅ Обратная ссылка для автосохранения

    def class_name(self) -> str:
        """Получить название класса"""
        return CLASS_ABILITIES.get(self.class_type, {}).get("name", "Неизвестный")

    def can_use_ability(self, ability_key: str) -> Tuple[bool, float]:
        """Проверить доступность способности (учёт кулдауна)"""
        ts = self._ability_cd.get(ability_key, 0.0)
        rem = ts - time.time()
        if rem > 0:
            return False, rem
        return True, 0.0

    def set_ability_cooldown(self, ability_key: str, cooldown_seconds: int) -> None:
        """Установить кулдаун на способность"""
        self._ability_cd[ability_key] = time.time() + int(cooldown_seconds)
        logging.debug(f"⏳ {self.name}: {ability_key} on CD {cooldown_seconds}s")

    def update_voices(self, new_voices: int) -> None:
        """
        Обновить количество голосов с автосохранением.
        
        ИСПРАВЛЕНО: Автосохранение в config.json
        """
        if self.voices != new_voices:
            old = self.voices
            self.voices = new_voices
            self._manager.save()
            logging.info(f"🔊 {self.name}: voices {old} → {new_voices}")

    # ---- VK API методы ----
    async def _messages_send(self, text: str, reply_to: int) -> bool:
        """Отправка сообщения через reply_to"""
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
        """Синхронная обёртка для отправки команды"""
        return self._vk.call(self._messages_send(text, reply_to_message_id))

    def get_history(self, peer_id: int, count: int = 20) -> List[Dict[str, Any]]:
        """
        Получить историю сообщений с кэшированием.
        
        ИСПРАВЛЕНО: Обработка ошибок JSON/сети
        """
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
    """
    Менеджер всех токенов.
    
    ИСПРАВЛЕНО:
    - Автосохранение при изменении voices
    - Обработка битого JSON
    - Валидация target_peer_id
    """
    
    def __init__(self, config_path: str, vk: VKAsyncClient):
        self.config_path = config_path
        self._lock = threading.Lock()
        self._vk = vk
        self.msg_cache = MessageCache(ttl=8)
        self.weight = TokenWeightManager()
        self.tokens: List[TokenHandler] = []
        self.load()

    def load(self) -> None:
        """
        Загрузка токенов из config.json.
        
        ИСПРАВЛЕНО: Обработка битого JSON
        """
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except FileNotFoundError:
            logging.warning(f"⚠️ {self.config_path} not found, creating empty")
            cfg = {"tokens": [], "settings": {"delay": 2}}
            self.save()
        except json.JSONDecodeError as e:
            logging.error(f"❌ Invalid JSON in {self.config_path}: {e}")
            raise

        with self._lock:
            self.tokens = []
            for t_cfg in cfg.get("tokens", []):
                # ✅ Валидация target_peer_id
                target = int(t_cfg.get("target_peer_id", 0))
                if 0 < target < 2000000000:
                    logging.warning(f"⚠️ Suspicious target_peer_id={target} for {t_cfg.get('id')}")
                
                self.tokens.append(TokenHandler(t_cfg, self._vk, self.msg_cache, self))
        
        logging.info(f"📋 Loaded {len(self.tokens)} tokens")

    def reload(self) -> None:
        """Перезагрузка конфигурации"""
        self.msg_cache.clear()
        self.load()
        logging.info("🔄 Config reloaded")

    def save(self) -> None:
        """
        Сохранение токенов в config.json.
        
        ВЫЗЫВАЕТСЯ АВТОМАТИЧЕСКИ при изменении voices.
        """
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

        payload = {"tokens": tokens_payload, "settings": {"delay": 2}}
        
        try:
            with open(self.config_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.error(f"❌ Failed to save config: {e}")

    def get_all_tokens_info(self) -> List[Dict[str, Any]]:
        """Информация о всех токенах для Telegram админки"""
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
        """Записать результат работы токена (для весов)"""
        if success:
            self.weight.record_success(token_id)
        else:
            self.weight.record_failure(token_id, failure_type)

    def get_weight_stats(self) -> List[Dict[str, Any]]:
        """Статистика весов для мониторинга"""
        return self.weight.get_stats()

    def tokens_for_ability(self, ability_key: str) -> List[TokenHandler]:
        """
        Получить токены, у которых есть способность + не на кулдауне.
        
        ИСПРАВЛЕНО: Фиксированный порядок классов (CLASS_ORDER)
        """
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


# ====== ПАРСИНГ КОМАНД ======
@dataclass
class ParsedAbility:
    """Распарсенная способность из команды /баф"""
    key: str
    text: str
    cooldown: int
    class_type: str
    uses_voices: bool


def build_ability_text_and_cd(class_type: str, key: str) -> Optional[Tuple[str, int, bool]]:
    """
    Построить текст способности и её кулдаун по классу и ключу.
    
    Возвращает: (текст, кулдаун_сек, использует_голоса)
    """
    c = CLASS_ABILITIES.get(class_type)
    if not c:
        return None
    
    abilities = c["abilities"]
    if key not in abilities:
        return None

    uses_voices = bool(c.get("uses_voices", False))
    v = abilities[key]
    
    if isinstance(v, tuple):
        # Способность с индивидуальным КД (например "воскрешение", 21600)
        name, cd = v
        return str(name), int(cd), uses_voices

    # Обычная способность с префиксом
    prefix = c.get("prefix", "")
    default_cd = int(c.get("default_cooldown", 61) or 61)
    text = f"{prefix} {v}".strip() if prefix else str(v)
    return text, default_cd, uses_voices


def parse_command_to_abilities(text: str) -> Optional[List[ParsedAbility]]:
    """
    Парсинг команды /баф азу → список способностей.
    
    ИСПРАВЛЕНО: Фиксированный порядок классов (CLASS_ORDER)
    
    Примеры:
    /баф а    → [ParsedAbility(key='а', text='благословение атаки', ...)]
    /баф азу  → [атаки, защиты, удачи]
    /баф лб   → [проклятие неудачи, проклятие боли]
    /баф втс  → [воскрешение, очищение огнем, очищение светом]
    """
    m = re.match(r"^/баф\s+([^\s]+)$", text.strip().lower())
    if not m:
        return None
    
    letters = list(m.group(1))
    out: List[ParsedAbility] = []
    
    for ch in letters:
        found = False
        # ✅ Ищем по фиксированному порядку
        for class_type in CLASS_ORDER:
            r = build_ability_text_and_cd(class_type, ch)
            if not r:
                continue
            
            ability_text, cd, uses_voices = r
            out.append(ParsedAbility(
                key=ch,
                text=ability_text,
                cooldown=cd,
                class_type=class_type,
                uses_voices=uses_voices
            ))
            found = True
            break
        
        if not found:
            logging.warning(f"⚠️ Unknown ability key: {ch}")
    
    return out if out else None


# ====== MULTI TOKEN BOT ======
class MultiTokenBot:
    """
    Основной бот с поддержкой множественных токенов.
    
    ИСПРАВЛЕНО:
    - Ограничение 4 параллельных потока
    - Автоматическое обновление voices
    - Правильный парсинг ответов
    """
    
    def __init__(self, config_path: str):
        self.vk = VKAsyncClient()
        self.tm = SimpleTokenManager(config_path, self.vk)

        if not self.tm.tokens:
            raise RuntimeError("No tokens in config.json")

        # Основной токен для чтения исходного чата
        self.main_token = self.tm.tokens[0]
        self.source_peer_id = self.main_token.source_peer_id

        self.delay_time = 2
        self.timing = AdaptiveTiming()

        self.last_processed_source_id = 0

        # Паттерны ответов NPC/системы
        self.confirm_pat = re.compile(
            r"наложено\s+(благословение|проклятие)|на\s+вас\s+наложено",
            re.IGNORECASE
        )
        self.no_voices_pat = re.compile(r"требуется\s+Голос", re.IGNORECASE)
        self.already_pat = re.compile(r"уже действует", re.IGNORECASE)
        self.cooldown_pat = re.compile(
            r"только через определенное время|оставшееся время",
            re.IGNORECASE
        )
        self.voices_pat = re.compile(r"Голос у Апостола:\s*(\d+)", re.IGNORECASE)

    # ---- Реакции ----
    def send_reaction(self, peer_id: int, cmid: Optional[int], reaction_id: int) -> bool:
        """
        Установка реакции на сообщение.
        
        ИСПРАВЛЕНО: Валидация cmid, логирование ошибок
        """
        if not cmid or cmid <= 0:
            logging.debug(f"Skip reaction: cmid={cmid}")
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
            if "error" in ret:
                logging.warning(f"Reaction error: {ret['error']}")
                return False
            
            emoji = {7: "😢", 16: "🎉"}.get(reaction_id, f"#{reaction_id}")
            logging.debug(f"Reaction {emoji} set on cmid={cmid}")
            return True
        except Exception as e:
            logging.error(f"Reaction exception: {e}")
            return False

    # ---- Парсинг ответов ----
    def _parse_recent_result(self, token: TokenHandler) -> str:
        """
        Парсинг последних сообщений от NPC для определения результата.
        
        ИСПРАВЛЕНО: 
        - Автообновление voices при успехе
        - Установка voices=0 при провале
        
        Возвращает:
        - "success" - успешно наложено
        - "no_voices" - нет голосов
        - "already_has" - уже есть баф
        - "cooldown" - на кулдауне
        - "unknown" - неизвестно
        """
        msgs = token.get_history(token.target_peer_id, count=8)
        
        for m in reversed(msgs):
            text = (m.get("text") or "").strip()
            from_id = m.get("from_id", 0)
            
            if from_id > 0:
                continue  # пользовательские пропускаем

            # ✅ Успех
            if self.confirm_pat.search(text):
                vm = self.voices_pat.search(text)
                if vm:
                    try:
                        new_voices = int(vm.group(1))
                        token.update_voices(new_voices)  # ✅ Автосохранение
                    except Exception:
                        pass
                return "success"

            # ✅ Нет голосов
            if self.no_voices_pat.search(text):
                token.update_voices(0)  # ✅ Явно ставим 0
                return "no_voices"

            if self.already_pat.search(text):
                return "already_has"

            if self.cooldown_pat.search(text):
                return "cooldown"

        return "unknown"

    # ---- Отправка в потоке ----
    def _send_one_thread(
        self,
        token: TokenHandler,
        ability: ParsedAbility,
        reply_to_mid: int,
        idx: int,
        results: Dict[int, str]
    ) -> None:
        """
        Отправка одной способности в отдельном потоке.
        
        Измеряет время ответа для адаптивного тайминга.
        """
        try:
            start = time.time()
            
            # Отправка
            ok = token.send_command_reply(ability.text, reply_to_mid)
            if not ok:
                results[idx] = "send_error"
                self.tm.record_token_result(token.id, False, "send_error")
                return

            # Адаптивное ожидание
            time.sleep(self.timing.get_wait_time())

            # Парсинг результата
            res = self._parse_recent_result(token)
            elapsed = time.time() - start
            
            # Записываем время для обучения
            self.timing.record_response_time(elapsed)

            results[idx] = res

            # Обновление весов
            if res == "success":
                self.tm.record_token_result(token.id, True)
                token.set_ability_cooldown(ability.key, ability.cooldown)
                logging.info(
                    f"✅ [{idx+1}] {token.name}({token.class_name()}): "
                    f"{ability.text} ({elapsed:.2f}s)"
                )
            elif res == "no_voices":
                self.tm.record_token_result(token.id, False, "no_voices")
                logging.warning(
                    f"🔇 [{idx+1}] {token.name}({token.class_name()}): "
                    f"{ability.text} NO_VOICES ({elapsed:.2f}s)"
                )
            else:
                self.tm.record_token_result(token.id, False, res)
                logging.warning(
                    f"⚠️ [{idx+1}] {token.name}({token.class_name()}): "
                    f"{ability.text} => {res} ({elapsed:.2f}s)"
                )

        except Exception as e:
            results[idx] = "exception"
            self.tm.record_token_result(token.id, False, "exception")
            logging.error(f"Thread error idx={idx}: {e}")

    # ---- Обработка команды ----
    def _process_command(
        self,
        from_id: int,
        message_id: int,
        cmid: Optional[int],
        abilities: List[ParsedAbility]
    ) -> None:
        """
        Обработка команды /баф с fallback и весовым выбором токенов.
        
        ИСПРАВЛЕНО: Ограничение 4 параллельных потока
        
        Алгоритм:
        1. Для каждой способности подбираем токен (учёт класса, КД, веса)
        2. Отправляем до 4 способностей одновременно (батчинг)
        3. Парсим результаты
        4. Неудавшиеся способности переотправляем (до 5 итераций)
        5. Устанавливаем реакцию на исходное сообщение
        """
        remaining = abilities[:]
        success = 0
        max_iter = 5
        iteration = 0
        
        MAX_PARALLEL = 4  # ✅ Максимум 4 потока одновременно

        while remaining and iteration < max_iter:
            iteration += 1
            logging.info(f"🔄 Iteration {iteration}/{max_iter}: remaining={len(remaining)}")

            # Подбор токенов для каждой способности
            pairs: List[Tuple[TokenHandler, ParsedAbility]] = []
            
            for ab in remaining:
                candidates = self.tm.tokens_for_ability(ab.key)
                if not candidates:
                    logging.warning(
                        f"⚠️ No available tokens for '{ab.text}' "
                        f"(class: {ab.class_type})"
                    )
                    continue
                
                # Взвешенный выбор
                chosen = self.tm.weight.choose_weighted_unique(candidates, 1)
                if chosen:
                    pairs.append((chosen[0], ab))

            if not pairs:
                logging.error("❌ No tokens available for remaining abilities")
                break

            # ✅ Батчинг по 4 потока
            for batch_start in range(0, len(pairs), MAX_PARALLEL):
                batch = pairs[batch_start:batch_start + MAX_PARALLEL]
                
                threads = []
                results: Dict[int, str] = {}

                for i, (tok, ab) in enumerate(batch):
                    global_idx = batch_start + i
                    t = threading.Thread(
                        target=self._send_one_thread,
                        args=(tok, ab, message_id, global_idx, results),
                        daemon=True
                    )
                    threads.append(t)
                    t.start()

                # Ждём завершения батча
                for t in threads:
                    t.join()

            # Анализ результатов
            new_remaining: List[ParsedAbility] = []
            
            for i, (tok, ab) in enumerate(pairs):
                r = results.get(i, "unknown")
                if r == "success":
                    success += 1
                else:
                    new_remaining.append(ab)

            remaining = new_remaining

            # Пауза перед следующей итерацией
            if remaining and iteration < max_iter:
                time.sleep(0.4)

        # Финальная реакция
        total = len(abilities)
        
        if success == total:
            logging.info(f"🎉 All {total} abilities used for id{from_id}")
            self.send_reaction(self.source_peer_id, cmid, 16)  # 🎉
        elif success > 0:
            logging.info(f"⚠️ Partial success: {success}/{total} for id{from_id}")
            self.send_reaction(self.source_peer_id, cmid, 16)  # 🎉
        else:
            logging.error(f"❌ Failed all abilities for id{from_id}")
            self.send_reaction(self.source_peer_id, cmid, 7)  # 😢

    # ---- Обработка сообщений из исходного чата ----
    def _handle_source_message(self, m: Dict[str, Any]) -> None:
        """Обработка входящего сообщения из чата источника"""
        text = (m.get("text") or "").strip()
        if not text:
            return

        if not text.lower().startswith("/баф"):
            return

        abilities = parse_command_to_abilities(text)
        if not abilities:
            logging.warning(f"⚠️ Invalid command: {text}")
            return

        from_id = int(m.get("from_id", 0))
        if from_id < 0:
            return  # Сообщения от сообществ игнорируем

        mid = int(m["id"])
        cmid = m.get("conversation_message_id")

        keys = "".join([a.key for a in abilities])
        logging.info(f"🎯 /баф from id{from_id}: {keys} ({len(abilities)} abilities)")

        # Запускаем обработку в отдельном потоке
        threading.Thread(
            target=self._process_command,
            args=(from_id, mid, cmid, abilities),
            daemon=True
        ).start()

    # ---- Инициализация ----
    def _init_last_message_id(self) -> None:
        """Получить ID последнего сообщения для начала обработки"""
        msgs = self.main_token.get_history(self.source_peer_id, count=1)
        if msgs:
            self.last_processed_source_id = int(msgs[0]["id"])
            logging.info(f"📌 Starting from message_id={self.last_processed_source_id}")

    # ---- Основной цикл ----
    def run(self) -> None:
        """
        Основной цикл бота.
        
        Слушает исходный чат, обрабатывает команды /баф.
        """
        self._init_last_message_id()
        
        logging.info("=" * 60)
        logging.info("🤖 MultiTokenBot STARTED")
        logging.info(f"📋 Tokens: {len(self.tm.tokens)}")
        logging.info(f"📁 Source chat: {self.source_peer_id}")
        logging.info(f"⏱️ Initial wait time: {self.timing.get_wait_time():.2f}s")
        logging.info("=" * 60)

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
                
            except KeyboardInterrupt:
                logging.info("👋 Shutting down...")
                break
            except Exception as e:
                logging.error(f"Main loop error: {e}", exc_info=True)
                time.sleep(3)


# ====== MAIN ======
def main():
    """Точка входа"""
    config_path = "config.json"
    
    if not os.path.exists(config_path):
        logging.error(f"❌ {config_path} not found")
        logging.info("Creating default config...")
        
        default = {
            "tokens": [],
            "settings": {"delay": 2}
        }
        
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(default, f, indent=2)
        
        logging.info(f"✅ Created {config_path}. Please add tokens via Telegram bot.")
        return

    try:
        bot = MultiTokenBot(config_path)
        bot.run()
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
    finally:
        logging.info("Bot stopped")


if __name__ == "__main__":
    main()
