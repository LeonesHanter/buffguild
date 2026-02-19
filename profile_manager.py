# -*- coding: utf-8 -*-
"""
ProfileManager — менеджер фоновых проверок (профиль/виртуальные голоса).

Что делает:
1) "Мой профиль" — обновление голосов/уровня/рас.
2) "Виртуальные голоса" — если у паладинов/проклинателей/АПОСТОЛОВ 0 голосов.

Режимы проверки профиля:
- WARMUP (после запуска): проходим ВСЕ доступные токены по одному, каждые 2 минуты.
  Как только сделали полный круг по списку на момент старта — переключаемся в NORMAL.
- NORMAL: по одному токену каждые 30 минут, при этом один и тот же токен не проверяем чаще,
  чем раз в 2 часа (PROFILE_CHECK_INTERVAL).

Файл состояния: profile_manager_state.json
"""
import json
import logging
import os
import random
import re
import threading
import time
from typing import Any, Dict, List, Optional

from .regexes import RE_PROFILE_LEVEL, RE_VOICES_ANY, RE_VOICES_GENERIC
from .token_handler import TokenHandler
from .token_manager import OptimizedTokenManager

logger = logging.getLogger(__name__)


class ProfileManager:
    """Менеджер фоновых проверок профиля и виртуальных голосов."""

    # --- интервалы (сек) ---
    PROFILE_CHECK_INTERVAL = 2 * 60 * 60  # 2 часа между проверками одного токена (NORMAL)
    TOKEN_CHECK_DELAY_NORMAL = 30 * 60    # 30 минут между разными токенами (NORMAL)

    # WARMUP: после запуска пройти всех токенов быстрее
    TOKEN_CHECK_DELAY_WARMUP = 120        # 2 минуты между токенами в WARMUP

    VIRTUAL_VOICE_INTERVAL = 3 * 60 * 60       # 3 часа между успешными "вирт. голосами"
    VIRTUAL_VOICE_RETRY_INTERVAL = 60          # 1 минута между проверками "вирт. голосов"
    MAX_VIRTUAL_ATTEMPTS = 5                   # максимум попыток выдать виртуальный голос

    STATE_FILE = "profile_manager_state.json"
    
    # ============= Voice Prophet Storage =============
    VOICE_PROPHET_STORAGE = "data/voice_prophet"
    # ================================================

    # Из профиля "голоса" — это число в скобках у класса: "Класс: апостол (25), ..."
    RE_VOICES_FROM_CLASS_PARENS = re.compile(r"👤\s*Класс:\s*[^\(\n]*\((\d+)\)", re.IGNORECASE)

    def __init__(self, token_manager: OptimizedTokenManager):
        self.tm = token_manager
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

        self._state = self._load_state()

        # Отладочная инфа по путям
        try:
            cwd = os.getcwd()
            sf = os.path.abspath(self.STATE_FILE)
            logger.info(f"🧾 ProfileManager: STATE_FILE='{sf}', cwd='{cwd}'")
        except Exception:
            pass

    # ---------------------------
    # State load/save
    # ---------------------------

    def _default_state(self) -> Dict[str, Any]:
        return {
            "last_profile_check": {},         # token_id -> ts
            "last_token_check_time": 0.0,     # ts последней проверки любого токена
            "current_token_index": 0,         # индекс очереди

            "last_virtual_check": 0.0,        # ts последней проверки виртуальных голосов
            "virtual_attempts": {},           # token_id -> attempts
            "last_virtual_grant_times": {},   # token_id -> ts

            # WARMUP
            "warmup_done": False,
            "warmup_target_ids": [],          # снимок списка токенов при старте warmup
            "warmup_checked_ids": [],         # кого уже проверили в warmup
        }

    def _load_state(self) -> Dict[str, Any]:
        state = self._default_state()

        try:
            if not os.path.exists(self.STATE_FILE):
                logger.warning(f"ℹ️ ProfileManager: state-файл не найден: {os.path.abspath(self.STATE_FILE)}")
                return state

            with open(self.STATE_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f) or {}

            for k in state.keys():
                if k in loaded:
                    state[k] = loaded[k]

            # нормализуем типы
            for k in ("last_token_check_time", "last_virtual_check"):
                try:
                    state[k] = float(state.get(k, 0) or 0)
                except Exception:
                    state[k] = 0.0

            lpc = state.get("last_profile_check", {}) or {}
            if isinstance(lpc, dict):
                for tid, ts in list(lpc.items()):
                    try:
                        lpc[tid] = float(ts)
                    except Exception:
                        lpc[tid] = 0.0
            else:
                state["last_profile_check"] = {}

            lvg = state.get("last_virtual_grant_times", {}) or {}
            if isinstance(lvg, dict):
                for tid, ts in list(lvg.items()):
                    try:
                        lvg[tid] = float(ts)
                    except Exception:
                        lvg[tid] = 0.0
            else:
                state["last_virtual_grant_times"] = {}

            # warmup поля
            if not isinstance(state.get("warmup_target_ids"), list):
                state["warmup_target_ids"] = []
            if not isinstance(state.get("warmup_checked_ids"), list):
                state["warmup_checked_ids"] = []
            state["warmup_done"] = bool(state.get("warmup_done", False))

            logger.info(f"✅ ProfileManager: состояние загружено из {self.STATE_FILE}")
            return state

        except Exception as e:
            logger.error(f"❌ ProfileManager: ошибка загрузки состояния: {e}", exc_info=True)
            return state

    def _save_state(self) -> None:
        try:
            with self._lock:
                state_to_save = {
                    k: self._state.get(k)
                    for k in self._default_state().keys()
                }
            with open(self.STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(state_to_save, f, indent=2, ensure_ascii=False)
            logger.debug(f"💾 ProfileManager: состояние сохранено -> {os.path.abspath(self.STATE_FILE)}")
        except Exception as e:
            logger.error(f"❌ ProfileManager: ошибка сохранения состояния: {e}", exc_info=True)

    # ---------------------------
    # Public control
    # ---------------------------

    def start(self) -> None:
        """Запуск ProfileManager с активацией Voice Prophet"""
        if self._running:
            return
        
        # ============= Активируем Voice Prophet для всех токенов =============
        for token in self.tm.tokens:
            if token.class_type in ["apostle", "crusader", "light_incarnation"]:
                if not token.voice_prophet:
                    token.enable_voice_prophet(self.VOICE_PROPHET_STORAGE)
                    logger.debug(f"🔮 Voice Prophet активирован для {token.name}")
        # ====================================================================

        self._running = True
        self._thread = threading.Thread(
            target=self._main_loop,
            daemon=True,
            name="ProfileManager",
        )
        self._thread.start()
        logger.info("🔄 ProfileManager запущен с Voice Prophet")

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        self._save_state()
        logger.info("🛑 ProfileManager остановлен")

    def reset_virtual_attempts(self, token_id: str) -> None:
        """Сброс счетчика виртуальных попыток для токена."""
        with self._lock:
            old_attempts = int(self._state.get("virtual_attempts", {}).get(token_id, 0))
            self._state.get("virtual_attempts", {}).pop(token_id, None)
            self._state.get("last_virtual_grant_times", {}).pop(token_id, None)

        token = self.tm.get_token_by_id(token_id)
        if token:
            token.needs_manual_voices = False
            token.mark_for_save()

        logger.info(f"🔄 ProfileManager: сброс виртуальных попыток для {token_id}: {old_attempts} → 0")
        self._save_state()

    # ---------------------------
    # Token selection
    # ---------------------------

    def _get_eligible_tokens(self, for_profile: bool = True) -> List[TokenHandler]:
        eligible: List[TokenHandler] = []
        observer = self.tm.get_observer()

        for token in self.tm.tokens:
            if not token.enabled or token.is_captcha_paused():
                continue

            if observer and token.id == observer.id:
                continue

            if for_profile:
                # ДЛЯ ПРОВЕРКИ ПРОФИЛЯ: исключаем warlock
                if token.class_type not in ["warlock"]:
                    eligible.append(token)
            else:
                # ДЛЯ ВИРТУАЛЬНЫХ ГОЛОСОВ: все классы, включая warlock
                if token.class_type in ["warlock", "crusader", "light_incarnation", "apostle"]:
                    eligible.append(token)

        return eligible

    # ---------------------------
    # Profile parsing
    # ---------------------------

    def _parse_profile_response(self, text: str) -> Dict[str, Any]:
        """
        Парсит ответ на "Мой профиль".
        Важно: голоса берём из числа в скобках у класса.
        """
        result: Dict[str, Any] = {"level": None, "voices": None, "races": []}

        if not text:
            return result

        # 1) Уровень
        m = RE_PROFILE_LEVEL.search(text)
        if m:
            try:
                result["level"] = int(m.group(1))
            except Exception:
                pass

        # 2) Голоса — приоритет: число в скобках у класса
        voices: Optional[int] = None
        vm = self.RE_VOICES_FROM_CLASS_PARENS.search(text)
        if vm:
            try:
                voices = int(vm.group(1))
            except Exception:
                voices = None

        # fallback: старые regexes
        if voices is None:
            vm = RE_VOICES_GENERIC.search(text)
            if vm:
                try:
                    voices = int(vm.group(1))
                except Exception:
                    voices = None

        if voices is None:
            vm = RE_VOICES_ANY.search(text)
            if vm:
                try:
                    voices = int(vm.group(1))
                except Exception:
                    voices = None

        result["voices"] = voices

        # 3) Расы
        text_lower = text.lower()
        race_mapping = {
            "человек": "ч", "гоблин": "г", "нежить": "н",
            "эльф": "э", "гном": "м", "демон": "д", "орк": "о",
            "людей": "ч", "гоблинов": "г", "нежити": "н",
            "эльфов": "э", "гномов": "м", "демонов": "д", "орков": "о",
        }

        races: List[str] = []
        for race_name, race_key in race_mapping.items():
            if race_name in text_lower:
                races.append(race_key)

        result["races"] = sorted(list(set(races)))
        return result

    # ---------------------------
    # Profile check logic
    # ---------------------------
    
    # ============= Voice Prophet Integration =============
    def _should_check_profile_normal(self, token: TokenHandler) -> bool:
        """
        Используем Voice Prophet для принятия решения о проверке.
        """
        if token.voice_prophet:
            return token.voice_prophet.should_check_profile()
        
        # Старая логика (fallback)
        with self._lock:
            last = float(self._state.get("last_profile_check", {}).get(token.id, 0) or 0)
        return (time.time() - last) >= float(self.PROFILE_CHECK_INTERVAL)
    # ====================================================

    def _check_single_profile(self, token: TokenHandler) -> bool:
        """
        Проверяет профиль одного токена.
        Возвращает True если были изменения (голоса/уровень/расы), иначе False.
        """
        if token.class_type == "warlock":
            logger.debug(f"⏭️ {token.name}: пропускаем проверку профиля (warlock)")
            return False

        logger.info(f"🔍 Проверка профиля: {token.name} ({token.class_type})")

        try:
            ok, status = token.send_to_peer(token.target_peer_id, "Мой профиль", None)
            if not ok:
                logger.warning(f"❌ {token.name}: не удалось отправить 'Мой профиль' ({status})")
                return False

            time.sleep(3.0)

            token.invalidate_cache(token.target_peer_id)
            history = token.get_history_cached(token.target_peer_id, count=25)
            if not history:
                logger.debug(f"ℹ️ {token.name}: нет истории сообщений")
                return False

            found_any_change = False
            found_any_profile_msg = False

            # Смотрим последние 5 сообщений
            for msg in history[:5]:
                text = str(msg.get("text", "") or "").strip()
                if not text:
                    continue

                if "мой профиль" in text.lower():
                    continue

                meta = {
                    "from_id": msg.get("from_id"),
                    "cmid": msg.get("conversation_message_id"),
                    "date": msg.get("date"),
                }
                logger.debug(f"📩 {token.name}: raw profile text:\n{text[:200]}...")

                profile_data = self._parse_profile_response(text)
                logger.debug(f"🧩 {token.name}: parsed profile_data={profile_data}")

                if profile_data["level"] is None and profile_data["voices"] is None and not profile_data["races"]:
                    continue

                found_any_profile_msg = True

                # 1) Голоса — для всех классов
                if profile_data["voices"] is not None:
                    new_voices = int(profile_data["voices"])
                    
                    # Запоминаем старые значения для Voice Prophet
                    old_voices = token.voices
                    old_manual_flag = token.needs_manual_voices
                    
                    # Обновляем голоса через систему (этот метод сам решит, сбрасывать ли флаг)
                    token.update_voices_from_system(new_voices)
                    
                    # Логируем изменение
                    if old_voices != new_voices:
                        logger.info(f"🗣 {token.name}: voices {old_voices} → {new_voices}")
                        found_any_change = True
                        
                        # Если были реальные голоса и флаг ручного ввода сбросился
                        if old_manual_flag and not token.needs_manual_voices and new_voices > 0:
                            logger.info(f"✅ {token.name}: сброшен флаг ручного ввода (получены реальные голоса)")
                    else:
                        # Голоса не изменились, но проверяем, не было ли виртуальных
                        if token.virtual_voices > 0 and new_voices > 0:
                            # Токен получил реальные голоса, виртуальные больше не нужны
                            token.clear_virtual_voices()
                            logger.info(f"✅ {token.name}: виртуальные голоса очищены (получены реальные)")
                            found_any_change = True

                # 2) Уровень — для паладинов/воплощений
                if token.class_type in ["crusader", "light_incarnation"]:
                    if profile_data["level"] is not None and token.level != int(profile_data["level"]):
                        old = token.level
                        token.update_level(int(profile_data["level"]))
                        token.mark_for_save()
                        logger.info(f"📊 {token.name}: уровень {old} → {token.level}")
                        found_any_change = True
                
                # 3) Уровень для апостолов
                if token.class_type == "apostle":
                    if profile_data["level"] is not None and token.level != int(profile_data["level"]):
                        old = token.level
                        token.update_level(int(profile_data["level"]))
                        token.mark_for_save()
                        logger.info(f"📊 {token.name}: уровень {old} → {token.level}")
                        found_any_change = True

                # 4) Расы — для апостолов
                if token.class_type == "apostle":
                    races = profile_data.get("races") or []
                    if races and set(races) != set(token.races):
                        old_races = token.races.copy()
                        token.races = list(races)
                        token.mark_for_save()
                        self.tm.mark_for_save()
                        self.tm.update_race_index(token)
                        logger.info(f"🎭 {token.name}: расы обновлены {old_races} → {token.races}")
                        found_any_change = True

                break

            if not found_any_profile_msg:
                logger.debug(f"⚠️ {token.name}: профильный ответ не найден")
            elif not found_any_change:
                logger.debug(f"ℹ️ {token.name}: профиль не дал новых данных")
            
            # Логируем статистику Voice Prophet
            if token.voice_prophet and token.voices <= 3:
                stats = token.voice_prophet.get_stats()
                logger.debug(
                    f"📊 {token.name}: голосов {token.voices}, "
                    f"предсказание: {stats['next_predicted_zero']}, "
                    f"уверенность: {stats['confidence']}"
                )

            with self._lock:
                self._state["last_profile_check"][token.id] = float(time.time())
            self._save_state()

            return found_any_change

        except Exception as e:
            logger.error(f"❌ {token.name}: ошибка проверки профиля: {e}", exc_info=True)
            return False

    # ---------------------------
    # Warmup logic
    # ---------------------------

    def _ensure_warmup_targets(self, eligible_tokens: List[TokenHandler]) -> None:
        """Если warmup ещё не инициализирован — фиксируем список токенов на момент старта."""
        with self._lock:
            if self._state.get("warmup_done", False):
                return
            if self._state.get("warmup_target_ids"):
                return

            target_ids = [t.id for t in eligible_tokens]
            self._state["warmup_target_ids"] = target_ids
            self._state["warmup_checked_ids"] = []
            self._state["current_token_index"] = 0
        self._save_state()
        logger.info(f"🧩 ProfileManager: warmup_targets={len(target_ids)}")

    def _warmup_mark_checked(self, token_id: str) -> None:
        with self._lock:
            checked = set(self._state.get("warmup_checked_ids", []) or [])
            checked.add(token_id)
            self._state["warmup_checked_ids"] = sorted(list(checked))

            targets = set(self._state.get("warmup_target_ids", []) or [])
            if targets and checked.issuperset(targets):
                self._state["warmup_done"] = True
        self._save_state()

        if self._state.get("warmup_done", False):
            logger.info("✅ ProfileManager: warmup завершён — переключаюсь на NORMAL (30 мин)")

    # ---------------------------
    # Profile scheduling
    # ---------------------------

    def _check_next_profile(self) -> None:
        now = time.time()

        eligible = self._get_eligible_tokens(for_profile=True)
        if not eligible:
            return

        with self._lock:
            warmup_done = bool(self._state.get("warmup_done", False))

        if not warmup_done:
            self._ensure_warmup_targets(eligible)
            delay_needed = float(self.TOKEN_CHECK_DELAY_WARMUP)
        else:
            delay_needed = float(self.TOKEN_CHECK_DELAY_NORMAL)

        with self._lock:
            last_any = float(self._state.get("last_token_check_time", 0) or 0)
        dt = now - last_any
        if dt < delay_needed:
            logger.debug(f"⏳ ProfileManager: skip TOKEN_CHECK_DELAY (dt={int(dt)}s, need={int(delay_needed)}s)")
            return

        token_to_check: Optional[TokenHandler] = None

        with self._lock:
            start_index = int(self._state.get("current_token_index", 0) or 0)

        if not warmup_done:
            idx = start_index % len(eligible)
            token_to_check = eligible[idx]
            with self._lock:
                self._state["current_token_index"] = (idx + 1) % len(eligible)
        else:
            for i in range(len(eligible)):
                idx = (start_index + i) % len(eligible)
                t = eligible[idx]
                if self._should_check_profile_normal(t):
                    token_to_check = t
                    with self._lock:
                        self._state["current_token_index"] = (idx + 1) % len(eligible)
                    break

        self._save_state()

        if not token_to_check:
            return

        ok = self._check_single_profile(token_to_check)

        with self._lock:
            self._state["last_token_check_time"] = float(now)
        self._save_state()

        if not warmup_done:
            self._warmup_mark_checked(token_to_check.id)

        if warmup_done:
            logger.info(f"⏭️ ProfileManager: проверили '{token_to_check.name}', ok={ok}. Следующая проверка через 30 мин")
        else:
            logger.info(f"⏭️ ProfileManager: проверили '{token_to_check.name}', ok={ok}. Следующая проверка через 2 мин")

    # ---------------------------
    # Virtual voices
    # ---------------------------

    def _grant_virtual_voice(self, token: TokenHandler) -> bool:
        """
        Выдать виртуальный голос токену.
        Теперь работает для ВСЕХ классов, включая апостолов и warlock.
        """
        try:
            with self._lock:
                attempts = int(self._state.get("virtual_attempts", {}).get(token.id, 0)) + 1
                self._state.setdefault("virtual_attempts", {})[token.id] = attempts

            # Запоминаем, что это виртуальный голос
            old_voices = token.voices
            old_virtual = token.virtual_voices
            
            # Увеличиваем счётчик виртуальных голосов
            token.virtual_voices += 1
            # Для совместимости также увеличиваем реальные голоса
            token.voices += 1
            token.mark_for_save()

            logger.info(
                f"🎁 {token.name}: виртуальный голос выдан "
                f"(попытка {attempts}/{self.MAX_VIRTUAL_ATTEMPTS}), "
                f"голоса {old_voices}→{token.voices} (виртуальных: {old_virtual}→{token.virtual_voices})"
            )

            if attempts >= self.MAX_VIRTUAL_ATTEMPTS:
                token.needs_manual_voices = True
                token.mark_for_save()
                logger.warning(
                    f"🚫 {token.name}: превышен лимит виртуальных голосов ({self.MAX_VIRTUAL_ATTEMPTS}). "
                    f"Требуется ручной ввод."
                )

            self._save_state()
            return True

        except Exception as e:
            logger.error(f"❌ {token.name}: ошибка выдачи виртуального голоса: {e}", exc_info=True)
            return False

    def _check_virtual_voices(self) -> None:
        """
        Проверка и выдача виртуальных голосов.
        ТЕПЕРЬ ВКЛЮЧАЕТ ВСЕ КЛАССЫ!
        """
        now = time.time()

        with self._lock:
            last = float(self._state.get("last_virtual_check", 0) or 0)
        if now - last < float(self.VIRTUAL_VOICE_RETRY_INTERVAL):
            return

        eligible = self._get_eligible_tokens(for_profile=False)

        candidates: List[TokenHandler] = []
        for token in eligible:
            if token.needs_manual_voices:
                continue

            with self._lock:
                last_grant = float(self._state.get("last_virtual_grant_times", {}).get(token.id, 0) or 0)
                attempts = int(self._state.get("virtual_attempts", {}).get(token.id, 0) or 0)

            if now - last_grant < float(self.VIRTUAL_VOICE_INTERVAL):
                continue

            if attempts >= int(self.MAX_VIRTUAL_ATTEMPTS):
                token.needs_manual_voices = True
                token.mark_for_save()
                logger.warning(
                    f"🚫 {token.name}: превышен лимит виртуальных голосов "
                    f"({self.MAX_VIRTUAL_ATTEMPTS})"
                )
                continue

            # Учитываем виртуальные голоса при проверке
            if token.voices <= 0 and token.virtual_voices == 0:
                candidates.append(token)
            elif token.voices <= 0 and token.virtual_voices > 0:
                # Уже есть виртуальные голоса, но реальных нет
                logger.debug(f"ℹ️ {token.name}: есть виртуальные голоса ({token.virtual_voices}), но реальных нет")

        logger.debug(f"🎟️ ProfileManager: eligible_for_virtual={len(eligible)}")

        if candidates:
            logger.info(f"🎁 Найдено кандидатов на виртуальный голос: {len(candidates)}")
            for token in candidates:
                logger.debug(f"   • {token.name} ({token.class_type}) - {token.voices} голосов")
            
            for token in candidates:
                if self._grant_virtual_voice(token):
                    with self._lock:
                        self._state.setdefault("last_virtual_grant_times", {})[token.id] = float(now)
                    self._save_state()
                time.sleep(1)

        with self._lock:
            self._state["last_virtual_check"] = float(now)
        self._save_state()

    # ---------------------------
    # Main loop
    # ---------------------------

    def _main_loop(self) -> None:
        jitter = random.randint(0, 300)
        logger.info(f"⏳ ProfileManager: initial jitter sleep {jitter}s")
        time.sleep(jitter)

        logger.info("✅ ProfileManager: main loop entered")

        tick = 0
        while self._running:
            tick += 1
            try:
                logger.debug(f"💓 ProfileManager: tick={tick}")

                eligible_for_profile = self._get_eligible_tokens(for_profile=True)
                if tick == 1:
                    preview = ", ".join([f"{t.name}/{t.class_type}" for t in eligible_for_profile[:8]])
                    suffix = "..." if len(eligible_for_profile) > 8 else ""
                    logger.debug(f"🧩 ProfileManager: eligible_for_profile={len(eligible_for_profile)} [{preview}{suffix}]")

                self._check_next_profile()
                self._check_virtual_voices()

                for _ in range(60):
                    if not self._running:
                        break
                    time.sleep(1)

            except Exception as e:
                logger.error(f"❌ ProfileManager: ошибка в main loop: {e}", exc_info=True)
                time.sleep(60)

        self._save_state()
