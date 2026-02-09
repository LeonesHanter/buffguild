# -*- coding: utf-8 -*-
"""
ProfileManager - менеджер фоновых проверок с чередованием:
1. Проверка профилей всех токенов (каждые 2 часа на токен, чередование 30 мин)
2. Виртуальные голоса для паладинов/проклинателей (раз в 3 часа, максимум 5 попыток)
"""
import logging
import re
import threading
import time
import random
import json
import os
from typing import Dict, List, Optional, Any

from .token_manager import OptimizedTokenManager
from .token_handler import TokenHandler
from .regexes import RE_PROFILE_LEVEL, RE_VOICES_ANY, RE_VOICES_GENERIC

logger = logging.getLogger(__name__)


class ProfileManager:
    """Универсальный менеджер фоновых проверок с чередованием"""

    def __init__(self, token_manager: OptimizedTokenManager):
        self.tm = token_manager
        self._running = False
        self._thread: Optional[threading.Thread] = None

        # Интервалы в секундах
        self.PROFILE_CHECK_INTERVAL = 2 * 60 * 60  # 2 часа между проверками одного токена
        self.TOKEN_CHECK_DELAY = 30 * 60  # 30 минут между запусками разных токенов
        self.VIRTUAL_VOICE_INTERVAL = 3 * 60 * 60  # 3 часа между виртуальными голосами
        self.VIRTUAL_VOICE_RETRY_INTERVAL = 60  # 1 минута между проверками виртуальных голосов
        self.MAX_VIRTUAL_ATTEMPTS = 5  # Максимум 5 попыток выдать виртуальный голос

        # Файл для сохранения состояния
        self.STATE_FILE = "profile_manager_state.json"
        
        # Инициализация состояния (загрузим из файла если есть)
        self._state = self._load_state()

        # Блокировка для потокобезопасности
        self._lock = threading.Lock()

    def _load_state(self) -> Dict[str, Any]:
        """Загружает состояние из файла"""
        state = {
            "last_profile_check": {},  # token_id -> timestamp
            "last_virtual_check": 0,  # Время последней проверки виртуальных голосов
            "virtual_attempts": {},  # token_id -> количество попыток
            "current_token_index": 0,  # Индекс для чередования
            "last_token_check_time": 0,  # Время последней проверки токена
            "last_virtual_grant_times": {}  # token_id -> timestamp последней выдачи вирт. голоса
        }
        
        try:
            if os.path.exists(self.STATE_FILE):
                with open(self.STATE_FILE, 'r', encoding='utf-8') as f:
                    loaded = json.load(f)
                    
                # Проверяем и обновляем состояние
                for key in state.keys():
                    if key in loaded:
                        state[key] = loaded[key]
                
                # Преобразуем timestamps из строк обратно в float
                for token_id, timestamp in state["last_profile_check"].items():
                    if isinstance(timestamp, str):
                        state["last_profile_check"][token_id] = float(timestamp)
                
                if isinstance(state["last_virtual_check"], str):
                    state["last_virtual_check"] = float(state["last_virtual_check"])
                    
                if isinstance(state["last_token_check_time"], str):
                    state["last_token_check_time"] = float(state["last_token_check_time"])
                    
                for token_id, timestamp in state["last_virtual_grant_times"].items():
                    if isinstance(timestamp, str):
                        state["last_virtual_grant_times"][token_id] = float(timestamp)
                
                logger.info(f"✅ Загружено состояние ProfileManager из {self.STATE_FILE}")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки состояния: {e}")
        
        return state

    def _save_state(self):
        """Сохраняет состояние в файл"""
        try:
            with self._lock:
                # Создаем копию состояния для сохранения
                state_to_save = {
                    "last_profile_check": self._state["last_profile_check"].copy(),
                    "last_virtual_check": self._state["last_virtual_check"],
                    "virtual_attempts": self._state["virtual_attempts"].copy(),
                    "current_token_index": self._state["current_token_index"],
                    "last_token_check_time": self._state["last_token_check_time"],
                    "last_virtual_grant_times": self._state["last_virtual_grant_times"].copy()
                }
                
            with open(self.STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(state_to_save, f, indent=2, ensure_ascii=False)
                
            logger.debug(f"💾 Сохранено состояние ProfileManager")
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения состояния: {e}")

    def start(self):
        """Запуск менеджера"""
        if self._running:
            return

        self._running = True
        self._thread = threading.Thread(
            target=self._main_loop,
            daemon=True,
            name="ProfileManager"
        )
        self._thread.start()
        logger.info("🔄 ProfileManager запущен (чередование: 30 мин)")

    def stop(self):
        """Остановка менеджера"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        # Сохраняем состояние при остановке
        self._save_state()
        logger.info("🛑 ProfileManager остановлен")

    def reset_virtual_attempts(self, token_id: str):
        """Сброс счетчика виртуальных попыток для токена"""
        with self._lock:
            if token_id in self._state["virtual_attempts"]:
                old_attempts = self._state["virtual_attempts"][token_id]
                del self._state["virtual_attempts"][token_id]
                
                # Удаляем из времени выдачи виртуальных голосов
                if token_id in self._state["last_virtual_grant_times"]:
                    del self._state["last_virtual_grant_times"][token_id]

                # Сбрасываем флаг в токене
                token = self.tm.get_token_by_id(token_id)
                if token:
                    token.needs_manual_voices = False
                    token.mark_for_save()

                logger.info(f"🔄 Сброс виртуальных попыток для {token_id}: {old_attempts} → 0")
                self._save_state()

    def _get_eligible_tokens(self, for_profile: bool = True) -> List[TokenHandler]:
        """Получает список токенов, доступных для проверки"""
        eligible = []
        observer = self.tm.get_observer()

        for token in self.tm.tokens:
            # Пропускаем отключенные и с CAPTCHA
            if not token.enabled or token.is_captcha_paused():
                continue

            # Пропускаем observer
            if observer and token.id == observer.id:
                continue

            # Для проверки профиля подходят все токены
            if for_profile:
                eligible.append(token)
            # Для виртуальных голосов - только паладины и проклинатели
            elif token.class_type in ["warlock", "crusader", "light_incarnation"]:
                eligible.append(token)

        return eligible

    def _parse_profile_response(self, text: str) -> Dict[str, any]:
        """Парсит ответ на 'Мой профиль'"""
        result = {
            "level": None,
            "voices": None,
            "races": []
        }

        if not text:
            return result

        # 1. Парсим уровень (для всех классов)
        level_match = RE_PROFILE_LEVEL.search(text)
        if level_match:
            try:
                result["level"] = int(level_match.group(1))
            except Exception:
                pass

        # 2. Парсим голоса (для всех классов)
        voices = None

        # Сначала пробуем RE_VOICES_GENERIC
        vm = RE_VOICES_GENERIC.search(text)
        if vm:
            try:
                voices = int(vm.group(1))
            except Exception:
                pass

        # Если не нашли, пробуем RE_VOICES_ANY
        if voices is None:
            vm = RE_VOICES_ANY.search(text)
            if vm:
                try:
                    voices = int(vm.group(1))
                except Exception:
                    pass

        result["voices"] = voices

        # 3. Парсим расы (только для апостолов будет использоваться)
        text_lower = text.lower()
        race_mapping = {
            "человек": "ч", "гоблин": "г", "нежить": "н",
            "эльф": "э", "гном": "м", "демон": "д", "орк": "о",
            "людей": "ч", "гоблинов": "г", "нежити": "н",
            "эльфов": "э", "гномов": "м", "демонов": "д", "орков": "о"
        }

        for race_name, race_key in race_mapping.items():
            if race_name in text_lower:
                result["races"].append(race_key)

        result["races"] = list(set(result["races"]))

        return result

    def _check_single_profile(self, token: TokenHandler) -> bool:
        """
        Проверяет профиль одного токена.
        Без пересылов, просто "Мой профиль"
        """
        logger.info(f"🔍 Проверка профиля: {token.name} ({token.class_type})")

        try:
            # 1. Отправляем "Мой профиль" БЕЗ пересыла
            ok, status = token.send_to_peer(
                token.target_peer_id,
                "Мой профиль",
                None  # Без forward/reply
            )

            if not ok:
                logger.warning(f"❌ {token.name}: не удалось отправить 'Мой профиль' ({status})")
                return False

            # 2. Ждем ответа
            time.sleep(3.0)

            # 3. Получаем свежие сообщения
            token.invalidate_cache(token.target_peer_id)
            history = token.get_history_cached(token.target_peer_id, count=25)

            if not history:
                logger.debug(f"ℹ️ {token.name}: нет истории сообщений")
                return False

            # 4. Ищем ответ на наш запрос (последние 5 сообщений)
            found_data = False
            for msg in history[:5]:
                text = str(msg.get("text", "")).strip()
                if not text or "мой профиль" in text.lower():
                    continue  # Пропускаем сам запрос

                # Парсим профиль
                profile_data = self._parse_profile_response(text)

                # 2.1. Для ВСЕХ классов обновляем голоса из профиля
                if profile_data["voices"] is not None and token.voices != profile_data["voices"]:
                    old_voices = token.voices
                    token.update_voices_from_system(profile_data["voices"])
                    token.mark_for_save()  # ← СОХРАНЕНИЕ
                    logger.info(f"🗣️ {token.name}: голоса {old_voices} → {profile_data['voices']}")
                    found_data = True

                # 2.2. Для паладинов обновляем уровень
                if token.class_type in ["crusader", "light_incarnation"]:
                    if profile_data["level"] is not None and token.level != profile_data["level"]:
                        old_level = token.level
                        token.update_level(profile_data["level"])
                        token.mark_for_save()  # ← СОХРАНЕНИЕ
                        logger.info(f"📊 {token.name}: уровень {old_level} → {profile_data['level']}")
                        found_data = True

                # 2.4. Для апостолов обновляем расы
                if token.class_type == "apostle":
                    if profile_data["races"] and set(profile_data["races"]) != set(token.races):
                        old_races = token.races.copy()
                        token.races = profile_data["races"]
                        token.mark_for_save()
                        self.tm.mark_for_save()  # ← ДОБАВИТЬ ЭТО
                        self.tm.update_race_index(token)
                        logger.info(f"🎭 {token.name}: расы обновлены {old_races} → {token.races}")
                        found_data = True

                if found_data:
                    logger.debug(f"💾 {token.name}: изменения помечены для сохранения")
                    break  # Нашли данные, выходим

            if not found_data:
                logger.debug(f"ℹ️ {token.name}: профиль не дал новых данных")

            # Обновляем время последней проверки
            with self._lock:
                self._state["last_profile_check"][token.id] = time.time()
                self._save_state()

            return found_data

        except Exception as e:
            logger.error(f"❌ {token.name}: ошибка проверки профиля: {e}", exc_info=True)
            return False

    def _should_check_profile(self, token: TokenHandler) -> bool:
        """Проверяет, нужно ли проверять профиль токена"""
        with self._lock:
            last_check = self._state["last_profile_check"].get(token.id, 0)
            return time.time() - last_check >= self.PROFILE_CHECK_INTERVAL

    def _check_next_profile(self):
        """
        Проверяет профиль следующего токена в очереди (чередование 30 мин).
        """
        current_time = time.time()

        # Проверяем интервал между проверками разных токенов
        with self._lock:
            if current_time - self._state["last_token_check_time"] < self.TOKEN_CHECK_DELAY:
                return

        eligible_tokens = self._get_eligible_tokens(for_profile=True)
        if not eligible_tokens:
            return

        # Ищем токен, который нужно проверить
        token_to_check = None
        
        with self._lock:
            start_index = self._state["current_token_index"]

        for i in range(len(eligible_tokens)):
            idx = (start_index + i) % len(eligible_tokens)
            token = eligible_tokens[idx]

            if self._should_check_profile(token):
                token_to_check = token
                with self._lock:
                    self._state["current_token_index"] = (idx + 1) % len(eligible_tokens)
                    self._save_state()
                break

        if token_to_check:
            self._check_single_profile(token_to_check)
            with self._lock:
                self._state["last_token_check_time"] = current_time
                self._save_state()
            logger.info(f"⏭️ Следующая проверка профиля через {self.TOKEN_CHECK_DELAY//60} мин")

    def _grant_virtual_voice(self, token: TokenHandler) -> bool:
        """
        Выдает виртуальный голос паладину/проклинателю.
        Вызывается только если голосов 0.
        """
        with self._lock:
            try:
                # Увеличиваем счетчик попыток
                attempts = self._state["virtual_attempts"].get(token.id, 0) + 1
                self._state["virtual_attempts"][token.id] = attempts

                # Даем виртуальный голос
                old_voices = token.voices
                token.voices = 1
                token.mark_for_save()  # ← СОХРАНЕНИЕ

                logger.info(f"🎁 {token.name}: виртуальный голос выдан (попытка {attempts}/{self.MAX_VIRTUAL_ATTEMPTS})")

                # Если превысили лимит, помечаем для ручного ввода
                if attempts >= self.MAX_VIRTUAL_ATTEMPTS:
                    token.needs_manual_voices = True
                    token.mark_for_save()  # ← СОХРАНЕНИЕ
                    logger.warning(f"🚫 {token.name}: превышен лимит виртуальных голосов. Требуется ручной ввод.")

                # Сохраняем состояние
                self._save_state()

                return True

            except Exception as e:
                logger.error(f"❌ {token.name}: ошибка выдачи виртуального голоса: {e}")
                return False

    def _check_virtual_voices(self):
        """
        Проверяет паладинов/проклинателей на 0 голосов.
        Выдает виртуальный голос если нужно.
        """
        current_time = time.time()

        # Проверяем интервал
        with self._lock:
            if current_time - self._state["last_virtual_check"] < self.VIRTUAL_VOICE_RETRY_INTERVAL:
                return

        eligible_tokens = self._get_eligible_tokens(for_profile=False)
        candidates = []

        for token in eligible_tokens:
            # Проверяем базовые условия
            if token.needs_manual_voices:
                continue  # Пропускаем, требует ручного ввода

            # Проверяем интервал 3 часа с последней успешной выдачи
            with self._lock:
                last_grant_time = self._state["last_virtual_grant_times"].get(token.id, 0)
            
            if current_time - last_grant_time < self.VIRTUAL_VOICE_INTERVAL:
                continue

            # Проверяем лимит попыток
            with self._lock:
                attempts = self._state["virtual_attempts"].get(token.id, 0)
            
            if attempts >= self.MAX_VIRTUAL_ATTEMPTS:
                token.needs_manual_voices = True
                token.mark_for_save()  # ← СОХРАНЕНИЕ
                logger.warning(f"🚫 {token.name}: превышен лимит виртуальных голосов ({self.MAX_VIRTUAL_ATTEMPTS}). Требуется ручной ввод.")
                continue

            # Если голосов 0 - кандидат на получение
            if token.voices <= 0:
                candidates.append(token)

        if candidates:
            logger.info(f"🎁 Найдено кандидатов на виртуальный голос: {len(candidates)}")

            for token in candidates:
                if self._grant_virtual_voice(token):
                    with self._lock:
                        self._state["last_virtual_grant_times"][token.id] = current_time
                        self._save_state()
                time.sleep(1)  # Пауза между выдачами

        with self._lock:
            self._state["last_virtual_check"] = current_time
            self._save_state()

    def _main_loop(self):
        """Основной цикл менеджера"""
        # Начальная случайная задержка для распределения нагрузки
        time.sleep(random.randint(0, 300))

        while self._running:
            try:
                # Шаг 1: Проверяем следующий профиль (через 30 мин после предыдущего)
                self._check_next_profile()

                # Шаг 2: Проверяем виртуальные голоса (каждую минуту)
                self._check_virtual_voices()

                # Ждем 1 минуту до следующей итерации
                for _ in range(60):
                    if not self._running:
                        break
                    time.sleep(1)

            except Exception as e:
                logger.error(f"❌ Ошибка в ProfileManager: {e}", exc_info=True)
                time.sleep(60)
        # Сохраняем состояние при выходе из цикла
        self._save_state()
