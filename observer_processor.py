# -*- coding: utf-8 -*-
import logging
import threading
import queue
import time
import re
from typing import Dict, Any, Optional

from .regexes import RE_PROFILE_LEVEL, RE_VOICES_GENERIC, RE_VOICES_ANY
from .constants import RACE_NAMES

logger = logging.getLogger(__name__)


class MessageProcessor:
    """Поток для обработки сообщений из очереди"""

    def __init__(self, bot, queue_type='user'):
        self.bot = bot
        self.queue_type = queue_type
        self._thread = None
        self._running = False
        self.GUILD_BOT_ID = 92900278

        # ID чатов
        self.USER_CHAT_ID = 2000000120  # Чат 120 для команд пользователей и ответов Ара/Кир
        self.GROUP_CHAT_ID = 2000000007  # Чат 7 для команд группы
        self.GAME_CHAT_ID = -183040898   # Чат игры для ответов на /баф

        # Регулярка для голосов в скобках у класса (как в ProfileManager)
        self.RE_VOICES_FROM_CLASS_PARENS = re.compile(
            r"👤\s*Класс:\s*[^\(\n]*\((\d+)\)", re.IGNORECASE
        )

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._worker, daemon=True)
        self._thread.start()
        logger.info(f"📨 Processor ({self.queue_type}) запущен")
        logger.info(f"📋 Фильтрация сообщений: user_chat={self.USER_CHAT_ID}, group_chat={self.GROUP_CHAT_ID}, game_chat={self.GAME_CHAT_ID}")

    def stop(self):
        self._running = False

    def _worker(self):
        while self._running:
            try:
                if self.queue_type == 'user':
                    msg_type, msg = self.bot.user_message_queue.get(timeout=1)
                    self._process_user_message(msg_type, msg)
                else:
                    msg_type, msg = self.bot.group_message_queue.get(timeout=1)
                    self._process_group_message(msg_type, msg)
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"❌ Ошибка в processor ({self.queue_type}): {e}", exc_info=True)

    def _process_user_message(self, msg_type: str, msg: dict):
        """Обработка сообщений из очереди пользовательского токена"""

        from_id = msg.get("from_id", 0)
        msg_id = msg.get("id", 0)
        text = (msg.get("text") or "").strip()
        peer_id = msg.get("peer_id", 0)

        logger.debug(f"📨 Получено сообщение (user): peer={peer_id}, from={from_id}, text='{text[:50]}...'")

        # ============= СНАЧАЛА ПРОВЕРЯЕМ, НЕ ОТВЕТ ЛИ ЭТО ИГРЫ =============
        # Ответы от игры могут приходить в любой чат, но у них from_id < 0
        if from_id < 0:  # Сообщение от игры
            logger.info(f"🎯 Ответ игры в чате {peer_id} от {from_id}")
            try:
                handled = self.bot.triggers_handler.handle_game_response(msg)
                if handled:
                    return
            except Exception as e:
                logger.error(f"❌ Ошибка в handle_game_response: {e}", exc_info=True)
            return
        # ====================================================================

        # Если это не ответ игры, продолжаем обычную фильтрацию по чатам
        if peer_id == self.GAME_CHAT_ID:
            # Обработка сообщений из чата игры
            self._process_game_chat_message(from_id, text, msg)
            return

        if peer_id == self.USER_CHAT_ID:
            self._process_user_commands(from_id, text, msg)
            return

        logger.debug(f"ℹ️ Игнорируем сообщение из чата {peer_id} (не целевой)")

    def _process_game_chat_message(self, from_id: int, text: str, msg: dict):
        """
        Обработка сообщений из чата игры (-183040898)
        - Команда "Мой профиль" от токенов (from_id > 0)
        """
        logger.debug(f"🎮 Сообщение из чата игры: from={from_id}, text='{text[:50]}...'")

        # Обработка "Мой профиль" от токенов (from_id > 0)
        if from_id > 0 and text.lower() == "мой профиль":
            logger.info(f"📋 Получена команда 'Мой профиль' от токена {from_id} в чате игры")

            # Ищем токен по ID отправителя
            token = self.bot.tm.get_token_by_sender_id(from_id)

            if token:
                logger.info(f"✅ Найден токен {token.name} для ID {from_id}")

                threading.Thread(
                    target=self._check_profile_like_manager,
                    args=(token, from_id),
                    daemon=True
                ).start()
            else:
                logger.warning(f"⚠️ Токен для ID {from_id} не найден")
            return

        # Всё остальное в чате игры игнорируем
        logger.debug(f"ℹ️ Игнорируем сообщение в чате игры: {text[:50]}...")

    def _process_user_commands(self, from_id: int, text: str, msg: dict):
        """
        Обработка команд от пользователей в чате 120
        """
        logger.info(f"👤 Команда от пользователя {from_id} в чате 120: {text[:50]}...")

        # Команда воскрешения
        if text.startswith("/воскрешение"):
            self.bot.res_handler.handle(text, from_id)
            return

        # Кастомные триггеры (Ара/Кир)
        if self.bot.triggers_handler.handle_command(text, from_id):
            return

        # Остальные команды (/баф, /диагностика, /апо и т.п.)
        self.bot.cmd_handler.handle(text, from_id, msg)

    def _parse_profile_response(self, text: str) -> Dict[str, Any]:
        """
        Парсер ответа на "Мой профиль" (как в ProfileManager)
        """
        result = {"level": None, "voices": None, "races": []}

        if not text:
            return result

        # 1) Уровень
        m = RE_PROFILE_LEVEL.search(text)
        if m:
            try:
                result["level"] = int(m.group(1))
            except Exception:
                pass

        # 2) Голоса из скобок у класса
        voices = None
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

        races = []
        for race_name, race_key in race_mapping.items():
            if race_name in text_lower:
                races.append(race_key)

        result["races"] = sorted(list(set(races)))
        return result

    def _check_profile_like_manager(self, token, from_id):
        """
        Проверка профиля (как в ProfileManager)
        """
        try:
            logger.info(f"🔍 Проверка профиля для {token.name} по запросу из чата игры")

            # Запоминаем старые значения
            old_voices = token.voices
            old_level = token.level
            old_races = token.races.copy() if token.races else []

            # 1. Отправляем команду
            ok, status = token.send_to_peer(token.target_peer_id, "Мой профиль", None)
            if not ok:
                logger.warning(f"❌ {token.name}: не удалось отправить 'Мой профиль' ({status})")
                return

            # 2. Ждём 3 секунды
            time.sleep(3.0)

            # 3. Инвалидируем кэш и получаем историю
            token.invalidate_cache(token.target_peer_id)
            history = token.get_history_cached(token.target_peer_id, count=25)
            if not history:
                logger.debug(f"ℹ️ {token.name}: нет истории сообщений")
                return

            # 4. Ищем ответ (последние 5 сообщений)
            found_any = False
            for msg in history[:5]:
                text = str(msg.get("text", "") or "").strip()
                if not text:
                    continue

                # Пропускаем само сообщение "Мой профиль"
                if "мой профиль" in text.lower():
                    continue

                # Парсим ответ
                profile_data = self._parse_profile_response(text)

                if profile_data["level"] is None and profile_data["voices"] is None and not profile_data["races"]:
                    continue

                found_any = True
                changes = []

                # Обновляем голоса
                if profile_data["voices"] is not None and token.voices != profile_data["voices"]:
                    old = token.voices
                    token.update_voices_from_system(profile_data["voices"])
                    changes.append(f"голоса: {old}→{token.voices}")

                # Обновляем уровень
                if profile_data["level"] is not None and token.level != profile_data["level"]:
                    old = token.level
                    token.update_level(profile_data["level"])
                    changes.append(f"уровень: {old}→{token.level}")

                # Обновляем расы для апостолов
                if token.class_type == "apostle" and profile_data["races"]:
                    if set(profile_data["races"]) != set(token.races):
                        old = token.races.copy()
                        token.races = profile_data["races"]
                        token.mark_for_save()
                        self.bot.tm.update_race_index(token)
                        changes.append(f"расы: {old}→{token.races}")

                if changes:
                    logger.info(f"✅ {token.name}: обновлён: {', '.join(changes)}")
                else:
                    logger.info(f"ℹ️ {token.name}: профиль не изменился")

                break

            if not found_any:
                logger.debug(f"⚠️ {token.name}: профильный ответ не найден")

        except Exception as e:
            logger.error(f"❌ Ошибка при проверке профиля {token.name}: {e}", exc_info=True)

    def _process_group_message(self, msg_type: str, msg: dict):
        """
        Обработка своих сообщений от группы (чат 7)
        """
        msg_id = msg.get("id", 0)
        text = msg.get("text", "")
        peer_id = msg.get("peer_id", 0)

        # Фильтруем только чат 7
        if peer_id != self.GROUP_CHAT_ID:
            logger.debug(f"ℹ️ Игнорируем сообщение группы из чата {peer_id}")
            return

        logger.info(f"👥 Своё сообщение от группы в чате 7: ID={msg_id}, текст={text[:50]}...")

        if hasattr(self.bot, 'pending_group_messages') and self.bot.pending_group_messages:
            if "✅ Баф зарегистрирован" in text:
                logger.info(f"🔍 Найдено сообщение регистрации от группы, ID={msg_id}")

                found = False
                for temp_id, data in list(self.bot.pending_group_messages.items()):
                    if time.time() - data['time'] < 60:
                        user_id = data['user_id']
                        self.bot.state.update_message_id(user_id, msg_id)

                        if user_id in self.bot.state._active_jobs:
                            self.bot.state._active_jobs[user_id].job.registration_msg_id = msg_id
                            logger.info(f"✅ Job для user_id={user_id} обновлен")

                        logger.info(f"✅ ОБНОВЛЕН registration_msg_id: {temp_id} → {msg_id} для user_id={user_id}")
                        del self.bot.pending_group_messages[temp_id]
                        found = True
                        break

                if not found:
                    logger.warning(f"⚠️ Не найдено ожидающих сообщений для ID={msg_id}")
            else:
                logger.debug(f"ℹ️ Сообщение группы не является регистрацией: {text[:30]}...")
