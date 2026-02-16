# -*- coding: utf-8 -*-
import logging
import threading
import queue
import time

logger = logging.getLogger(__name__)


class MessageProcessor:
    """Поток для обработки сообщений из очереди"""

    def __init__(self, bot, queue_type='user'):
        self.bot = bot
        self.queue_type = queue_type
        self._thread = None
        self._running = False
        # (в старом коде использовался, сейчас не критично)
        self.GUILD_BOT_ID = 92900278

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._worker, daemon=True)
        self._thread.start()
        logger.info(f"📨 Processor ({self.queue_type}) запущен")

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
        """Обработка сообщений из user-чата (120 и других)"""

        from_id = msg.get("from_id", 0)
        msg_id = msg.get("id", 0)
        text = (msg.get("text") or "").strip()

        # ID Ары и Кир из handler'а
        try:
            ara_id = getattr(self.bot.triggers_handler, "ARA_ID", None)
            kir_id = getattr(self.bot.triggers_handler, "KIR_ID", None)
        except Exception:
            ara_id = None
            kir_id = None

        # 1) Сначала пробуем как ОТВЕТ ИГРЫ / Ары / Кира
        #    (игра обычно from_id < 0, Ара/Кир — конкретные id)
        if from_id < 0 or (ara_id and from_id == ara_id) or (kir_id and from_id == kir_id):
            try:
                handled = self.bot.triggers_handler.handle_game_response(msg)
                if handled:
                    # Ответ игры успешно обработан
                    return
            except Exception as e:
                logger.error(f"❌ Ошибка в handle_game_response: {e}", exc_info=True)
            # Даже если не подошло — дальше такие сообщения нам не нужны
            return

        # 2) Если не ответ игры — это команда от живого пользователя
        logger.info(f"👤 Команда от пользователя {from_id}: {text[:50]}...")

        # Команда воскрешения
        if text.startswith("/воскрешение"):
            self.bot.res_handler.handle(text, from_id)
            return

        # Кастомные триггеры (Ара/Кир)
        if self.bot.triggers_handler.handle_command(text, from_id):
            return

        # Остальные команды (/баф, /диагностика и т.п.)
        self.bot.cmd_handler.handle(text, from_id, msg)

    def _process_group_message(self, msg_type: str, msg: dict):
        """Обработка своих сообщений от группы (если понадобится)"""
        msg_id = msg.get("id", 0)
        text = msg.get("text", "")

        logger.info(f"👥 Своё сообщение от группы: ID={msg_id}, текст={text[:50]}...")

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
