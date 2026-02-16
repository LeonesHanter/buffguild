# -*- coding: utf-8 -*-
import logging
import re
import threading
import time

from .custom_triggers import custom_parser, custom_storage, CustomBuff

logger = logging.getLogger(__name__)


class CustomTriggerHandler:
    def __init__(self, bot):
        self.bot = bot
        # VK ID аккаунтов Ара и Кир (исполнители бафов)
        self.ARA_ID = 294529251
        self.KIR_ID = 8244449

    def handle_command(self, text: str, from_id: int) -> bool:
        """Обработка команды от пользователя (Ара/Кир)"""
        trig, q = custom_parser.parse_command(text)
        if not trig or not q:
            return False

        keys = custom_parser.parse_buff_query(trig, q)
        if not keys:
            return False

        ex_id = self.ARA_ID if trig == 'ара' else self.KIR_ID
        logger.info(f"🎯 {trig} для @id{from_id}: {keys}")

        # Регистрируем триггер
        custom_storage.register_trigger(from_id, trig, ex_id, keys)

        # Запускаем поток ожидания
        threading.Thread(
            target=self._wait,
            args=(from_id, len(keys)),
            daemon=True
        ).start()
        return True

    def handle_game_response(self, msg: dict) -> bool:
        """Обработка ответа от игры"""
        text = msg.get("text", "")
        msg_id = msg.get("id", 0)
        cmid = msg.get("conversation_message_id", 0)

        logger.info(
            f"📩 ПОЛУЧЕН ОТВЕТ ИГРЫ: id={msg_id}, cmid={cmid}"
        )
        logger.info(f"📄 Текст ответа: {text[:200]}...")

        # Ищем ID пользователя в тексте [id123|Имя]
        m = re.search(r'\[id(\d+)\|', text)
        if not m:
            logger.debug("❌ Не найден ID пользователя в ответе")
            return False

        uid = int(m.group(1))
        logger.info(f"👤 ID пользователя в ответе: {uid}")

        # Проверяем, есть ли активный триггер для этого пользователя
        tdata = custom_storage.get_trigger_data(uid)
        if not tdata:
            logger.debug(f"❌ Нет активного триггера для {uid}")
            return False

        logger.info(f"📋 Ожидаемые бафы: {tdata['buff_keys']}")

        low = text.lower()
        bkey = None

        # Атака
        if any(word in low for word in ["атак", "🗡️", "меч", "оружи"]):
            bkey = 'а'
            logger.info("✅ Определен баф: АТАКА")
        # Защита
        elif any(word in low for word in ["защит", "🛡️", "брон", "щит", "броня"]):
            bkey = 'з'
            logger.info("✅ Определен баф: ЗАЩИТА")
        # Удача
        elif any(word in low for word in ["удач", "🍀", "везен", "фортун"]):
            bkey = 'у'
            logger.info("✅ Определен баф: УДАЧА")
        # Человек
        elif any(word in low for word in ["человек", "людей", "🧍"]):
            bkey = 'ч'
            logger.info("✅ Определен баф: ЧЕЛОВЕК")
        # Эльф
        elif any(word in low for word in ["эльф", "🧝"]):
            bkey = 'э'
            logger.info("✅ Определен баф: ЭЛЬФ")
        else:
            logger.warning(
                f"❌ Не удалось определить тип бафа в тексте: "
                f"{text[:100]}"
            )
            return False

        logger.info(f"🔑 Определен ключ бафа: {bkey}")

        if bkey not in tdata['buff_keys']:
            logger.warning(
                f"❌ Баф {bkey} не в списке ожидаемых "
                f"{tdata['buff_keys']}"
            )
            return False

        crit, val, buff_type = custom_parser.parse_game_response(text)
        voices = custom_parser.extract_voices_from_response(text)

        buff = CustomBuff(
            trigger=tdata['trigger'],
            buff_key=bkey,
            buff_name=custom_parser.buff_names[bkey],
            is_critical=crit,
            buff_value=val,
            full_response=text,
            user_id=uid,
            executor_id=tdata['executor_id'],
            timestamp=time.time()
        )

        all_col, notif = custom_storage.add_response(uid, buff)
        custom_storage.mark_msg_processed(msg_id, cmid)

        current = len(tdata['responses'])
        total = len(tdata['buff_keys'])
        logger.info(
            f"✅ Добавлен {bkey} для {uid} "
            f"({current}/{total})"
        )

        if notif:
            self._send_notif(uid)
        return True

    def _wait(self, uid: int, need: int):
        max_wait = 300
        waited = 0
        interval = 0.5
        command_check_interval = 5
        last_command_check = time.time()

        logger.info(
            f"⏳ Начато ожидание {need} бафов "
            f"для user_id={uid}"
        )

        while waited < max_wait:
            time.sleep(interval)
            waited += interval
            now = time.time()

            if now - last_command_check >= command_check_interval:
                last_command_check = now
                td = custom_storage.get_trigger_data(uid)
                if td:
                    received = len(td['responses'])
                    logger.info(
                        f"⏳ Ожидание бафов для {uid}: "
                        f"{received}/{need}"
                    )
                    if received >= need:
                        logger.info(
                            f"✅ Все {need} ответов получены для {uid}"
                        )
                        return
                else:
                    logger.debug(
                        f"ℹ️ Триггер для {uid} уже завершен"
                    )
                    return

        logger.warning(
            f"⏰ Таймаут для user_id={uid} "
            f"(прошло {max_wait} секунд)"
        )
        td = custom_storage.get_trigger_data(uid)

        if td:
            received = len(td['responses'])
            if received > 0:
                logger.info(
                    f"📤 Уведомление по таймауту для {uid} "
                    f"({received}/{need})"
                )

                if not custom_storage.has_notification_been_sent(uid):
                    self._send_notif(uid)
                else:
                    logger.debug(
                        f"ℹ️ Уведомление для {uid} уже было "
                        f"отправлено"
                    )

                custom_storage.complete_trigger(
                    uid, keep_notification_flag=True
                )
            else:
                logger.info(
                    f"🔇 Триггер для {uid} без ответов — "
                    f"ничего не выводим"
                )
                custom_storage.complete_trigger(
                    uid, keep_notification_flag=False
                )
        else:
            logger.debug(
                f"ℹ️ Триггер для {uid} уже был завершен"
            )

    def _send_notif(self, uid: int):
        """Отправка нотификации (120 + дубль в 7)"""
        td = custom_storage.get_trigger_data(uid)
        rs = custom_storage.get_responses(uid)

        if not td or not rs:
            logger.warning(
                f"⚠️ Нет данных для уведомления user_id={uid}"
            )
            return

        notif = custom_parser.format_notification(
            td['trigger'],
            uid,
            td['executor_id'],
            rs
        )

        # 1) В 120 чат – через пользовательский токен (как раньше)
        try:
            if hasattr(self.bot, 'reader_token') and self.bot.reader_token:
                ok, status = self.bot.reader_token.send_to_peer(
                    self.bot.source_peer_id,
                    notif
                )
                logger.info(
                    f"📤 [Custom] в чат 120 (user): "
                    f"ok={ok}, status={status}"
                )
            else:
                # Фолбэк – если почему-то нет reader_token
                self.bot.send_to_peer(
                    self.bot.source_peer_id, notif
                )
                logger.info(
                    f"📤 [Custom] в чат 120 через "
                    f"bot.send_to_peer (fallback)"
                )
        except Exception as e:
            logger.error(
                f"❌ Ошибка отправки custom в чат 120: {e}"
            )

        # 2) Дублируем в чат 7 – через групповой токен
        try:
            self.bot.send_to_peer(
                self.bot.source_peer_id, notif
            )
            logger.info(
                f"📤 [Custom] дублирован в чат группы"
            )
        except Exception as e:
            logger.error(
                f"❌ Ошибка отправки custom в чат группы: {e}"
            )

        # Завершаем триггер
        custom_storage.complete_trigger(
            uid, keep_notification_flag=True
        )
