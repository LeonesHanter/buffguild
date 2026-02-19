# -*- coding: utf-8 -*-
import logging
import re
import threading
import time
from typing import List, Optional

from .custom_triggers import trigger_store

logger = logging.getLogger(__name__)


class CustomTriggerHandler:
    """Обработчик Ара/Кир с потоком ожидания до 315 секунд"""

    def __init__(self, bot):
        self.bot = bot
        self.ARA_ID = 294529251
        self.KIR_ID = 8244449
        
        # Словарь для маппинга текста в ключи бафов
        self.buff_keywords = {
            'а': ['атак', '🗡️', 'меч', 'оружи'],
            'з': ['защит', '🛡️', 'брон', 'щит', 'броня'],
            'у': ['удач', '🍀', 'везен', 'фортун'],
            'ч': ['человек', 'людей', '🧍'],
            'э': ['эльф', '🧝'],
        }
        
        # Словарь названий бафов
        self.buff_names = {
            'а': 'Атака', 'з': 'Защита', 'у': 'Удача', 
            'ч': 'Человек', 'э': 'Эльф'
        }
        
        # Словарь эмодзи
        self.buff_emojis = {
            'а': '🗡️', 'з': '🛡️', 'у': '🍀', 'ч': '🧍', 'э': '🧝'
        }

    def handle_command(self, text: str, from_id: int) -> bool:
        """Обработка команды от пользователя"""
        text_lower = text.lower().strip()
        
        if text_lower.startswith('ара'):
            query = text_lower[3:].strip()
            executor_id = self.ARA_ID
        elif text_lower.startswith('кир'):
            query = text_lower[3:].strip()
            executor_id = self.KIR_ID
        else:
            return False

        # Парсим запрос
        buff_keys = []
        
        # ALL-команда
        if query in ['все', 'всего', 'всё']:
            buff_keys = ['а', 'з', 'у']
        else:
            # Ищем по ключевым словам
            for key, keywords in self.buff_keywords.items():
                if any(kw in query for kw in keywords):
                    if key not in buff_keys:
                        buff_keys.append(key)
            
            # Если ничего не нашли - ищем по отдельным буквам
            if not buff_keys:
                for ch in query:
                    if ch in self.buff_keywords:
                        buff_keys.append(ch)

        if not buff_keys:
            logger.warning(f"❌ Не удалось распарсить запрос: '{query}'")
            return False

        logger.info(f"🎯 команда для {from_id}: {buff_keys} (исполнитель: {executor_id})")

        # Регистрируем триггер
        trigger_index = trigger_store.register_trigger(from_id, buff_keys, executor_id)
        
        # Запускаем поток ожидания
        threading.Thread(
            target=self._wait,
            args=(from_id, len(buff_keys), trigger_index),
            daemon=True
        ).start()
        
        return True

    def handle_game_response(self, msg: dict) -> bool:
        """Обработка ответа от игры"""
        text = msg.get("text", "")
        msg_id = msg.get("id", 0)

        # Пропускаем уже обработанные
        if trigger_store.is_msg_processed(msg_id):
            return False

        # Ищем ID пользователя
        match = re.search(r'\[id(\d+)\|', text)
        if not match:
            return False

        uid = int(match.group(1))
        
        # Определяем тип бафа
        text_lower = text.lower()
        buff_key = None
        
        for key, keywords in self.buff_keywords.items():
            if any(kw in text_lower for kw in keywords):
                buff_key = key
                break

        if not buff_key:
            logger.debug(f"❌ Не удалось определить тип бафа в тексте")
            return False

        # Определяем критичность и значение
        is_critical = "критический" in text_lower or "🍀" in text
        buff_value = 150 if is_critical else 100
        
        # Для атаки/защиты проверяем проценты
        if buff_key in ['а', 'з']:
            percent_patterns = [
                r"на\s+(\d{1,3})\s*%",
                r"повышена\s+на\s+(\d{1,3})\s*%",
                r"увеличена\s+на\s+(\d{1,3})\s*%",
                r"(\d{1,3})\s*%",
                r"\+(\d{1,3})%"
            ]
            for pattern in percent_patterns:
                match = re.search(pattern, text_lower)
                if match:
                    try:
                        percent = int(match.group(1))
                        if percent >= 30:
                            is_critical = True
                            buff_value = 150
                        break
                    except:
                        pass
        
        # Для удачи проверяем единицы
        elif buff_key == 'у':
            luck_match = re.search(r"удача\s+повышена\s+на\s+(\d{1,3})", text_lower)
            if luck_match:
                try:
                    luck_val = int(luck_match.group(1))
                    if luck_val >= 9:
                        is_critical = True
                        buff_value = 150
                except:
                    pass

        logger.info(f"📩 Ответ игры для {uid}: баф {buff_key}, крит={is_critical}, значение={buff_value}")

        # Ищем активный триггер для этого пользователя
        # В реальности нужно найти правильный индекс, но для простоты будем считать
        # что у пользователя может быть несколько триггеров, и нужно найти подходящий
        # Пока используем заглушку - первый активный
        trigger_index = 0
        
        all_collected, current = trigger_store.add_response(uid, trigger_index, buff_key, is_critical, buff_value)
        trigger_store.mark_msg_processed(msg_id)

        return True

    def _wait(self, uid: int, need: int, trigger_index: int):
        """
        Ожидание ответов от игры.
        - Максимум 315 секунд
        - Проверка каждые 5 секунд
        - При сборе всех бафов - немедленная отправка
        - При таймауте - отправка того, что успели собрать
        """
        max_wait = 315  # 5 минут + 15 секунд запаса
        waited = 0
        interval = 0.5
        check_interval = 5
        last_check = 0
        notification_sent = False

        logger.info(f"⏳ Начато ожидание {need} бафов для user_id={uid} (триггер #{trigger_index}), макс. {max_wait}с")

        while waited < max_wait and not notification_sent:
            time.sleep(interval)
            waited += interval
            now = time.time()

            # Проверяем каждые 5 секунд
            if now - last_check >= check_interval:
                last_check = now
                
                trigger = trigger_store.get_trigger(uid, trigger_index)
                
                if not trigger:
                    logger.debug(f"ℹ️ Триггер #{trigger_index} для {uid} уже завершен")
                    return

                received = len(trigger['responses'])
                logger.info(f"⏳ Ожидание бафов для {uid}: {received}/{need} (прошло {waited:.0f}с)")
                
                # Если собрали все - немедленно отправляем
                if received >= need:
                    logger.info(f"✅ Все {need} ответов получены для {uid} (через {waited:.0f}с)")
                    self._send_notification(uid, trigger_index)
                    notification_sent = True
                    break

        # Таймаут - отправляем то, что успели собрать
        if not notification_sent:
            logger.warning(f"⏰ Таймаут {max_wait}с для user_id={uid}, проверяем собранные бафы")
            
            trigger = trigger_store.get_trigger(uid, trigger_index)
            
            if trigger and trigger['responses']:
                received = len(trigger['responses'])
                logger.info(f"📤 Отправка по таймауту для {uid}: получено {received}/{need}")
                self._send_notification(uid, trigger_index)
            else:
                logger.info(f"🔇 Триггер #{trigger_index} для {uid} без ответов, ничего не отправляем")
                trigger_store.complete_trigger(uid, trigger_index)

    def _send_notification(self, user_id: int, trigger_index: int):
        """Отправляет уведомление для конкретного триггера"""
        responses = trigger_store.get_responses(user_id, trigger_index)
        
        if not responses:
            logger.warning(f"⚠️ Нет данных для уведомления user_id={user_id}, триггер #{trigger_index}")
            return

        # Формируем уведомление
        lines = ["🎉 Баф успешно выдан!"]
        total_cost = 0

        for buff_key, executor_id, is_critical, buff_value in responses:
            executor_link = f"[https://vk.ru/id{executor_id}|{self.buff_emojis.get(buff_key, '✨')}]"
            buff_name = self.buff_names.get(buff_key, 'Баф')

            # Форматируем как в обычном бафере
            if buff_key in ['а', 'з']:
                if is_critical:
                    value = f"+30%!🍀"
                else:
                    value = f"+20%!"
                line = f"{executor_link}{buff_name} {value}"
            elif buff_key == 'у':
                if is_critical:
                    value = f"+9!🍀"
                else:
                    value = f"+6!"
                line = f"{executor_link}{buff_name} {value}"
            else:
                if is_critical:
                    line = f"{executor_link}{buff_name}!🍀"
                else:
                    line = f"{executor_link}{buff_name}!"

            lines.append(line)
            total_cost += buff_value

        lines.append(f"[https://vk.ru/id{user_id}|💰]Списано {total_cost} баллов")
        notif = "\n".join(lines)

        # Отправляем
        try:
            self.bot.send_to_peer(self.bot.source_peer_id, notif)
            logger.info(f"📤 Уведомление отправлено для {user_id} (триггер #{trigger_index}, бафов: {len(responses)})")
        except Exception as e:
            logger.error(f"❌ Ошибка отправки: {e}")

        # Удаляем триггер
        trigger_store.complete_trigger(user_id, trigger_index)
