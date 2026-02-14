# -*- coding: utf-8 -*-
"""
Обработка кастомных триггеров (Ара, Кир).
"""
import re
import time
import logging
import threading
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from .regexes import RE_VOICES_GENERIC, RE_VOICES_ANY, RE_VOICES_IN_PARENTHESES

logger = logging.getLogger(__name__)


@dataclass
class CustomBuff:
    """Структура бафа для кастомных триггеров"""
    trigger: str
    buff_key: str
    buff_name: str
    is_critical: bool = False
    buff_value: int = 100
    full_response: str = ""
    user_id: int = 0
    executor_id: int = 0
    timestamp: float = 0.0


class CustomTriggerParser:
    """Парсер для кастомных триггеров Ара и Кир."""

    def __init__(self):
        self.buff_mappings = {
            'а': 'а', 'ата': 'а', 'атак': 'а', 'атака': 'а', 'атаки': 'а',
            'з': 'з', 'защ': 'з', 'защит': 'з', 'защита': 'з', 'защиты': 'з',
            'у': 'у', 'уд': 'у', 'удач': 'у', 'удача': 'у', 'удачи': 'у',
            'ч': 'ч', 'чел': 'ч', 'челов': 'ч', 'человек': 'ч', 'люди': 'ч', 'людей': 'ч',
            'э': 'э', 'эльф': 'э', 'эльфа': 'э', 'эльфов': 'э',
        }
        self.all_commands = ['все', 'всего', 'всё']
        self.allowed_races = ['ч', 'э']
        self.buff_names = {
            'а': 'Атака', 'з': 'Защита', 'у': 'Удача', 'ч': 'Человек', 'э': 'Эльф',
        }
        self.buff_emojis = {
            'а': '🗡️', 'з': '🛡️', 'у': '🍀', 'ч': '🧍', 'э': '🧝',
        }
        self.sort_order = {'а': 1, 'з': 2, 'у': 3, 'ч': 4, 'э': 5}
        self.VK_URL = "https://vk.ru/id"

    def parse_command(self, text: str) -> Tuple[Optional[str], Optional[str]]:
        """Парсит сообщение на наличие команд Ара или Кир"""
        if not text:
            return None, None
        text_lower = text.lower().strip()
        if text_lower.startswith('ара'):
            return 'ара', text_lower[3:].strip()
        elif text_lower.startswith('кир'):
            return 'кир', text_lower[3:].strip()
        return None, None

    def parse_buff_query(self, trigger: str, query: str) -> List[str]:
        """
        Парсит запрос пользователя и возвращает список ключей бафов.
        Теперь с гибким поиском паттернов в любом месте строки.
        """
        if not query:
            logger.warning(f"⚠️ Пустой запрос для {trigger}")
            return []
        
        # Приводим к нижнему регистру и удаляем лишние пробелы
        query = query.lower().strip()
        logger.info(f"🔍 Парсинг запроса {trigger}: '{query}'")
        
        # Проверка на ALL-команды
        if query in self.all_commands:
            logger.info(f"📋 {trigger.title()} ALL: {query}")
            return ['а', 'з', 'у']
        
        # Проверяем, есть ли слово "все" в запросе (для фраз типа "дай все")
        words = query.split()
        if any(cmd in words for cmd in self.all_commands):
            logger.info(f"📋 {trigger.title()} ALL (в тексте): {query}")
            return ['а', 'з', 'у']
        
        # Сортируем паттерны по длине (от длинных к коротким)
        # чтобы "защиты" ловилось до "защ", "атака" до "ата" и т.д.
        sorted_patterns = sorted(self.buff_mappings.items(), 
                               key=lambda x: len(x[0]), 
                               reverse=True)
        
        found_buffs = set()
        
        # Ищем каждый паттерн в запросе
        for pattern, key in sorted_patterns:
            # Пропускаем уже найденные бафы
            if key in found_buffs:
                continue
                
            # Различные варианты совпадения
            if (pattern == query or  # точное совпадение
                query.startswith(pattern + ' ') or  # в начале с пробелом после
                query.endswith(' ' + pattern) or  # в конце с пробелом перед
                ' ' + pattern + ' ' in query or  # в середине
                query.startswith(pattern) or  # начинается с паттерна
                pattern in query):  # паттерн есть где-то в строке
                
                logger.info(f"✅ Найден баф {key} по паттерну '{pattern}'")
                
                if key in ['а', 'з', 'у', 'ч', 'э']:
                    found_buffs.add(key)
                else:
                    logger.warning(f"❌ Баф {key} не разрешен для {trigger}")
                    return []
        
        # Если ничего не нашли, пробуем найти по отдельным словам
        if not found_buffs:
            for word in words:
                for pattern, key in sorted_patterns:
                    if pattern in word or word in pattern:
                        if key in ['а', 'з', 'у', 'ч', 'э']:
                            logger.info(f"✅ Найден баф {key} по слову '{word}'")
                            found_buffs.add(key)
                            break
        
        result = list(found_buffs)
        if result:
            logger.info(f"✅ Итоговые бафы для {trigger}: {result}")
        else:
            logger.warning(f"❌ Бафы не найдены в запросе: '{query}'")
        
        return result

    def parse_game_response(self, response_text: str) -> Tuple[bool, int, str]:
        """
        Парсит ответ игры после использования бафа.
        Определяет тип бафа, критичность и значение.
        """
        if not response_text:
            return False, 100, ""
        
        logger.info(f"🔍 Парсинг ответа игры: '{response_text[:100]}...'")
        text_lower = response_text.lower()
        is_critical = False
        buff_value = 100
        buff_type = ""

        # Определение типа бафа по ключевым словам
        if any(word in text_lower for word in ["атак"]):
            buff_type = "атака"
            logger.info("📊 Определен тип: атака")
        elif any(word in text_lower for word in ["защит"]):
            buff_type = "защита"
            logger.info("📊 Определен тип: защита")
        elif any(word in text_lower for word in ["удач"]):
            buff_type = "удача"
            logger.info("📊 Определен тип: удача")
        elif any(word in text_lower for word in ["человек", "людей"]):
            buff_type = "человек"
            logger.info("📊 Определен тип: человек")
        elif any(word in text_lower for word in ["эльф"]):
            buff_type = "эльф"
            logger.info("📊 Определен тип: эльф")

        # Проверка на критический баф
        if "критический" in text_lower or "🍀" in response_text:
            is_critical = True
            buff_value = 150
            logger.info(f"🍀 Критический баф!")

        # Поиск процентов для атаки/защиты
        if buff_type in ["атака", "защита"]:
            percent_patterns = [
                r"повышена\s+на\s+(\d{1,3})\s*%",
                r"на\s+(\d{1,3})\s*%",
                r"(\+?\d{1,3})\s*%"
            ]
            for pattern in percent_patterns:
                match = re.search(pattern, text_lower)
                if match:
                    try:
                        percent = int(match.group(1))
                        if percent >= 30:
                            is_critical = True
                            buff_value = 150
                            logger.info(f"📊 Найдено {percent}% - критический")
                        else:
                            is_critical = False
                            buff_value = 100
                            logger.info(f"📊 Найдено {percent}% - обычный")
                        break
                    except Exception as e:
                        logger.error(f"Ошибка парсинга процентов: {e}")

        # Поиск значения для удачи
        if buff_type == "удача":
            luck_match = re.search(r"удача\s+повышена\s+на\s+(\d{1,3})", text_lower)
            if luck_match:
                try:
                    luck_val = int(luck_match.group(1))
                    if luck_val >= 9:
                        is_critical = True
                        buff_value = 150
                        logger.info(f"🍀 Удача +{luck_val} (крит)")
                    else:
                        is_critical = False
                        buff_value = 100
                        logger.info(f"🍀 Удача +{luck_val} (обычный)")
                except Exception as e:
                    logger.error(f"Ошибка парсинга удачи: {e}")

        logger.info(f"📊 Результат парсинга: крит={is_critical}, значение={buff_value}, тип={buff_type}")
        return is_critical, buff_value, buff_type

    def extract_voices_from_response(self, response_text: str) -> Optional[int]:
        """Извлекает количество голосов из ответа игры"""
        if not response_text:
            return None
        
        # Пробуем разные регулярные выражения
        vm = RE_VOICES_GENERIC.search(response_text)
        if vm:
            try:
                return int(vm.group(1))
            except Exception:
                pass
        
        vm = RE_VOICES_ANY.search(response_text)
        if vm:
            try:
                return int(vm.group(1))
            except Exception:
                pass
        
        vm = RE_VOICES_IN_PARENTHESES.search(response_text)
        if vm:
            try:
                return int(vm.group(1))
            except Exception:
                pass
        
        return None

    def format_notification(self, trigger: str, user_id: int, executor_id: int,
                          buffs: List[CustomBuff]) -> str:
        """Форматирует уведомление о выдаче бафов"""
        lines = ["🎉 Баф успешно выдан!"]
        
        # Сортируем бафы для красивого отображения
        sorted_buffs = sorted(buffs, key=lambda x: self.sort_order.get(x.buff_key, 99))
        total_cost = 0

        for buff in sorted_buffs:
            executor_link = f"[{self.VK_URL}{executor_id}|{self.buff_emojis.get(buff.buff_key, '✨')}]"

            # Форматирование в зависимости от типа бафа
            if buff.buff_key in ['а', 'з']:
                if buff.is_critical:
                    value = f"+30%!🍀"
                else:
                    value = f"+20%!"
                line = f"{executor_link}{buff.buff_name} {value}"
            elif buff.buff_key == 'у':
                if buff.is_critical:
                    value = f"+9!🍀"
                else:
                    value = f"+6!"
                line = f"{executor_link}{buff.buff_name} {value}"
            else:  # расовые бафы
                if buff.is_critical:
                    line = f"{executor_link}{buff.buff_name}!🍀"
                else:
                    line = f"{executor_link}{buff.buff_name}!"

            lines.append(line)
            total_cost += buff.buff_value

        user_link = f"[{self.VK_URL}{user_id}|💰]"
        lines.append(f"{user_link}Списано {total_cost} баллов")
        
        return "\n".join(lines)


class CustomTriggerStorage:
    """Общее хранилище для кастомных триггеров между потоками."""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance.pending_triggers = {}
                cls._instance.responses = {}
                cls._instance.processed_msgs = set()
                cls._instance.processed_cmids = set()
                cls._instance.notification_sent = set()  # Множество user_id, для которых уже отправлено уведомление
            return cls._instance

    def register_trigger(self, user_id: int, trigger: str, executor_id: int, buff_keys: List[str]):
        """Регистрирует новый триггер для пользователя"""
        with self._lock:
            # Убеждаемся, что для этого user_id нет активного триггера
            if user_id in self.pending_triggers:
                logger.warning(f"⚠️ Перезапись существующего триггера для user_id={user_id}")
                self.complete_trigger(user_id)
            
            self.pending_triggers[user_id] = {
                'trigger': trigger,
                'executor_id': executor_id,
                'buff_keys': buff_keys,
                'timestamp': time.time(),
                'responses': []
            }
            self.responses[user_id] = []
            # НЕ удаляем notification_sent при регистрации нового триггера!
            # Это гарантирует, что для одного user_id уведомление отправится только один раз
            logger.info(f"📝 Зарегистрирован триггер для user_id={user_id}, бафы={buff_keys}")

    def add_response(self, user_id: int, buff: CustomBuff) -> Tuple[bool, bool]:
        """
        Добавляет ответ от игры для пользователя.
        Возвращает (all_collected, should_notify)
        """
        with self._lock:
            # Если уведомление уже было отправлено для этого user_id, игнорируем все новые ответы
            if user_id in self.notification_sent:
                logger.debug(f"⏭️ Уведомление уже отправлено для user_id={user_id}, игнорируем новый баф {buff.buff_key}")
                return False, False
            
            if user_id not in self.pending_triggers:
                logger.debug(f"⚠️ Нет активного триггера для user_id={user_id}")
                return False, False

            trigger_data = self.pending_triggers[user_id]
            expected_count = len(trigger_data['buff_keys'])

            # Проверка на дубликаты
            for existing in trigger_data['responses']:
                if existing.buff_key == buff.buff_key:
                    logger.debug(f"⏭️ Дубль бафа {buff.buff_key} для user_id={user_id}")
                    return False, False

            # Добавляем ответ
            trigger_data['responses'].append(buff)
            self.responses[user_id].append(buff)

            current_count = len(trigger_data['responses'])
            all_collected = current_count >= expected_count
            should_notify = all_collected and user_id not in self.notification_sent

            if should_notify:
                self.notification_sent.add(user_id)
                logger.info(f"🎉 СОБРАНЫ ВСЕ {expected_count} БАФОВ для user_id={user_id}!")

            logger.debug(f"✅ Добавлен ответ {buff.buff_key} для user_id={user_id} ({current_count}/{expected_count})")
            return all_collected, should_notify

    def get_trigger_data(self, user_id: int) -> Optional[Dict]:
        """Возвращает данные триггера для пользователя"""
        with self._lock:
            return self.pending_triggers.get(user_id)

    def get_responses(self, user_id: int) -> List[CustomBuff]:
        """Возвращает список ответов для пользователя"""
        with self._lock:
            return self.responses.get(user_id, [])

    def has_notification_been_sent(self, user_id: int) -> bool:
        """Проверяет, было ли уже отправлено уведомление для user_id"""
        with self._lock:
            return user_id in self.notification_sent

    def force_send_notification(self, user_id: int, reason: str = "timeout") -> bool:
        """
        Принудительно помечает, что уведомление отправлено.
        Используется при таймауте или других принудительных отправках.
        Возвращает True, если уведомление ещё не было отправлено.
        """
        with self._lock:
            if user_id in self.notification_sent:
                logger.debug(f"⏭️ Уведомление уже было отправлено для user_id={user_id}, пропускаем принудительную отправку")
                return False
            
            logger.info(f"📢 Принудительная отметка об отправке уведомления для user_id={user_id} (причина: {reason})")
            self.notification_sent.add(user_id)
            return True

    def complete_trigger(self, user_id: int, keep_notification_flag: bool = True) -> Optional[Dict]:
        """
        Завершает триггер и удаляет данные пользователя.
        
        Args:
            user_id: ID пользователя
            keep_notification_flag: Если True, сохраняет флаг notification_sent (чтобы не отправлять повторно)
                                    Если False, удаляет флаг (для тестов или принудительной очистки)
        """
        with self._lock:
            data = self.pending_triggers.pop(user_id, None)
            self.responses.pop(user_id, None)
            
            # Не удаляем notification_sent, если keep_notification_flag=True
            # Это гарантирует, что для одного user_id уведомление отправится только один раз
            if not keep_notification_flag:
                self.notification_sent.discard(user_id)
                logger.debug(f"🗑️ Удален флаг notification_sent для user_id={user_id}")
            
            if data:
                age = time.time() - data['timestamp']
                logger.info(f"🗑️ Триггер завершен для user_id={user_id} (возраст {age:.1f}с)")
            
            return data

    def is_msg_processed(self, msg_id: int, cmid: int = 0) -> bool:
        """Проверяет, было ли сообщение уже обработано"""
        with self._lock:
            if cmid > 0:
                return cmid in self.processed_cmids
            return msg_id in self.processed_msgs

    def mark_msg_processed(self, msg_id: int, cmid: int = 0):
        """Отмечает сообщение как обработанное"""
        with self._lock:
            if cmid > 0:
                self.processed_cmids.add(cmid)
                # Ограничиваем размер множества
                if len(self.processed_cmids) > 1000:
                    self.processed_cmids = set(list(self.processed_cmids)[-500:])
            else:
                self.processed_msgs.add(msg_id)
                if len(self.processed_msgs) > 1000:
                    self.processed_msgs = set(list(self.processed_msgs)[-500:])

    def cleanup_old_triggers(self, max_age: float = 300.0):
        """
        Очищает старые триггеры (по умолчанию 5 минут).
        НЕ отправляет уведомления, только очищает данные.
        Уведомления по таймауту должны обрабатываться отдельно.
        """
        with self._lock:
            now = time.time()
            expired = []
            
            for user_id, data in self.pending_triggers.items():
                age = now - data['timestamp']
                if age > max_age:
                    expired.append((user_id, age, data))

            for user_id, age, data in expired:
                logger.info(f"🧹 Очистка устаревшего триггера для user_id={user_id} (возраст {age:.1f}с)")
                
                # При очистке НЕ отправляем уведомление, просто удаляем данные
                # Уведомление по таймауту должно отправляться в отдельном методе check_timeouts
                
                # Завершаем триггер, но сохраняем флаг notification_sent, если он был
                self.pending_triggers.pop(user_id, None)
                self.responses.pop(user_id, None)
                # НЕ удаляем notification_sent

    def check_timeouts_and_notify(self, max_age: float = 300.0, callback=None):
        """
        Проверяет таймауты и вызывает callback для отправки уведомлений.
        
        Args:
            max_age: Максимальный возраст триггера в секундах
            callback: Функция, которая будет вызвана для отправки уведомления.
                     Должна принимать (user_id, trigger_data)
        
        Returns:
            Список user_id, для которых были отправлены уведомления по таймауту
        """
        notified_users = []
        
        with self._lock:
            now = time.time()
            expired = []
            
            for user_id, data in list(self.pending_triggers.items()):
                # Пропускаем, если уведомление уже отправлено
                if user_id in self.notification_sent:
                    continue
                    
                age = now - data['timestamp']
                if age > max_age:
                    expired.append((user_id, age, data))
            
            # Отмечаем как отправленные и вызываем callback
            for user_id, age, data in expired:
                logger.warning(f"⏰ ТАЙМАУТ для user_id={user_id} (возраст {age:.1f}с, собрано {len(data['responses'])}/{len(data['buff_keys'])} бафов)")
                
                # Помечаем, что уведомление отправлено
                self.notification_sent.add(user_id)
                notified_users.append(user_id)
                
                # Вызываем callback для отправки уведомления, если он предоставлен
                if callback:
                    try:
                        # Выходим из блокировки перед вызовом callback
                        self._lock.release()
                        try:
                            callback(user_id, data)
                        finally:
                            self._lock.acquire()
                    except Exception as e:
                        logger.error(f"Ошибка в callback для user_id={user_id}: {e}")
        
        return notified_users

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику хранилища"""
        with self._lock:
            return {
                'pending_triggers': len(self.pending_triggers),
                'total_responses': sum(len(r) for r in self.responses.values()),
                'notification_sent': len(self.notification_sent),
                'processed_msgs': len(self.processed_msgs),
                'processed_cmids': len(self.processed_cmids)
            }


# Создаем глобальные экземпляры
custom_parser = CustomTriggerParser()
custom_storage = CustomTriggerStorage()
