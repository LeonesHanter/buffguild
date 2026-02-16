# -*- coding: utf-8 -*-
"""
Обработка кастомных триггеров (Ара, Кир).
"""
import re
import time
import logging
import threading
from collections import OrderedDict
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


class TTLCache:
    """Кэш с автоматическим удалением старых записей"""
    def __init__(self, max_size: int = 5000, ttl_seconds: int = 3600):
        self.max_size = max_size
        self.ttl = ttl_seconds
        self.cache = OrderedDict()
        self.timestamps = {}
        self._lock = threading.Lock()
    
    def add(self, key: int) -> None:
        """Добавляет ключ в кэш"""
        with self._lock:
            now = time.time()
            # Удаляем старые записи
            while self.cache and now - self.timestamps[next(iter(self.cache))] > self.ttl:
                oldest = next(iter(self.cache))
                self.cache.pop(oldest)
                del self.timestamps[oldest]
            
            # Добавляем новую
            self.cache[key] = True
            self.timestamps[key] = now
            
            # Ограничиваем размер
            if len(self.cache) > self.max_size:
                oldest = next(iter(self.cache))
                self.cache.pop(oldest)
                del self.timestamps[oldest]
    
    def __contains__(self, key: int) -> bool:
        """Проверяет наличие ключа в кэше"""
        with self._lock:
            if key in self.cache:
                # Обновляем время при обращении
                self.timestamps[key] = time.time()
                return True
            return False
    
    def size(self) -> int:
        """Возвращает текущий размер кэша"""
        with self._lock:
            return len(self.cache)
    
    def clear(self) -> None:
        """Очищает кэш"""
        with self._lock:
            self.cache.clear()
            self.timestamps.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику кэша"""
        with self._lock:
            return {
                'size': len(self.cache),
                'max_size': self.max_size,
                'ttl': self.ttl,
                'oldest': min(self.timestamps.values()) if self.timestamps else None,
                'newest': max(self.timestamps.values()) if self.timestamps else None,
            }


class CustomTriggerParser:
    """Парсер для кастомных триггеров Ара и Кир."""

    def __init__(self):
        self.buff_mappings = {
            'а': 'а', 'ата': 'а', 'атак': 'а', 'атака': 'а', 'атаки': 'а',
            'з': 'з', 'защ': 'з', 'защит': 'з', 'защита': 'з', 'защиты': 'з', 'брон': 'з', 'броня': 'з',
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
        Поддерживает любой порядок: ['а','з','у'] или ['з','а','у'] и т.д.
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
            return ['а', 'з', 'у']  # Всегда атака, защита, удача
        
        # Разбиваем на слова
        words = query.split()
        
        # Проверка на ALL в составе фразы
        if any(cmd in words for cmd in self.all_commands):
            logger.info(f"📋 {trigger.title()} ALL (в тексте): {query}")
            return ['а', 'з', 'у']  # Всегда атака, защита, удача
        
        # Для одиночных команд ищем по словам
        found_buffs = set()
        
        # Сначала ищем по полным словам
        for word in words:
            # Пропускаем очень короткие слова (1 буква) - они будут обработаны позже
            if len(word) <= 1:
                continue
                
            # Ищем совпадение с маппингами
            for pattern, key in self.buff_mappings.items():
                if len(pattern) > 1 and (pattern == word or pattern in word):
                    if key in ['а', 'з', 'у', 'ч', 'э']:
                        logger.info(f"✅ Найден баф {key} по слову '{word}' (паттерн '{pattern}')")
                        found_buffs.add(key)
                        break
        
        # Если ничего не нашли по словам, ищем по отдельным буквам
        if not found_buffs:
            for ch in query:
                if ch in ['а', 'з', 'у', 'ч', 'э']:
                    logger.info(f"✅ Найден баф {ch} по отдельной букве")
                    found_buffs.add(ch)
        
        # Если нашли больше 3-х бафов, ограничиваем только атакой/защитой/удачей
        if len(found_buffs) > 3:
            found_buffs = {k for k in found_buffs if k in ['а', 'з', 'у']}
        
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
        
        logger.info(f"🔍 Парсинг ответа игры:")
        logger.info(f"📄 Полный текст: '{response_text[:200]}...'")
        
        text_lower = response_text.lower()
        
        # Расширенные списки ключевых слов
        attack_patterns = ["атак", "🗡️", "меч", "оружи"]
        defense_patterns = ["защит", "🛡️", "брон", "щит", "броня"]
        luck_patterns = ["удач", "🍀", "везен", "фортун"]
        human_patterns = ["человек", "людей", "🧍"]
        elf_patterns = ["эльф", "🧝"]
        
        # Логируем найденные паттерны
        found_attack = [p for p in attack_patterns if p in text_lower]
        found_defense = [p for p in defense_patterns if p in text_lower]
        found_luck = [p for p in luck_patterns if p in text_lower]
        found_human = [p for p in human_patterns if p in text_lower]
        found_elf = [p for p in elf_patterns if p in text_lower]
        
        if found_attack:
            logger.info(f"✅ Найдены паттерны АТАКИ: {found_attack}")
        if found_defense:
            logger.info(f"✅ Найдены паттерны ЗАЩИТЫ: {found_defense}")
        if found_luck:
            logger.info(f"✅ Найдены паттерны УДАЧИ: {found_luck}")
        if found_human:
            logger.info(f"✅ Найдены паттерны ЧЕЛОВЕКА: {found_human}")
        if found_elf:
            logger.info(f"✅ Найдены паттерны ЭЛЬФА: {found_elf}")
        
        # Определение типа бафа
        buff_type = ""
        
        if found_attack:
            buff_type = "атака"
            logger.info(f"📊 Определен тип: АТАКА")
        elif found_defense:
            buff_type = "защита"
            logger.info(f"📊 Определен тип: ЗАЩИТА")
        elif found_luck:
            buff_type = "удача"
            logger.info(f"📊 Определен тип: УДАЧА")
        elif found_human:
            buff_type = "человек"
            logger.info(f"📊 Определен тип: ЧЕЛОВЕК")
        elif found_elf:
            buff_type = "эльф"
            logger.info(f"📊 Определен тип: ЭЛЬФ")
        
        # Определение критичности и значения
        is_critical = False
        buff_value = 100
        
        # Для защиты и атаки ищем проценты
        if buff_type in ["атака", "защита"]:
            percent_patterns = [
                r"на\s+(\d{1,3})\s*%",
                r"повышена\s+на\s+(\d{1,3})\s*%",
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
                            logger.info(f"📊 Найдено {percent}% - КРИТИЧЕСКИЙ")
                        else:
                            is_critical = False
                            buff_value = 100
                            logger.info(f"📊 Найдено {percent}% - обычный")
                        break
                    except Exception as e:
                        logger.error(f"Ошибка парсинга процентов: {e}")
        
        # Для удачи
        elif buff_type == "удача":
            luck_match = re.search(r"удача\s+повышена\s+на\s+(\d{1,3})", text_lower)
            if luck_match:
                try:
                    luck_val = int(luck_match.group(1))
                    if luck_val >= 9:
                        is_critical = True
                        buff_value = 150
                        logger.info(f"🍀 Удача +{luck_val} (КРИТ)")
                    else:
                        is_critical = False
                        buff_value = 100
                        logger.info(f"🍀 Удача +{luck_val} (обычный)")
                except Exception:
                    pass
        
        # Проверка на критический баф по эмодзи
        if "критический" in text_lower or "🍀" in response_text:
            if not is_critical:
                is_critical = True
                buff_value = 150
                logger.info(f"🍀 Критический баф определен по эмодзи/тексту!")
        
        logger.info(f"📊 Итог: тип={buff_type}, крит={is_critical}, значение={buff_value}")
        return is_critical, buff_value, buff_type

    def extract_voices_from_response(self, response_text: str) -> Optional[int]:
        """Извлекает количество голосов из ответа игры"""
        if not response_text:
            return None
        
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
            else:
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
    _lock = threading.RLock()  # Используем RLock вместо Lock для предотвращения deadlock'ов

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance.pending_triggers = {}
                cls._instance.responses = {}
                # Используем TTL-кэши для processed сообщений
                cls._instance.processed_msgs_cache = TTLCache(max_size=5000, ttl_seconds=3600)
                cls._instance.processed_cmids_cache = TTLCache(max_size=5000, ttl_seconds=3600)
                cls._instance.notification_sent = set()
                cls._instance.recent_commands = {}
            return cls._instance

    def register_trigger(self, user_id: int, trigger: str, executor_id: int, buff_keys: List[str]) -> bool:
        """
        Регистрирует новый триггер для пользователя.
        Возвращает True, если триггер зарегистрирован.
        """
        with self._lock:
            # Проверяем, есть ли уже активный триггер
            if user_id in self.pending_triggers:
                age = time.time() - self.pending_triggers[user_id]['timestamp']
                logger.info(f"⏳ У user_id={user_id} уже есть активный триггер (возраст {age:.1f}с), ожидаем ответа...")
                return True
            
            # СБРАСЫВАЕМ ФЛАГ УВЕДОМЛЕНИЯ при новой регистрации
            if user_id in self.notification_sent:
                logger.info(f"🔄 Сброс флага notification_sent для user_id={user_id} (новая команда)")
                self.notification_sent.discard(user_id)
            
            self.pending_triggers[user_id] = {
                'trigger': trigger,
                'executor_id': executor_id,
                'buff_keys': buff_keys,
                'timestamp': time.time(),
                'responses': []
            }
            self.responses[user_id] = []
            logger.info(f"📝 Зарегистрирован триггер для user_id={user_id}, бафы={buff_keys}")
            return True

    def add_response(self, user_id: int, buff: CustomBuff) -> Tuple[bool, bool]:
        """
        Добавляет ответ от игры для пользователя.
        Возвращает (all_collected, should_notify)
        """
        with self._lock:
            # Проверяем, не отправлено ли уже уведомление
            if user_id in self.notification_sent:
                logger.debug(f"⏭️ Уведомление уже отправлено для user_id={user_id}, игнорируем новый баф {buff.buff_key}")
                return False, False
            
            # Проверяем наличие активного триггера
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
            
            # Обновляем responses для обратной совместимости
            if user_id not in self.responses:
                self.responses[user_id] = []
            self.responses[user_id].append(buff)

            current_count = len(trigger_data['responses'])
            logger.debug(f"📊 После добавления: current_count={current_count}, expected={expected_count}")
            
            all_collected = current_count >= expected_count
            should_notify = all_collected and user_id not in self.notification_sent

            if should_notify:
                self.notification_sent.add(user_id)
                logger.info(f"🎉 СОБРАНЫ ВСЕ {expected_count} БАФОВ для user_id={user_id}!")
            
            return all_collected, should_notify

    def get_trigger_data(self, user_id: int) -> Optional[Dict]:
        """Возвращает данные триггера для пользователя"""
        with self._lock:
            data = self.pending_triggers.get(user_id)
            if data:
                # Возвращаем копию, чтобы избежать изменений извне
                return {
                    'trigger': data['trigger'],
                    'executor_id': data['executor_id'],
                    'buff_keys': data['buff_keys'].copy(),
                    'timestamp': data['timestamp'],
                    'responses': data['responses'].copy()
                }
            return None

    def get_responses(self, user_id: int) -> List[CustomBuff]:
        """Возвращает список ответов для пользователя"""
        with self._lock:
            return self.responses.get(user_id, []).copy()

    def has_notification_been_sent(self, user_id: int) -> bool:
        """Проверяет, было ли уже отправлено уведомление"""
        with self._lock:
            return user_id in self.notification_sent

    def complete_trigger(self, user_id: int, keep_notification_flag: bool = True) -> Optional[Dict]:
        """Завершает триггер и удаляет данные пользователя"""
        with self._lock:
            data = self.pending_triggers.pop(user_id, None)
            self.responses.pop(user_id, None)
            
            # Важно: НЕ удаляем notification_sent если keep_notification_flag=True
            # Это предотвращает повторные уведомления для одного и того же триггера
            if not keep_notification_flag:
                self.notification_sent.discard(user_id)
                logger.debug(f"🗑️ Удален флаг notification_sent для user_id={user_id}")
            
            if data:
                age = time.time() - data['timestamp']
                logger.info(f"🗑️ Триггер завершен для user_id={user_id} (возраст {age:.1f}с)")
            
            return data

    def is_msg_processed(self, msg_id: int, cmid: int = 0) -> bool:
        """Проверяет, было ли сообщение уже обработано"""
        if cmid > 0:
            return cmid in self.processed_cmids_cache
        return msg_id in self.processed_msgs_cache

    def mark_msg_processed(self, msg_id: int, cmid: int = 0):
        """Отмечает сообщение как обработанное"""
        if cmid > 0:
            self.processed_cmids_cache.add(cmid)
        else:
            self.processed_msgs_cache.add(msg_id)

    def cleanup_old_triggers(self, max_age: float = 300.0):
        """
        Очищает старые триггеры (по умолчанию 5 минут).
        НЕ отправляет уведомления, только очищает данные.
        """
        with self._lock:
            now = time.time()
            expired = []
            
            for user_id, data in list(self.pending_triggers.items()):
                age = now - data['timestamp']
                if age > max_age:
                    expired.append((user_id, age, data))

            for user_id, age, data in expired:
                logger.info(f"🧹 Очистка устаревшего триггера для user_id={user_id} (возраст {age:.1f}с)")
                self.pending_triggers.pop(user_id, None)
                self.responses.pop(user_id, None)

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
        # Собираем данные под блокировкой
        with self._lock:
            now = time.time()
            expired_data = []  # Будем хранить копии данных для внешних вызовов
            
            for user_id, data in list(self.pending_triggers.items()):
                if user_id in self.notification_sent:
                    continue
                    
                age = now - data['timestamp']
                if age > max_age:
                    # Копируем данные для внешнего вызова
                    expired_data.append({
                        'user_id': user_id,
                        'age': age,
                        'data': {
                            'trigger': data['trigger'],
                            'executor_id': data['executor_id'],
                            'buff_keys': data['buff_keys'].copy(),
                            'timestamp': data['timestamp'],
                            'responses': data['responses'].copy()
                        }
                    })
                    self.notification_sent.add(user_id)
        
        # Вызываем callback БЕЗ блокировки
        notified_users = []
        for item in expired_data:
            user_id = item['user_id']
            data = item['data']
            try:
                if callback:
                    callback(user_id, data)
                notified_users.append(user_id)
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
                'processed_msgs': self.processed_msgs_cache.size(),
                'processed_cmids': self.processed_cmids_cache.size(),
                'processed_msgs_stats': self.processed_msgs_cache.get_stats(),
                'processed_cmids_stats': self.processed_cmids_cache.get_stats(),
            }

    def get_user_state(self, user_id: int) -> Dict[str, Any]:
        """Возвращает состояние пользователя для отладки"""
        with self._lock:
            return {
                'has_pending': user_id in self.pending_triggers,
                'has_responses': user_id in self.responses,
                'notification_sent': user_id in self.notification_sent,
                'pending_data': self.pending_triggers.get(user_id),
                'responses_count': len(self.responses.get(user_id, [])),
            }

    def reset_user_state(self, user_id: int):
        """Сброс состояния пользователя для тестирования"""
        with self._lock:
            self.pending_triggers.pop(user_id, None)
            self.responses.pop(user_id, None)
            self.notification_sent.discard(user_id)
            logger.info(f"🔄 Сброшено состояние для user_id={user_id}")


# Создаем глобальные экземпляры
custom_parser = CustomTriggerParser()
custom_storage = CustomTriggerStorage()
