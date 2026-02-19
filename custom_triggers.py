# -*- coding: utf-8 -*-
"""
Простое хранилище для триггеров Ара/Кир
"""
import time
import logging
import threading
from typing import Dict, List, Optional, Tuple, Any

logger = logging.getLogger(__name__)


class SimpleTriggerStore:
    """Хранилище триггеров без таймеров - только данные"""
    
    def __init__(self):
        self._lock = threading.RLock()
        # Структура: {user_id: [триггер1, триггер2, ...]}
        self._triggers: Dict[int, List[Dict]] = {}
        self._processed_msgs = set()
        self._max_processed = 10000

    def register_trigger(self, user_id: int, buff_keys: List[str], executor_id: int) -> int:
        """
        Регистрирует новый триггер для пользователя.
        Возвращает индекс триггера.
        """
        with self._lock:
            if user_id not in self._triggers:
                self._triggers[user_id] = []
            
            trigger = {
                'buff_keys': buff_keys.copy(),
                'responses': [],  # список полученных бафов
                'responses_full': [],  # список кортежей (buff_key, is_critical, buff_value)
                'completed': False,
                'created_at': time.time(),
                'executor_id': executor_id
            }
            self._triggers[user_id].append(trigger)
            trigger_index = len(self._triggers[user_id]) - 1
            
            logger.info(f"📝 Триггер #{trigger_index} для {user_id}: {buff_keys} (исполнитель: {executor_id})")
            return trigger_index

    def add_response(self, user_id: int, trigger_index: int, buff_key: str, is_critical: bool = False, buff_value: int = 100) -> Tuple[bool, int]:
        """
        Добавляет полученный баф к триггеру.
        Возвращает (все_собраны, текущее_количество)
        """
        with self._lock:
            if user_id not in self._triggers:
                logger.debug(f"⚠️ Нет триггеров для {user_id}")
                return False, 0
            
            if trigger_index >= len(self._triggers[user_id]):
                logger.debug(f"⚠️ Нет триггера #{trigger_index} для {user_id}")
                return False, 0
            
            trigger = self._triggers[user_id][trigger_index]
            
            if trigger['completed']:
                logger.debug(f"⏭️ Триггер #{trigger_index} уже завершён")
                return False, len(trigger['responses'])
            
            # Проверка на дубликат
            if buff_key in trigger['responses']:
                logger.debug(f"⏭️ Дубль бафа {buff_key} в триггере #{trigger_index}")
                return False, len(trigger['responses'])
            
            # Добавляем ответ
            trigger['responses'].append(buff_key)
            trigger['responses_full'].append((buff_key, is_critical, buff_value))
            
            current = len(trigger['responses'])
            total = len(trigger['buff_keys'])
            
            crit_str = "КРИТ" if is_critical else "обычный"
            logger.info(f"✅ Триггер #{trigger_index}: получен {buff_key} ({current}/{total}) [{crit_str}, {buff_value}]")
            
            all_collected = current >= total
            if all_collected:
                trigger['completed'] = True
                logger.info(f"🎉 Триггер #{trigger_index} для {user_id} полностью собран!")
            
            return all_collected, current

    def get_trigger(self, user_id: int, trigger_index: int) -> Optional[Dict]:
        """Возвращает данные триггера"""
        with self._lock:
            if user_id not in self._triggers:
                return None
            if trigger_index >= len(self._triggers[user_id]):
                return None
            return self._triggers[user_id][trigger_index].copy()

    def get_responses(self, user_id: int, trigger_index: int) -> List[Tuple[str, int, bool, int]]:
        """
        Возвращает список полученных бафов в формате (buff_key, executor_id, is_critical, buff_value)
        """
        with self._lock:
            if user_id not in self._triggers:
                return []
            if trigger_index >= len(self._triggers[user_id]):
                return []
            
            trigger = self._triggers[user_id][trigger_index]
            executor_id = trigger['executor_id']
            
            return [(key, executor_id, crit, val) for key, crit, val in trigger['responses_full']]

    def complete_trigger(self, user_id: int, trigger_index: int):
        """Удаляет триггер"""
        with self._lock:
            if user_id in self._triggers:
                if trigger_index < len(self._triggers[user_id]):
                    self._triggers[user_id].pop(trigger_index)
                    logger.info(f"🗑️ Триггер #{trigger_index} для {user_id} удалён")
                    
                    # Если не осталось триггеров - удаляем пользователя
                    if not self._triggers[user_id]:
                        del self._triggers[user_id]
                        logger.info(f"🗑️ Пользователь {user_id} удалён из хранилища")

    def is_msg_processed(self, msg_id: int) -> bool:
        return msg_id in self._processed_msgs

    def mark_msg_processed(self, msg_id: int):
        self._processed_msgs.add(msg_id)
        if len(self._processed_msgs) > self._max_processed:
            self._processed_msgs = set(list(self._processed_msgs)[-5000:])


# Создаём глобальный экземпляр
trigger_store = SimpleTriggerStore()
custom_storage = trigger_store  # для обратной совместимости
