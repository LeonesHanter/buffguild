# -*- coding: utf-8 -*-
"""
Voice Prophet - интеллектуальный предсказатель голосов.

Вместо проверки "каждые 30 минут" проверяет "когда реально нужно".
Анализирует историю расходов и предсказывает момент обнуления.
"""
import json
import time
import logging
import os
from collections import deque
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class VoiceSpendEvent:
    """Событие расхода голоса (успешный баф)"""
    timestamp: float
    voices_before: int
    voices_after: int
    spent: int = 1


@dataclass
class VoiceCheckEvent:
    """Событие проверки профиля (Мой профиль)"""
    timestamp: float
    voices_found: int
    was_predicted: bool = False
    prediction_error: Optional[float] = None


class VoiceProphet:
    """
    Предсказатель голосов для одного токена.
    
    Принцип работы:
    1. Запоминает каждый успешный баф (spend_voice -> record_spend)
    2. Запоминает каждую проверку профиля (update_voices_from_system -> record_check)
    3. Рассчитывает средний расход в час
    4. Предсказывает время обнуления голосов
    5. Сигнализирует, когда нужно проверить профиль
    """
    
    def __init__(self, token, storage_dir: str = "data/voice_prophet"):
        self.token = token
        self.token_id = token.id
        self.token_name = token.name
        
        # Директория для хранения истории
        self.storage_dir = storage_dir
        self.storage_path = f"{storage_dir}/voice_prophet_{self.token_id}.json"
        
        # История расходов (последние 100)
        self.spend_history: deque[VoiceSpendEvent] = deque(maxlen=100)
        
        # История проверок (последние 50)
        self.check_history: deque[VoiceCheckEvent] = deque(maxlen=50)
        
        # Кэш предсказаний
        self._last_prediction: Optional[Tuple[float, float]] = None  # (timestamp, predicted_zero_at)
        self._prediction_confidence: float = 0.0
        
        # Статистика
        self.total_spent: int = 0
        self.total_checks: int = 0
        self.successful_predictions: int = 0
        self.last_spend_time: float = 0
        self.last_check_time: float = 0
        
        # Параметры (настраиваются)
        self.MIN_CHECK_INTERVAL = 15 * 60  # 15 минут (не чаще)
        self.MAX_CHECK_INTERVAL = 4 * 60 * 60  # 4 часа (не реже)
        self.PREDICTION_LEAD_TIME = 15 * 60  # проверяем за 15 минут до
        self.CRITICAL_VOICES = 3  # критический уровень голосов
        self.REQUIRED_HISTORY_SIZE = 5  # минимум событий для предсказания
        
        # Создаём директорию, если нет
        os.makedirs(storage_dir, exist_ok=True)
        
        # Загружаем историю
        self._load_history()
        
        logger.debug(f"🔮 VoiceProphet инициализирован для {self.token_name}")
    
    # ============= ЗАПИСЬ РАСХОДА =============
    def record_spend(self, voices_before: int) -> None:
        """
        Записать факт расхода голоса.
        Вызывается ТОЛЬКО из TokenHandler.spend_voice()
        
        Args:
            voices_before: Количество голосов ДО списания
        """
        voices_after = max(0, voices_before - 1)
        
        event = VoiceSpendEvent(
            timestamp=time.time(),
            voices_before=voices_before,
            voices_after=voices_after
        )
        
        self.spend_history.append(event)
        self.total_spent += 1
        self.last_spend_time = event.timestamp
        
        # Сброс кэша предсказания
        self._last_prediction = None
        
        # Сохраняем историю
        self._save_history()
        
        logger.debug(f"  💰 {self.token_name}: record_spend({voices_before}→{voices_after})")
    # ===========================================
    
    # ============= ЗАПИСЬ ПРОВЕРКИ =============
    def record_check(self, voices_found: int, predicted_zero_at: Optional[float] = None) -> None:
        """
        Записать факт проверки профиля.
        Вызывается ТОЛЬКО из TokenHandler.update_voices_from_system()
        
        Args:
            voices_found: Актуальное количество голосов из профиля
            predicted_zero_at: Предсказанное время обнуления (если было)
        """
        self.total_checks += 1
        self.last_check_time = time.time()
        
        was_predicted = False
        prediction_error = None
        
        if predicted_zero_at:
            # Оцениваем точность предсказания
            prediction_error = (time.time() - predicted_zero_at) / 3600  # в часах
            was_predicted = abs(prediction_error) < 1.0  # ошибка менее часа
            
            if was_predicted:
                self.successful_predictions += 1
        
        event = VoiceCheckEvent(
            timestamp=time.time(),
            voices_found=voices_found,
            was_predicted=was_predicted,
            prediction_error=prediction_error
        )
        
        self.check_history.append(event)
        self._update_confidence()
        self._save_history()
        
        logger.debug(f"  📊 {self.token_name}: record_check(voices={voices_found})")
    # ===========================================
    
    # ============= ПРЕДСКАЗАНИЕ =============
    def predict_zero_at(self) -> Optional[float]:
        """
        Предсказать время обнуления голосов.
        
        Returns:
            timestamp когда голоса станут 0, или None если недостаточно данных
        """
        # Если уже 0 голосов
        if self.token.voices <= 0:
            return time.time()
        
        # Недостаточно данных
        if len(self.spend_history) < self.REQUIRED_HISTORY_SIZE:
            logger.debug(f"  ⏳ {self.token_name}: недостаточно данных для предсказания ({len(self.spend_history)}/{self.REQUIRED_HISTORY_SIZE})")
            return None
        
        # Берём историю за последние 24 часа
        day_ago = time.time() - 86400
        recent_spends = [e for e in self.spend_history if e.timestamp > day_ago]
        
        if len(recent_spends) < 3:
            logger.debug(f"  ⏳ {self.token_name}: недостаточно данных за последние 24ч")
            return None
        
        # Расчёт среднего расхода
        first_event = recent_spends[0]
        last_event = recent_spends[-1]
        
        time_span_hours = (last_event.timestamp - first_event.timestamp) / 3600
        if time_span_hours < 0.1:  # менее 6 минут
            time_span_hours = 0.1
        
        spend_count = len(recent_spends)
        spend_rate = spend_count / time_span_hours  # голосов в час
        
        if spend_rate <= 0:
            return None
        
        # Корректировка по времени суток
        current_hour = datetime.now().hour
        if 19 <= current_hour <= 23:  # вечерний прайм
            spend_rate *= 1.5
        elif 0 <= current_hour <= 6:   # ночной спад
            spend_rate *= 0.3
        
        # Корректировка по дню недели
        if datetime.now().weekday() >= 5:  # выходные
            spend_rate *= 1.3
        
        # Предсказание
        hours_left = self.token.voices / spend_rate
        zero_at = time.time() + (hours_left * 3600)
        
        # Кэшируем
        self._last_prediction = (time.time(), zero_at)
        
        logger.debug(
            f"  🔮 {self.token_name}: {self.token.voices} голосов хватит на "
            f"{hours_left:.1f}ч (расход {spend_rate:.2f}/ч)"
        )
        
        return zero_at
    # =========================================
    
    # ============= РЕШЕНИЕ О ПРОВЕРКЕ =============
    def should_check_profile(self) -> bool:
        """
        Определить, нужно ли проверять профиль прямо сейчас.
        
        Returns:
            True если пора проверить профиль
        """
        # 1. Критический уровень голосов
        if self.token.voices <= self.CRITICAL_VOICES:
            logger.debug(f"  ⚠️ {self.token_name}: критический уровень голосов ({self.token.voices})")
            return True
        
        # 2. Не проверяем чаще 15 минут
        if time.time() - self.last_check_time < self.MIN_CHECK_INTERVAL:
            return False
        
        # 3. Не проверяем реже 4 часов (подстраховка)
        if time.time() - self.last_check_time > self.MAX_CHECK_INTERVAL:
            logger.debug(f"  ⏰ {self.token_name}: плановая проверка (прошло >4ч)")
            return True
        
        # 4. Предсказание
        zero_at = self.predict_zero_at()
        if zero_at:
            time_to_zero = zero_at - time.time()
            
            # Проверяем за PREDICTION_LEAD_TIME до обнуления
            if 0 < time_to_zero < self.PREDICTION_LEAD_TIME:
                logger.debug(
                    f"  🎯 {self.token_name}: проверка по предсказанию "
                    f"(осталось {time_to_zero/60:.0f} мин)"
                )
                return True
            
            # Если уверенность низкая, проверяем чаще
            if self._prediction_confidence < 0.5:
                if time.time() - self.last_check_time > 60 * 60:  # каждый час
                    return True
        
        return False
    # ==============================================
    
    # ============= СТАТИСТИКА =============
    def get_stats(self) -> Dict[str, Any]:
        """Получить статистику предсказателя"""
        zero_at = self.predict_zero_at()
        
        return {
            'token_name': self.token_name,
            'current_voices': self.token.voices,
            'total_spent': self.total_spent,
            'total_checks': self.total_checks,
            'success_rate': f"{self.successful_predictions/self.total_checks*100:.0f}%" if self.total_checks > 0 else "0%",
            'confidence': f"{self._prediction_confidence*100:.0f}%",
            'next_predicted_zero': datetime.fromtimestamp(zero_at).strftime("%H:%M") if zero_at else None,
            'hours_until_zero': f"{(zero_at - time.time())/3600:.1f}" if zero_at else None,
            'last_check': datetime.fromtimestamp(self.last_check_time).strftime("%H:%M") if self.last_check_time else "никогда",
            'last_spend': datetime.fromtimestamp(self.last_spend_time).strftime("%H:%M") if self.last_spend_time else "никогда",
            'history_size': len(self.spend_history)
        }
    # ======================================
    
    # ============= ВНУТРЕННИЕ МЕТОДЫ =============
    def _update_confidence(self) -> None:
        """Обновить уровень уверенности в предсказаниях"""
        if len(self.check_history) < 3:
            self._prediction_confidence = 0.3
            return
        
        # Берём последние 10 проверок
        recent = list(self.check_history)[-10:]
        successful = sum(1 for c in recent if c.was_predicted)
        total = len(recent)
        
        if total > 0:
            base_confidence = successful / total
        else:
            base_confidence = 0.3
        
        # Корректировка на размер истории
        history_factor = min(len(self.spend_history) / 50, 1.0)
        
        self._prediction_confidence = base_confidence * history_factor
    
    def _save_history(self) -> None:
        """Сохранить историю в JSON"""
        try:
            data = {
                'spend_history': [asdict(e) for e in self.spend_history],
                'check_history': [asdict(e) for e in self.check_history],
                'total_spent': self.total_spent,
                'total_checks': self.total_checks,
                'successful_predictions': self.successful_predictions,
                'last_spend_time': self.last_spend_time,
                'last_check_time': self.last_check_time,
                'version': '3.0.0'
            }
            
            with open(self.storage_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            logger.error(f"❌ {self.token_name}: ошибка сохранения истории: {e}")
    
    def _load_history(self) -> None:
        """Загрузить историю из JSON"""
        try:
            if not os.path.exists(self.storage_path):
                return
            
            with open(self.storage_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Загружаем историю расходов
            for e in data.get('spend_history', []):
                self.spend_history.append(VoiceSpendEvent(**e))
            
            # Загружаем историю проверок
            for e in data.get('check_history', []):
                self.check_history.append(VoiceCheckEvent(**e))
            
            self.total_spent = data.get('total_spent', 0)
            self.total_checks = data.get('total_checks', 0)
            self.successful_predictions = data.get('successful_predictions', 0)
            self.last_spend_time = data.get('last_spend_time', 0)
            self.last_check_time = data.get('last_check_time', 0)
            
            logger.debug(f"📂 {self.token_name}: загружена история ({len(self.spend_history)} расходов, {len(self.check_history)} проверок)")
            
        except Exception as e:
            logger.error(f"❌ {self.token_name}: ошибка загрузки истории: {e}")
    # ==============================================
