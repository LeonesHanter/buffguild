# -*- coding: utf-8 -*-
import logging
import random
import threading
import time
from typing import List, Optional, Tuple, Callable, Any, Dict
from collections import defaultdict

from .ability import build_ability_text_and_cd
from .constants import CLASS_ORDER, CLASS_ABILITIES, RACE_NAMES
from .models import ParsedAbility, Job
from .token_handler import TokenHandler

logger = logging.getLogger(__name__)


class Scheduler:
    def __init__(
        self,
        tm,
        executor,
        on_buff_complete: Callable[[Job, Dict], None] = None,
    ):
        self.tm = tm
        self.executor = executor
        # очередь: (when_ts, job, letter, preferred_token_id)
        self._q: List[Tuple[float, Job, str, Optional[str]]] = []
        self._lock = threading.Lock()
        self._last_cleanup_time: float = 0.0
        self._on_buff_complete = on_buff_complete
        
        # ============= ТУРБО-РЕЖИМ =============
        self.turbo_mode_enabled = True
        self.TURBO_DELAY = 0.15  # 150 мс между разными токенами
        self.SAME_CLASS_DELAY = 0.3  # 300 мс для одного класса (социальное КД)
        self.MIN_LETTERS_FOR_TURBO = 2
        self.MAX_LETTERS = 4
        
        # Статистика турбо-режима
        self.turbo_stats = {
            'total_bursts': 0,
            'total_letters': 0,
            'total_time': 0.0,  # float!
            'total_time_saved': 0.0,  # float!
            'race_bursts': 0,
            'mixed_bursts': 0,
            'same_class_bursts': 0
        }
        # =======================================
        
        self._thr = threading.Thread(target=self._run_loop, daemon=True)
        self._thr.start()

    # ============= ТУРБО-РЕЖИМ =============
    def enqueue_letters(self, job: Job, letters: str) -> None:
        """
        ТУРБО-РЕЖИМ для любых комбинаций бафов.
        
        ОСОБЕННОСТИ:
        1. Максимум 1 РАСА в команде (ч,г,н,э,м,д,о)
        2. Остальные буквы - атака/защита/удача/проклятия/очищения/воскрешения
        3. Разные токены = интервал 0.15с
        4. Один токен = интервал 0.3с (социальное КД)
        """
        letters = (letters or "")[:self.MAX_LETTERS]
        now = time.time()
        
        with self._lock:
            # Проверяем количество рас в команде
            race_letters = [ch for ch in letters if ch in RACE_NAMES]
            
            # ЕСЛИ БОЛЬШЕ 1 РАСЫ - ИСПОЛЬЗУЕМ ОБЫЧНЫЙ РЕЖИМ!
            if len(race_letters) > 1:
                logger.warning(f"⚠️ В команде {len(race_letters)} расы! Турбо-режим ТОЛЬКО для 1 расы в команде.")
                logger.warning(f"   Использую обычный режим для: {letters}")
                for ch in letters:
                    self._q.append((now, job, ch, None))
                return
            
            # Проверяем, можно ли включить турбо
            use_turbo = (
                self.turbo_mode_enabled and 
                len(letters) >= self.MIN_LETTERS_FOR_TURBO
            )
            
            if use_turbo:
                # ============= ТУРБО-РЕЖИМ =============
                burst_type = "РАСА" if race_letters else "НЕ-РАСЫ"
                logger.info(f"🚀 TURBO [{burst_type}]: {job.sender_id} заказал {len(letters)} бафов: {letters}")
                
                # Анализируем классы способностей для каждой буквы
                letter_classes = {}
                valid_letters = []
                
                for ch in letters:
                    ability = self._build_ability(ch)
                    if ability and ability.token_name:
                        letter_classes[ch] = {
                            'class': ability.token_name,
                            'is_race': ch in RACE_NAMES
                        }
                        valid_letters.append(ch)
                        logger.debug(f"   📌 Буква '{ch}': класс {ability.token_name}")
                    else:
                        # Если способность не найдена - используем обычный режим для этой буквы
                        logger.warning(f"⚠️ Буква '{ch}' не распознана, добавляю без задержки")
                        self._q.append((now, job, ch, None))
                
                # Если нет валидных букв для турбо - выходим
                if not valid_letters:
                    logger.warning("❌ Нет валидных букв для турбо-режима")
                    return
                
                # Создаём таймлайн с учётом повторяющихся классов
                timeline = []
                class_last_used = {}  # class -> timestamp
                
                for idx, ch in enumerate(valid_letters):
                    # Базовая задержка от позиции
                    base_delay = idx * self.TURBO_DELAY
                    additional_delay = 0.0
                    
                    # Получаем класс способности
                    cls_info = letter_classes.get(ch, {})
                    class_type = cls_info.get('class', 'unknown')
                    
                    # Если этот класс уже использовался - добавляем задержку
                    if class_type in class_last_used:
                        # Сколько времени прошло с последнего использования
                        time_since_last = base_delay - class_last_used[class_type]
                        if time_since_last < self.SAME_CLASS_DELAY:
                            additional_delay = self.SAME_CLASS_DELAY - time_since_last
                            logger.debug(f"   🔄 Класс {class_type} повторяется через {time_since_last:.2f}с, добавляю {additional_delay:.2f}с")
                            self.turbo_stats['same_class_bursts'] += 1
                    
                    # Итоговая задержка
                    total_delay = round(base_delay + additional_delay, 2)
                    
                    # Запоминаем время использования этого класса
                    class_last_used[class_type] = total_delay
                    
                    # Добавляем в очередь
                    self._q.append((now + total_delay, job, ch, None))
                    timeline.append(f"{ch}+{total_delay:.2f}с")
                
                # Логируем таймлайн
                logger.info(f"   📊 Таймлайн: {' → '.join(timeline)}")
                
                # ============= ИСПРАВЛЕНИЕ ОШИБКИ =============
                # Безопасно получаем общее время
                total_time = 0.0
                if timeline:
                    try:
                        last_item = timeline[-1]
                        time_str = last_item.split('+')[1].rstrip('с')
                        total_time = float(time_str)
                    except (IndexError, ValueError, AttributeError) as e:
                        logger.error(f"❌ Ошибка парсинга времени: {e}")
                        total_time = len(valid_letters) * self.TURBO_DELAY
                
                # Статистика
                self.turbo_stats['total_bursts'] += 1
                self.turbo_stats['total_letters'] += len(valid_letters)
                self.turbo_stats['total_time'] += total_time
                
                if race_letters:
                    self.turbo_stats['race_bursts'] += 1
                else:
                    self.turbo_stats['mixed_bursts'] += 1
                
                # Примерное время сэкономлено (сравнение с обычным режимом 2с/баф)
                estimated_normal_time = len(valid_letters) * 2.0
                self.turbo_stats['total_time_saved'] += estimated_normal_time - total_time
                
                return
            # ======================================
            
            # Обычный режим (для 1 буквы или выключенного турбо)
            for ch in letters:
                self._q.append((now, job, ch, None))
    
    def get_turbo_stats(self) -> Dict[str, Any]:
        """Получить статистику турбо-режима"""
        return {
            'enabled': self.turbo_mode_enabled,
            'turbo_delay': f"{self.TURBO_DELAY*1000:.0f}мс",
            'same_class_delay': f"{self.SAME_CLASS_DELAY*1000:.0f}мс",
            'min_letters': self.MIN_LETTERS_FOR_TURBO,
            'total_bursts': self.turbo_stats['total_bursts'],
            'total_letters': self.turbo_stats['total_letters'],
            'race_bursts': self.turbo_stats['race_bursts'],
            'mixed_bursts': self.turbo_stats['mixed_bursts'],
            'same_class_bursts': self.turbo_stats['same_class_bursts'],
            'avg_time': f"{self.turbo_stats['total_time'] / max(self.turbo_stats['total_bursts'], 1):.2f}с",
            'time_saved': f"{self.turbo_stats['total_time_saved']:.1f}с"
        }
    # =============================================

    def get_queue_size(self) -> int:
        with self._lock:
            return len(self._q)

    def cancel_user_jobs(self, user_id: int) -> bool:
        with self._lock:
            original_len = len(self._q)
            self._q = [(ts, job, ch, tid) for ts, job, ch, tid in self._q if job.sender_id != user_id]
            removed = original_len - len(self._q)
            if removed > 0:
                logging.info(f"🗑️ Отменены бафы пользователя {user_id}: {removed} шт.")
                return True
        return False

    def _cleanup_old_jobs(self) -> None:
        now = time.time()
        if now - self._last_cleanup_time < 300:
            return

        with self._lock:
            original_len = len(self._q)
            self._q = [(ts, job, ch, tid) for ts, job, ch, tid in self._q if now - ts < 3600]
            if len(self._q) != original_len:
                logging.info(f"🧹 Очищены старые задачи: {original_len - len(self._q)}")
            self._last_cleanup_time = now

    def _pop_ready(self) -> Optional[Tuple[float, Job, str, Optional[str]]]:
        """Взять задачу, которую уже можно выполнять"""
        now = time.time()
        with self._lock:
            self._q.sort(key=lambda x: x[0])
            if not self._q:
                return None
            if self._q[0][0] > now:
                return None
            return self._q.pop(0)

    def _reschedule(self, when_ts: float, job: Job, letter: str) -> None:
        """Переставить задачу в очереди на другое время"""
        with self._lock:
            self._q.append((when_ts, job, letter, None))

    def _build_ability(self, letter: str) -> Optional[ParsedAbility]:
        for cls in CLASS_ORDER:
            info = build_ability_text_and_cd(cls, letter)
            if info:
                txt, cd, uses_voices = info
                return ParsedAbility(letter, txt, cd, cls, uses_voices)
        return None

    # -------------------------
    # Candidate selection policy
    # -------------------------

    def _is_token_basic_ok(self, t: TokenHandler, ability: ParsedAbility) -> bool:
        if not t.enabled:
            return False
        if t.is_captcha_paused():
            return False
        if t.needs_manual_voices:
            return False
        if ability.uses_voices and t.voices <= 0:
            return False
        return True

    def _supports_ability(self, t: TokenHandler, ability: ParsedAbility) -> bool:
        class_data = CLASS_ABILITIES.get(t.class_type)
        if not class_data:
            return False
        return ability.key in class_data["abilities"]

    def _cooldown_wait_seconds(self, t: TokenHandler, ability: ParsedAbility) -> float:
        can_social, rem_social = t.can_use_social()
        can_ability, rem_ability = t.can_use_ability(ability.key)

        rs = 0.0 if can_social else float(rem_social)
        ra = 0.0 if can_ability else float(rem_ability)

        return max(rs, ra)

    def _candidates_and_wait(self, ability: ParsedAbility, preferred_token: Optional[str] = None) -> Tuple[List[TokenHandler], float]:
        """
        Получает кандидатов для бафа.
        Если указан preferred_token - пробуем его первым.
        """
        observer_token = self.tm.get_observer()
        observer_id = observer_token.id if observer_token else None

        # 1) Race ability: ONLY apostles with the race.
        if ability.key in RACE_NAMES:
            ready: List[TokenHandler] = []
            for t in self.tm.get_apostles_with_race(ability.key):
                if observer_id and t.id == observer_id:
                    continue
                if not self._is_token_basic_ok(t, ability):
                    continue
                if t.class_type != "apostle" or not t.has_race(ability.key):
                    continue
                if not self._supports_ability(t, ability):
                    continue
                if self._cooldown_wait_seconds(t, ability) > 0:
                    continue
                ready.append(t)

            if preferred_token:
                for i, t in enumerate(ready):
                    if t.id == preferred_token:
                        ready.pop(i)
                        ready.insert(0, t)
                        break

            random.shuffle(ready)
            return ready, 0.0

        # 2) Non-race ability
        ready2: List[TokenHandler] = []
        min_wait: Optional[float] = None

        for t in self.tm.all_buffers():
            if observer_id and t.id == observer_id:
                continue
            if not self._is_token_basic_ok(t, ability):
                continue
            if not self._supports_ability(t, ability):
                continue

            wait_s = self._cooldown_wait_seconds(t, ability)
            if wait_s <= 0:
                ready2.append(t)
            else:
                if min_wait is None or wait_s < min_wait:
                    min_wait = wait_s

        if preferred_token and ready2:
            for i, t in enumerate(ready2):
                if t.id == preferred_token:
                    ready2.pop(i)
                    ready2.insert(0, t)
                    break

        random.shuffle(ready2)
        return ready2, float(min_wait or 0.0)

    def _call_on_complete_safe(self, job: Job, buff_info: Dict) -> None:
        if not self._on_buff_complete or not buff_info:
            return
        try:
            self._on_buff_complete(job, buff_info)
        except Exception as e:
            logging.error(f"❌ Ошибка в колбэке on_buff_complete: {e}")

    def _run_loop(self):
        """
        ЕДИНСТВЕННЫЙ ИСПОЛНИТЕЛЬ.
        Этот поток забирает задачи из очереди и выполняет их.
        """
        while True:
            try:
                self._cleanup_old_jobs()

                item = self._pop_ready()
                if not item:
                    time.sleep(0.05)  # 50мс для быстрого отклика
                    continue

                when, job, letter, preferred_token = item
                
                # Точная задержка до времени старта
                sleep_time = when - time.time()
                if sleep_time > 0:
                    time.sleep(sleep_time)
                
                self._execute_buff(job, letter, preferred_token)

            except Exception as e:
                logging.error(f"❌ Ошибка в Scheduler: {e}", exc_info=True)
                time.sleep(1)

    def _execute_buff(self, job: Job, letter: str, preferred_token: Optional[str] = None):
        """Выполнение одного бафа"""
        ability = self._build_ability(letter)
        if not ability:
            logger.warning(f"⚠️ Unknown letter '{letter}'")
            return

        candidates, wait_s = self._candidates_and_wait(ability, preferred_token)

        # Если нет кандидатов для расы - пропускаем
        if not candidates and ability.key in RACE_NAMES:
            logger.warning(f"🚫 Нет кандидатов по расе для '{letter}', пропускаем задачу")
            if self._on_buff_complete:
                dummy_buff_info: Dict[str, Any] = {
                    "token_name": "",
                    "buff_value": 0,
                    "is_critical": False,
                    "ability_key": ability.key,
                    "buff_name": ability.text,
                    "full_text": "",
                    "status": "NO_RACE_CANDIDATES",
                }
                self._call_on_complete_safe(job, dummy_buff_info)
            return

        # Если нет кандидатов но есть КД - переставляем в очередь
        if not candidates and wait_s > 0:
            when = time.time() + wait_s + 0.5
            self._reschedule(when, job, letter)
            logger.info(f"⏳ Все токены в КД для '{letter}', повтор через {int(wait_s)}с")
            return

        # Нет кандидатов вообще
        if not candidates:
            logger.warning(f"🚫 Нет кандидатов для '{letter}', пропускаем задачу")
            if self._on_buff_complete:
                dummy_buff_info2: Dict[str, Any] = {
                    "token_name": "",
                    "buff_value": 0,
                    "is_critical": False,
                    "ability_key": ability.key,
                    "buff_name": ability.text,
                    "full_text": "",
                    "status": "NO_CANDIDATES",
                }
                self._call_on_complete_safe(job, dummy_buff_info2)
            return

        success = False
        attempt_status = ""
        buff_info: Optional[Dict[str, Any]] = None
        pass_to_next = False

        # Пробуем ВСЕХ кандидатов
        for token in candidates:
            ok, status, info = self.executor.execute_one(token, ability, job)
            attempt_status = status
            buff_info = info or {}
            norm_status = (status or "").upper()
            if norm_status == "ALREADY":
                norm_status = "ALREADY_BUFF"
            buff_info.setdefault("status", norm_status)

            if norm_status == "PASS_TO_NEXT_APOSTLE":
                pass_to_next = True
                logger.info(f"🔄 {token.name}: передача эстафеты другому апостолу для '{letter}'")
                continue

            if norm_status in ("NO_VOICES", "NO_VOICES_LOCAL"):
                logger.info(f"⛔ {token.name}: нет голосов, пробуем следующего")
                continue

            if norm_status == "OTHER_RACE":
                logger.info(f"🚫 OTHER_RACE для '{letter}' у {token.name}")
                self._call_on_complete_safe(job, buff_info)
                success = True
                break

            if ok or norm_status in ("SUCCESS", "ALREADY_BUFF"):
                success = True
                self._call_on_complete_safe(job, buff_info)
                break

        # Если прошли всех кандидатов и все сказали "PASS_TO_NEXT_APOSTLE"
        if not success and pass_to_next and attempt_status == "PASS_TO_NEXT_APOSTLE":
            logger.warning(f"🚫 Все апостолы не подходят для расы '{letter}'")
            if self._on_buff_complete:
                buff_info = buff_info or {}
                buff_info["status"] = "NO_SUITABLE_APOSTLE"
                self._call_on_complete_safe(job, buff_info)

        elif not success:
            if attempt_status and attempt_status.upper() in ("SUCCESS", "ALREADY", "ALREADY_BUFF"):
                self._call_on_complete_safe(job, buff_info or {})
            else:
                self._reschedule(time.time() + 30.0, job, letter)
                logger.info(f"⏳ Не удалось обработать '{letter}' (статус: {attempt_status}), повтор через 30с")
