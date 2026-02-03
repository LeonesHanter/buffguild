# -*- coding: utf-8 -*-
import logging
import threading
import time
from typing import List, Optional, Tuple, Callable, Any, Dict

from .ability import build_ability_text_and_cd
from .constants import CLASS_ORDER, CLASS_ABILITIES, RACE_NAMES
from .models import ParsedAbility, Job
from .token_handler import TokenHandler

logger = logging.getLogger(__name__)


class Scheduler:
    def __init__(self, tm, executor, on_buff_complete: Callable[[Job, Dict], None] = None):
        self.tm = tm
        self.executor = executor

        self._q: List[Tuple[float, Job, str]] = []
        self._lock = threading.Lock()
        self._last_cleanup_time: float = 0.0

        self._on_buff_complete = on_buff_complete

        self._thr = threading.Thread(target=self._run_loop, daemon=True)
        self._thr.start()

    def enqueue_letters(self, job: Job, letters: str) -> None:
        letters = (letters or "")[:4]
        now = time.time()
        with self._lock:
            for ch in letters:
                self._q.append((now, job, ch))

    def get_queue_size(self) -> int:
        with self._lock:
            return len(self._q)

    def cancel_user_jobs(self, user_id: int) -> bool:
        with self._lock:
            original_len = len(self._q)
            self._q = [
                (ts, job, ch) for ts, job, ch in self._q if job.sender_id != user_id
            ]
            removed = original_len - len(self._q)
            if removed > 0:
                logging.info(
                    f"🗑️ Отменены бафы пользователя {user_id}: {removed} шт."
                )
                return True
            return False

    def _cleanup_old_jobs(self) -> None:
        now = time.time()
        if now - self._last_cleanup_time < 300:
            return
        with self._lock:
            original_len = len(self._q)
            self._q = [(ts, job, ch) for ts, job, ch in self._q if now - ts < 3600]
            if len(self._q) != original_len:
                logging.info(
                    f"🧹 Очищены старые задачи: {original_len - len(self._q)}"
                )
            self._last_cleanup_time = now

    def _pop_ready(self) -> Optional[Tuple[float, Job, str]]:
        now = time.time()
        with self._lock:
            self._q.sort(key=lambda x: x[0])
            if not self._q:
                return None
            if self._q[0][0] > now:
                return None
            return self._q.pop(0)

    def _reschedule(self, when_ts: float, job: Job, letter: str) -> None:
        with self._lock:
            self._q.append((when_ts, job, letter))

    def _build_ability(self, letter: str) -> Optional[ParsedAbility]:
        for cls in CLASS_ORDER:
            info = build_ability_text_and_cd(cls, letter)
            if info:
                txt, cd, uses_voices = info
                return ParsedAbility(letter, txt, cd, cls, uses_voices)
        return None

    def _calculate_token_score(self, token: TokenHandler, ability: ParsedAbility) -> float:
        score = 0.0

        if ability.uses_voices:
            score += token.voices * 0.1

        can_social, rem_social = token.can_use_social()
        if can_social:
            score += 10.0
        else:
            score -= rem_social

        can, rem = token.can_use_ability(ability.key)
        if can:
            score += 5.0
        else:
            score -= rem

        if token.total_attempts > 10:
            success_rate = token.successful_buffs / token.total_attempts
            score += success_rate * 5.0

        if ability.key in RACE_NAMES:
            score += 50.0

        return score

    def _candidates_for_ability(self, ability: ParsedAbility) -> List[TokenHandler]:
        candidates_with_scores: List[Tuple[float, TokenHandler]] = []

        observer_token = self.tm.get_observer()
        observer_id = observer_token.id if observer_token else None

        if ability.key in RACE_NAMES:
            for t in self.tm.get_apostles_with_race(ability.key):
                if observer_id and t.id == observer_id:
                    continue
                if not t.enabled or t.is_captcha_paused() or t.needs_manual_voices:
                    continue
                if ability.uses_voices and t.voices <= 0:
                    continue

                can_social, _ = t.can_use_social()
                if not can_social:
                    continue

                can, _ = t.can_use_ability(ability.key)
                if not can:
                    continue

                if t.class_type == "apostle" and not t.has_race(ability.key):
                    continue

                score = self._calculate_token_score(t, ability)
                score += 100.0
                candidates_with_scores.append((score, t))

        if not candidates_with_scores:
            for t in self.tm.all_buffers():
                if observer_id and t.id == observer_id:
                    continue
                if not t.enabled or t.is_captcha_paused() or t.needs_manual_voices:
                    continue

                class_data = CLASS_ABILITIES.get(t.class_type)
                if not class_data:
                    continue
                if ability.key not in class_data["abilities"]:
                    continue

                if ability.uses_voices and t.voices <= 0:
                    continue

                can_social, _ = t.can_use_social()
                if not can_social:
                    continue

                can, _ = t.can_use_ability(ability.key)
                if not can:
                    continue

                if t.class_type == "apostle" and ability.key in RACE_NAMES:
                    if not t.has_race(ability.key):
                        continue

                score = self._calculate_token_score(t, ability)
                candidates_with_scores.append((score, t))

        candidates_with_scores.sort(key=lambda x: x[0], reverse=True)
        return [t for _, t in candidates_with_scores]

    def _run_loop(self):
        while True:
            try:
                self._cleanup_old_jobs()

                item = self._pop_ready()
                if not item:
                    time.sleep(0.2)
                    continue

                _, job, letter = item
                ability = self._build_ability(letter)
                if not ability:
                    logging.warning(f"⚠️ Unknown letter '{letter}'")
                    continue

                candidates = self._candidates_for_ability(ability)
                if not candidates:
                    logging.warning(
                        f"🚫 Нет кандидатов для '{letter}', пропускаем задачу"
                    )
                    continue

                success = False
                attempt_status = ""
                buff_info = None

                token = candidates[0]
                ok, status, info = self.executor.execute_one(token, ability, job)
                attempt_status = status
                buff_info = info

                if ok or status in ("SUCCESS", "ALREADY"):
                    success = True
                    if success and buff_info and self._on_buff_complete:
                        try:
                            self._on_buff_complete(job, buff_info)
                        except Exception as e:
                            logging.error(
                                f"❌ Ошибка в колбэке on_buff_complete: {e}"
                            )
                else:
                    if len(candidates) > 1:
                        token = candidates[1]
                        ok, status, info = self.executor.execute_one(
                            token, ability, job
                        )
                        attempt_status = status
                        buff_info = info

                        if ok or status in ("SUCCESS", "ALREADY"):
                            success = True
                            if success and buff_info and self._on_buff_complete:
                                try:
                                    self._on_buff_complete(job, buff_info)
                                except Exception as e:
                                    logging.error(
                                        f"❌ Ошибка в колбэке on_buff_complete: {e}"
                                    )

                if not success:
                    self._reschedule(time.time() + 30.0, job, letter)
                    logging.info(
                        f"⏳ Не удалось обработать '{letter}' "
                        f"(статус: {attempt_status}), повтор через 30с"
                    )

            except Exception as e:
                logging.error(f"❌ Ошибка в Scheduler: {e}", exc_info=True)
