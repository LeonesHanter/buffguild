# -*- coding: utf-8 -*-
import logging
import random
import threading
import time
from typing import List, Optional, Tuple, Callable, Any, Dict

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
        # очередь: (when_ts, job, letter)
        self._q: List[Tuple[float, Job, str]] = []
        self._lock = threading.Lock()
        self._last_cleanup_time: float = 0.0
        self._on_buff_complete = on_buff_complete
        self._thr = threading.Thread(target=self._run_loop, daemon=True)
        self._thr.start()

    def enqueue_letters(self, job: Job, letters: str) -> None:
        letters = (letters or "")[:4]
        now = time.time()

        # Приоритет рас: сначала расовые буквы, потом остальные
        race_keys = set(RACE_NAMES.keys())
        race_letters = [ch for ch in letters if ch in race_keys]
        non_race_letters = [ch for ch in letters if ch not in race_keys]
        ordered = race_letters + non_race_letters

        with self._lock:
            for ch in ordered:
                self._q.append((now, job, ch))

    def get_queue_size(self) -> int:
        with self._lock:
            return len(self._q)

    def cancel_user_jobs(self, user_id: int) -> bool:
        with self._lock:
            original_len = len(self._q)
            self._q = [(ts, job, ch) for ts, job, ch in self._q if job.sender_id != user_id]
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
            self._q = [(ts, job, ch) for ts, job, ch in self._q if now - ts < 3600]
            if len(self._q) != original_len:
                logging.info(f"🧹 Очищены старые задачи: {original_len - len(self._q)}")
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

    # -------------------------
    # Candidate selection policy:
    # - Race letters: ONLY apostles with that race; if none ready -> skip.
    # - Non-race letters: random among ready; if none ready due to cooldown -> reschedule to earliest.
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
        # How many seconds until the token can be used for this ability,
        # considering BOTH social and ability cooldowns.
        can_social, rem_social = t.can_use_social()
        can_ability, rem_ability = t.can_use_ability(ability.key)

        rs = 0.0 if can_social else float(rem_social)
        ra = 0.0 if can_ability else float(rem_ability)

        # Need both available => wait until both have expired
        return max(rs, ra)

    def _candidates_and_wait(self, ability: ParsedAbility) -> Tuple[List[TokenHandler], float]:
        """
        Returns:
            candidates: ready-to-use tokens in RANDOM order
            wait_s: if no ready candidates for NON-RACE ability, minimal time to wait
                    until any eligible token becomes available (0 if no wait / not applicable).
        """
        observer_token = self.tm.get_observer()
        observer_id = observer_token.id if observer_token else None

        # 1) Race ability: ONLY apostles with the race. No fallback.
        if ability.key in RACE_NAMES:
            ready: List[TokenHandler] = []
            for t in self.tm.get_apostles_with_race(ability.key):
                if observer_id and t.id == observer_id:
                    continue
                if not self._is_token_basic_ok(t, ability):
                    continue
                # Safety: ensure it really has this race
                if t.class_type != "apostle" or not t.has_race(ability.key):
                    continue
                if not self._supports_ability(t, ability):
                    continue
                # Must be ready NOW (no cooldown)
                if self._cooldown_wait_seconds(t, ability) > 0:
                    continue
                ready.append(t)

            random.shuffle(ready)
            return ready, 0.0  # if empty -> skip in run loop (no reschedule)

        # 2) Non-race ability: random among ready; if none, compute earliest wait.
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

                candidates, wait_s = self._candidates_and_wait(ability)

                # Если race letter и нет кандидатов -> skip (no fallback, no reschedule)
                if not candidates and ability.key in RACE_NAMES:
                    logging.warning(f"🚫 Нет кандидатов по расе для '{letter}', пропускаем задачу")
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
                    continue

                # Non-race: если нет кандидатов но есть cooldown wait -> reschedule to earliest moment
                if not candidates and wait_s > 0:
                    when = time.time() + wait_s + 0.5  # small buffer
                    self._reschedule(when, job, letter)
                    logging.info(f"⏳ Все токены в КД для '{letter}', повтор через {int(wait_s)}с")
                    continue

                # No candidates at all (disabled/captcha/no voices/etc.)
                if not candidates:
                    logging.warning(f"🚫 Нет кандидатов для '{letter}', пропускаем задачу")
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
                    continue

                success = False
                attempt_status = ""
                buff_info: Optional[Dict[str, Any]] = None
                pass_to_next = False  # 🆕 Флаг передачи эстафеты

                # 🆕 Пробуем ВСЕХ кандидатов (не только 2)
                for token in candidates:
                    ok, status, info = self.executor.execute_one(token, ability, job)
                    attempt_status = status
                    buff_info = info or {}
                    norm_status = (status or "").upper()
                    if norm_status == "ALREADY":
                        norm_status = "ALREADY_BUFF"
                    buff_info.setdefault("status", norm_status)

                    # 🆕 Если нужно передать другому апостолу
                    if norm_status == "PASS_TO_NEXT_APOSTLE":
                        pass_to_next = True
                        logging.info(
                            f"🔄 {token.name}: передача эстафеты другому апостолу для '{letter}'"
                        )
                        continue  # Пробуем следующего

                    # нет голосов у этого токена -> пробуем следующего
                    if norm_status in ("NO_VOICES", "NO_VOICES_LOCAL"):
                        logging.info(
                            f"⛔ {token.name}: нет голосов (status={norm_status}), "
                            f"пробуем следующего кандидата для '{letter}'"
                        )
                        continue

                    # на цели уже другое расовое благословение -> считаем попытку завершённой
                    if norm_status == "OTHER_RACE":
                        logging.info(
                            f"🚫 OTHER_RACE для '{letter}' у {token.name}: "
                            f"на цели уже другое расовое благословение, задачу не повторяем"
                        )
                        self._call_on_complete_safe(job, buff_info)
                        success = True
                        break

                    if ok or norm_status in ("SUCCESS", "ALREADY_BUFF"):
                        success = True
                        self._call_on_complete_safe(job, buff_info)
                        break

                # 🆕 Если прошли всех кандидатов и все сказали "PASS_TO_NEXT_APOSTLE"
                if not success and pass_to_next and attempt_status == "PASS_TO_NEXT_APOSTLE":
                    logging.warning(
                        f"🚫 Все апостолы не подходят для расы '{letter}' "
                        f"(NOT_APOSTLE_OF_RACE или OTHER_RACE)"
                    )
                    if self._on_buff_complete:
                        buff_info = buff_info or {}
                        buff_info["status"] = "NO_SUITABLE_APOSTLE"
                        self._call_on_complete_safe(job, buff_info)

                elif not success:
                    if attempt_status and attempt_status.upper() in ("SUCCESS", "ALREADY", "ALREADY_BUFF"):
                        self._call_on_complete_safe(job, buff_info or {})
                    else:
                        self._reschedule(time.time() + 30.0, job, letter)
                        logging.info(
                            f"⏳ Не удалось обработать '{letter}' (статус: {attempt_status}), повтор через 30с"
                        )

            except Exception as e:
                logging.error(f"❌ Ошибка в Scheduler: {e}", exc_info=True)
