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

        # 🔁 глобальный КД по цели + способности (ability.key)
        # ключ: (target_id, ability_key) -> unix_ts до какого времени не пробуем
        self._global_cooldowns: Dict[Tuple[int, str], float] = {}

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

                # 🔍 Глобальный КД по цели + способности
                # сейчас цель отождествляем с sender_id
                target_id = job.sender_id
                gc_key = (target_id, ability.key)
                now = time.time()
                gc_until = self._global_cooldowns.get(gc_key)

                if gc_until and gc_until > now:
                    remaining = int(gc_until - now)
                    logging.info(
                        f"⏳ Цель {target_id} в глобальном КД по '{ability.key}', "
                        f"осталось ~{remaining}s, пропускаем попытку"
                    )
                    # перенесём задачу ближе к окончанию глобального КД
                    when = gc_until + 0.5
                    self._reschedule(when, job, letter)
                    continue

                candidates, wait_s = self._candidates_and_wait(ability)

                # If race letter and no candidates -> skip (no fallback, no reschedule)
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

                # Non-race: if no candidates but there is a cooldown wait -> reschedule to earliest moment
                if not candidates and wait_s > 0:
                    now = time.time()
                    when = now + wait_s + 0.5  # small buffer
                    self._reschedule(when, job, letter)
                    logging.info(f"⏳ Все токены в КД для '{letter}', повтор через {int(wait_s)}с")

                    # 🔁 Ставим глобальный КД по цели + способности
                    target_id = job.sender_id
                    gc_key = (target_id, ability.key)
                    gc_until = now + wait_s
                    self._global_cooldowns[gc_key] = gc_until
                    logging.info(
                        f"⏳ Устанавливаем глобальный КД для цели {target_id} и '{ability.key}' на {int(wait_s)}s"
                    )
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

                cooldown_seen = False  # хотя бы один токен вернул COOLDOWN

                # Try up to 2 random candidates (already shuffled)
                for token in candidates[:2]:
                    ok, status, info = self.executor.execute_one(token, ability, job)
                    attempt_status = status
                    buff_info = info or {}
                    norm_status = (status or "").upper()
                    if norm_status == "ALREADY":
                        norm_status = "ALREADY_BUFF"
                    buff_info.setdefault("status", norm_status)

                    # нет голосов у этого токена -> просто пробуем следующего
                    if norm_status in ("NO_VOICES", "NO_VOICES_LOCAL"):
                        logging.info(
                            f"⛔ {token.name}: нет голосов (status={norm_status}), "
                            f"пробуем следующего кандидата для '{letter}'"
                        )
                        continue

                    # на цели уже другое расовое благословение -> считаем попытку завершённой, но с ошибкой
                    if norm_status == "OTHER_RACE":
                        logging.info(
                            f"🚫 OTHER_RACE для '{letter}' у {token.name}: "
                            f"на цели уже другое расовое благословение, задачу не повторяем"
                        )
                        self._call_on_complete_safe(job, buff_info)
                        success = True
                        break

                    # COOLDOWN: отмечаем и пробуем следующего токена
                    if norm_status.startswith("COOLDOWN"):
                        cooldown_seen = True
                        logging.info(
                            f"⏳ {token.name}: COOLDOWN для '{letter}' (status={norm_status}), "
                            f"пробуем следующего кандидата"
                        )
                        continue

                    if ok or norm_status in ("SUCCESS", "ALREADY_BUFF"):
                        success = True
                        self._call_on_complete_safe(job, buff_info)

                        # ✅ Баф/эффект прошёл — убираем глобальный КД для этой цели+способности
                        try:
                            target_id_ok = job.sender_id
                            gc_key_ok = (target_id_ok, ability.key)
                            if gc_key_ok in self._global_cooldowns:
                                self._global_cooldowns.pop(gc_key_ok, None)
                                logging.info(
                                    f"✅ Сброс глобального КД для цели {target_id_ok} и '{ability.key}' "
                                    f"после успешного статуса {norm_status}"
                                )
                        except Exception:
                            pass

                        break

                if not success:
                    norm_attempt = (attempt_status or "").upper()

                    # 🧊 Все кандидаты дали только COOLDOWN -> считаем задачу завершённой без повторов
                    if cooldown_seen and norm_attempt not in ("SUCCESS", "ALREADY", "ALREADY_BUFF"):
                        logging.info(
                            f"🧊 Все кандидаты для '{letter}' в КД (последний статус: {attempt_status}), "
                            f"задачу считаем завершённой без повторов"
                        )
                        if self._on_buff_complete:
                            info_cd = buff_info or {}
                            info_cd.setdefault("status", "ALL_IN_COOLDOWN")
                            self._call_on_complete_safe(job, info_cd)
                    else:
                        if norm_attempt in ("SUCCESS", "ALREADY", "ALREADY_BUFF"):
                            self._call_on_complete_safe(job, buff_info or {})
                        else:
                            self._reschedule(time.time() + 30.0, job, letter)
                            logging.info(
                                f"⏳ Не удалось обработать '{letter}' (статус: {attempt_status}), повтор через 30с"
                            )

            except Exception as e:
                logging.error(f"❌ Ошибка в Scheduler: {e}", exc_info=True)
