# -*- coding: utf-8 -*-
"""
Thread-safe job state store for Observer.
"""
from __future__ import annotations

import logging
import time
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from .models import Job
from .job_storage import JobStorage

logger = logging.getLogger(__name__)


@dataclass
class ActiveJobInfo:
    job: Job
    letters: str
    cmid: Optional[int]
    message_id: int
    registration_time: float
    registration_msg_id: Optional[int] = None


@dataclass
class BuffResultInfo:
    tokens_info: List[Dict[str, Any]]
    total_value: int
    expected_count: int
    completed_count: int
    registration_msg_id: Optional[int] = None


class JobStateStore:
    def __init__(self, storage_path: str = "jobs.json") -> None:
        self._lock = threading.Lock()
        self._active_jobs: Dict[int, ActiveJobInfo] = {}
        self._buff_results: Dict[int, BuffResultInfo] = {}
        self._storage = JobStorage(path=storage_path)

    def has_active(self, user_id: int) -> bool:
        with self._lock:
            return user_id in self._active_jobs

    def get_letters(self, user_id: int) -> str:
        with self._lock:
            info = self._active_jobs.get(user_id)
            return info.letters if info else ""

    def restore_and_enqueue(self, scheduler) -> None:
        try:
            stored = self._storage.load_all()
        except Exception as e:
            logger.error(f"❌ Ошибка восстановления активных бафов: {e}")
            return

        if not stored:
            return

        now = time.time()
        max_age = 3600
        restored = 0

        for user_id, (job_dict, buff_dict) in stored.items():
            try:
                job_payload = job_dict.get("job", {})
                job = Job(
                    sender_id=job_payload["sender_id"],
                    trigger_text=job_payload["trigger_text"],
                    letters=job_payload["letters"],
                    created_ts=job_payload["created_ts"],
                )
                # Восстанавливаем registration_msg_id
                job.registration_msg_id = job_dict.get("registration_msg_id")
            except Exception:
                continue

            if now - job.created_ts > max_age:
                continue

            job_info = ActiveJobInfo(
                job=job,
                letters=job_dict.get("letters", job.letters),
                cmid=job_dict.get("cmid"),
                message_id=job_dict.get("message_id", 0),
                registration_time=job_dict.get("registration_time", job.created_ts),
                registration_msg_id=job_dict.get("registration_msg_id"),
            )

            with self._lock:
                self._active_jobs[user_id] = job_info
                if buff_dict:
                    self._buff_results[user_id] = BuffResultInfo(
                        tokens_info=buff_dict.get("tokens_info", []),
                        total_value=buff_dict.get("total_value", 0),
                        expected_count=buff_dict.get("expected_count", 0),
                        completed_count=buff_dict.get("completed_count", 0),
                        registration_msg_id=buff_dict.get("registration_msg_id"),
                    )

            letters_all = (job_info.letters or "")
            done = 0
            if buff_dict:
                try:
                    done = int(buff_dict.get("completed_count", 0) or 0)
                except Exception:
                    done = 0
            done = max(0, min(done, len(letters_all)))
            letters_left = letters_all[done:]

            if letters_left:
                scheduler.enqueue_letters(job, letters_left)
                logger.info(
                    f"🔁 Восстановлена очередь бафов для {user_id}: осталось '{letters_left}' "
                    f"(из '{letters_all}', done={done})"
                )
            else:
                logger.info(f"ℹ️ Бафы для {user_id} уже завершены (letters='{letters_all}', done={done})")

            restored += 1

        if restored:
            logger.info(f"📦 Восстановлено активных бафов из jobs.json: {restored} пользователей")

    def register_job(self, user_id: int, job: Job, letters: str, cmid: Optional[int]) -> ActiveJobInfo:
        with self._lock:
            info = ActiveJobInfo(
                job=job,
                letters=letters,
                cmid=cmid,
                message_id=0,
                registration_time=time.time(),
                registration_msg_id=None,
            )
            self._active_jobs[user_id] = info
            self._buff_results[user_id] = BuffResultInfo(
                tokens_info=[],
                total_value=0,
                expected_count=len(letters),
                completed_count=0,
                registration_msg_id=None,
            )
            self._save_locked(user_id)
            return info

    def update_message_id(self, user_id: int, message_id: int) -> None:
        with self._lock:
            info = self._active_jobs.get(user_id)
            if not info:
                logger.warning(f"⚠️ Попытка обновить message_id для несуществующего job user_id={user_id}")
                return

            info.message_id = message_id
            info.registration_msg_id = message_id
            # Сохраняем ID в самом job
            info.job.registration_msg_id = message_id

            if user_id in self._buff_results:
                self._buff_results[user_id].registration_msg_id = message_id

            logger.info(f"📝 Сохранен registration_msg_id={message_id} для user_id={user_id}")
            self._save_locked(user_id)

    def cancel_and_clear(self, user_id: int) -> Tuple[bool, str]:
        with self._lock:
            info = self._active_jobs.get(user_id)
            if not info:
                return False, ""
            letters = info.letters
            self._buff_results.pop(user_id, None)
            self._active_jobs.pop(user_id, None)
            self._storage.delete_for_user(user_id)
            logger.info(f"🗑️ Отменены бафы для user_id={user_id}, letters='{letters}'")
            return True, letters

    def apply_completion(self, job: Job, buff_info: Dict[str, Any]) -> Tuple[bool, Optional[List[Dict[str, Any]]]]:
        user_id = job.sender_id
        with self._lock:
            if user_id not in self._active_jobs:
                logger.debug(f"⚠️ apply_completion для неактивного user_id={user_id}")
                return False, None

            buff_value = buff_info.get("buff_value", 0)
            try:
                buff_value_int = int(buff_value or 0)
            except Exception:
                buff_value_int = 0
            status = buff_info.get("status", "SUCCESS")

            if user_id not in self._buff_results:
                letters = self._active_jobs[user_id].letters
                self._buff_results[user_id] = BuffResultInfo(
                    tokens_info=[],
                    total_value=0,
                    expected_count=len(letters),
                    completed_count=0,
                    registration_msg_id=self._active_jobs[user_id].registration_msg_id,
                )

            user_data = self._buff_results[user_id]

            if "registration_msg_id" not in buff_info and user_data.registration_msg_id:
                buff_info["registration_msg_id"] = user_data.registration_msg_id
                logger.info(f"📝 Добавлен registration_msg_id={user_data.registration_msg_id} в buff_info для user_id={user_id}")

            user_data.tokens_info.append(buff_info)
            if status == "SUCCESS":
                user_data.total_value += buff_value_int
            user_data.completed_count += 1

            logger.debug(f"📊 user_id={user_id}: completed={user_data.completed_count}/{user_data.expected_count}")

            self._save_locked(user_id)

            if user_data.completed_count >= user_data.expected_count:
                snapshot = list(user_data.tokens_info)

                for i, item in enumerate(snapshot):
                    if "registration_msg_id" not in item and user_data.registration_msg_id:
                        item["registration_msg_id"] = user_data.registration_msg_id
                        logger.debug(f"📝 Добавлен registration_msg_id={user_data.registration_msg_id} в snapshot[{i}]")

                self._buff_results.pop(user_id, None)
                self._active_jobs.pop(user_id, None)
                self._storage.delete_for_user(user_id)

                logger.info(f"✅ Все бафы собраны для user_id={user_id}, всего {len(snapshot)} шт.")
                return True, snapshot

            return False, None

    def _save_locked(self, user_id: int) -> None:
        info = self._active_jobs.get(user_id)
        if not info:
            return
        buff = self._buff_results.get(user_id)

        job_dict = {
            "job": {
                "sender_id": info.job.sender_id,
                "trigger_text": info.job.trigger_text,
                "letters": info.job.letters,
                "created_ts": info.job.created_ts,
            },
            "letters": info.letters,
            "cmid": info.cmid,
            "message_id": info.message_id,
            "registration_time": info.registration_time,
            "registration_msg_id": info.registration_msg_id,
        }
        buff_dict = {
            "tokens_info": (buff.tokens_info if buff else []),
            "total_value": (buff.total_value if buff else 0),
            "expected_count": (buff.expected_count if buff else len(info.letters)),
            "completed_count": (buff.completed_count if buff else 0),
            "registration_msg_id": (buff.registration_msg_id if buff else info.registration_msg_id),
        }
        self._storage.save_for_user(user_id, job_dict, buff_dict)
        logger.debug(f"💾 Состояние сохранено для user_id={user_id}")
