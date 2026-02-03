# -*- coding: utf-8 -*-
import json
import logging
import os
import threading
import time
from dataclasses import asdict
from typing import Dict, Tuple, Any, Optional

from .observer import ActiveJobInfo, BuffResultInfo  # убедись, что пути совпадают

logger = logging.getLogger(__name__)


class JobStorage:
    """Простое файловое хранилище активных бафов по user_id."""

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()

    def _atomic_write(self, data: Dict[str, Any]) -> None:
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)

    def load_all(self) -> Dict[int, Tuple[ActiveJobInfo, BuffResultInfo]]:
        if not os.path.exists(self.path):
            return {}

        try:
            with open(self.path, "r", encoding="utf-8") as f:
                raw = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            logger.error(f"❌ JobStorage: ошибка чтения {self.path}: {e}")
            return {}

        result: Dict[int, Tuple[ActiveJobInfo, BuffResultInfo]] = {}
        now = time.time()
        max_age = 3600  # не восстанавливаем бафы старше часа

        for k, v in raw.items():
            try:
                user_id = int(k)
                job_raw = v.get("job")
                buff_raw = v.get("buff")
                if not job_raw or not buff_raw:
                    continue

                created_ts = float(job_raw.get("created_ts", 0))
                if created_ts <= 0 or now - created_ts > max_age:
                    continue

                job_info = ActiveJobInfo(
                    job=job_raw["job"],  # сюда мы положим dataclass Job как dict в Observer
                    letters=job_raw["letters"],
                    cmid=job_raw.get("cmid"),
                    message_id=job_raw.get("message_id", 0),
                    registration_time=job_raw.get("registration_time", created_ts),
                )
                buff_info = BuffResultInfo(
                    tokens_info=buff_raw.get("tokens_info", []),
                    total_value=buff_raw.get("total_value", 0),
                    expected_count=buff_raw.get("expected_count", 0),
                    completed_count=buff_raw.get("completed_count", 0),
                )
                result[user_id] = (job_info, buff_info)
            except Exception as e:
                logger.error(f"❌ JobStorage: ошибка восстановления записи '{k}': {e}")
                continue

        if result:
            logger.info(f"📦 JobStorage: восстановлено активных бафов: {len(result)}")
        return result

    def save_for_user(
        self,
        user_id: int,
        job_info: ActiveJobInfo,
        buff_info: Optional[BuffResultInfo],
    ) -> None:
        """Сохранить/обновить активный баф для пользователя."""
        with self._lock:
            data: Dict[str, Any] = {}
            if os.path.exists(self.path):
                try:
                    with open(self.path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                except (OSError, json.JSONDecodeError):
                    data = {}

            job_payload = {
                "job": {
                    "sender_id": job_info.job.sender_id,
                    "trigger_text": job_info.job.trigger_text,
                    "letters": job_info.job.letters,
                    "created_ts": job_info.job.created_ts,
                },
                "letters": job_info.letters,
                "cmid": job_info.cmid,
                "message_id": job_info.message_id,
                "registration_time": job_info.registration_time,
            }
            buff_payload = (
                {
                    "tokens_info": buff_info.tokens_info,
                    "total_value": buff_info.total_value,
                    "expected_count": buff_info.expected_count,
                    "completed_count": buff_info.completed_count,
                }
                if buff_info
                else None
            )

            data[str(user_id)] = {
                "job": job_payload,
                "buff": buff_payload,
            }
            self._atomic_write(data)

    def delete_for_user(self, user_id: int) -> None:
        """Удалить активный баф пользователя из хранилища."""
        with self._lock:
            if not os.path.exists(self.path):
                return
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except (OSError, json.JSONDecodeError):
                return

            changed = False
            if str(user_id) in data:
                del data[str(user_id)]
                changed = True

            if changed:
                if data:
                    self._atomic_write(data)
                else:
                    try:
                        os.remove(self.path)
                    except OSError:
                        pass
