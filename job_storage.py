# -*- coding: utf-8 -*-
import json
import logging
import os
import threading
import time
from typing import Dict, Tuple, Any, Optional

logger = logging.getLogger(__name__)


class JobStorage:
    """
    Простое файловое хранилище активных бафов по user_id.
    Хранит чистые dict-и, без импортов из observer.py, чтобы избежать циклов.
    """

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()

    def _atomic_write(self, data: Dict[str, Any]) -> None:
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)

    def load_all(self) -> Dict[int, Tuple[Dict[str, Any], Optional[Dict[str, Any]]]]:
        """
        Возвращает:
            { user_id: (job_info_dict, buff_info_dict_or_None) }
        job_info_dict:
            {
                "job": { sender_id, trigger_text, letters, created_ts },
                "letters": str,
                "cmid": int|None,
                "message_id": int,
                "registration_time": float,
            }
        buff_info_dict:
            {
                "tokens_info": [...],
                "total_value": int,
                "expected_count": int,
                "completed_count": int,
            }
        """
        if not os.path.exists(self.path):
            return {}

        try:
            with open(self.path, "r", encoding="utf-8") as f:
                raw = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            logger.error(f"❌ JobStorage: ошибка чтения {self.path}: {e}")
            return {}

        result: Dict[int, Tuple[Dict[str, Any], Optional[Dict[str, Any]]]] = {}
        now = time.time()
        max_age = 3600  # не восстанавливаем бафы старше часа

        for k, v in raw.items():
            try:
                user_id = int(k)
                job_raw = v.get("job")
                buff_raw = v.get("buff")
                if not job_raw:
                    continue

                created_ts = float(job_raw.get("job", {}).get("created_ts", 0))
                if created_ts <= 0 or now - created_ts > max_age:
                    continue

                result[user_id] = (job_raw, buff_raw)
            except Exception as e:
                logger.error(f"❌ JobStorage: ошибка восстановления записи '{k}': {e}")
                continue

        if result:
            logger.info(f"📦 JobStorage: восстановлено активных бафов: {len(result)}")
        return result

    def save_for_user(
        self,
        user_id: int,
        job_info: Dict[str, Any],
        buff_info: Optional[Dict[str, Any]],
    ) -> None:
        """Сохранить/обновить активный баф для пользователя (в виде dict-ов)."""
        with self._lock:
            data: Dict[str, Any] = {}
            if os.path.exists(self.path):
                try:
                    with open(self.path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                except (OSError, json.JSONDecodeError):
                    data = {}

            data[str(user_id)] = {
                "job": job_info,
                "buff": buff_info,
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
