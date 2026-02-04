# -*- coding: utf-8 -*-
import html
import re
from typing import Any, Optional
from .constants import RACE_NAMES

class InputValidator:
    """Валидатор входных данных"""

    @staticmethod
    def sanitize_text(text: str, max_length: int = 100) -> str:
        """Очистка текста (исправлено): сначала чистим опасные паттерны, потом экранируем HTML"""
        if not text:
            return ""

        text = text.strip()[:max_length]

        # Сначала убираем опасные подстроки В НЕЭКРАНИРОВАННОМ виде
        dangerous = ["<script>", "</script>", "javascript:", "onload=", "onerror="]
        low = text.lower()
        for d in dangerous:
            # удаляем case-insensitive через regex
            low_pat = re.escape(d)
            text = re.sub(low_pat, "", text, flags=re.IGNORECASE)

        # Потом экранируем HTML
        text = html.escape(text)
        return text

    @staticmethod
    def validate_race_key(race_key: str) -> bool:
        if not race_key or len(race_key) != 1:
            return False
        return race_key in RACE_NAMES

    @staticmethod
    def validate_token_name(name: str) -> bool:
        if not name or len(name) > 50:
            return False
        return bool(re.match(r"^[a-zA-Zа-яА-Я0-9_\- ]+$", name))

    @staticmethod
    def validate_message_id(msg_id: Any) -> Optional[int]:
        try:
            msg_id_int = int(msg_id)
            if msg_id_int <= 0 or msg_id_int > 2**31:
                return None
            return msg_id_int
        except (ValueError, TypeError):
            return None

    @staticmethod
    def validate_timestamp(ts: Any) -> Optional[int]:
        """Отдельная валидация timestamp (unix seconds)"""
        try:
            ts_int = int(ts)
            if ts_int <= 0 or ts_int > 2**31:
                return None
            return ts_int
        except (ValueError, TypeError):
            return None
