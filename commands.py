# -*- coding: utf-8 -*-
"""
Command parsing & normalization helpers.
"""
from __future__ import annotations

import re
from typing import Any, Dict, Optional, Tuple

from .constants import CLASS_ABILITIES
from .validators import InputValidator
from .utils import normalize_text


def parse_baf_letters(text: str) -> str:
    """Parse '!баф ...' and return up to 4 valid ability letters, or ''."""
    text_n = normalize_text(text or "")
    if not text_n.startswith("/баф"):
        return ""

    s = text_n[4:].strip()
    if not s:
        return ""

    s = s[:4]

    allowed = set()
    for cls in CLASS_ABILITIES.values():
        allowed.update(cls["abilities"].keys())

    out = "".join(ch for ch in s if ch in allowed)
    return out[:4]


def is_apo_cmd(text: str) -> bool:
    return normalize_text(text or "").startswith("/апо")


def is_baf_cancel_cmd(text: str) -> bool:
    return normalize_text(text or "") == "/баф отмена"


def parse_golosa_cmd(text: str) -> Optional[Tuple[None, int]]:
    """Parse '!голоса N' -> (None, n) or None."""
    t = (text or "").strip()
    if not normalize_text(t).startswith("/голоса"):
        return None

    parts = t.split()
    if len(parts) != 2:
        return None

    try:
        n = int(parts[1].strip())
    except Exception:
        return None

    return None, max(0, n)


def parse_doprasa_cmd(
    text: str,
    msg_item: Dict[str, Any],
) -> Optional[Tuple[str, Optional[str], Optional[int], str]]:
    """Parse '/допраса [race] [token_name?]'."""
    t = InputValidator.sanitize_text(text or "", max_length=50)
    if not normalize_text(t).startswith("/допраса"):
        return None

    parts = t.split()
    if len(parts) < 2 or len(parts) > 3:
        return None

    race = parts[1].strip().lower()
    if not InputValidator.validate_race_key(race):
        return None

    token_name: Optional[str] = None
    if len(parts) == 3:
        token_name = parts[2].strip()
        if not InputValidator.validate_token_name(token_name):
            return None

    original_timestamp: Optional[int] = None
    if "reply_message" in msg_item:
        original_timestamp = InputValidator.validate_timestamp(
            msg_item["reply_message"].get("date")
        )
    elif "fwd_messages" in msg_item and msg_item["fwd_messages"]:
        original_timestamp = InputValidator.validate_timestamp(
            msg_item["fwd_messages"][0].get("date")
        )

    return race, token_name, original_timestamp, (text or "")


# ============= ПАРСИНГ КОМАНДЫ /ВОСКРЕШЕНИЕ =============
def parse_resurrection_cmd(text: str) -> Optional[int]:
    """
    Парсит команду '/воскрешение [уровень]'
    
    Args:
        text: текст команды (например, "/воскрешение 25")
        
    Returns:
        int: уровень цели, или None если неверный формат
    """
    t = (text or "").strip()
    if not normalize_text(t).startswith("/воскрешение"):
        return None
    
    parts = t.split()
    if len(parts) != 2:
        return None
    
    try:
        level = int(parts[1].strip())
        if level < 1 or level > 1000:
            return None
        return level
    except ValueError:
        return None


def is_resurrection_cmd(text: str) -> bool:
    """Проверяет, является ли текст командой воскрешения"""
    return normalize_text(text or "").startswith("/воскрешение")
# =========================================================
