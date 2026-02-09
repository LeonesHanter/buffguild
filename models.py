# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
import time


@dataclass
class ParsedAbility:
    key: str
    text: str
    cooldown: int
    uses_voices: bool
    processed: bool = False
    token_name: Optional[str] = None
    # сюда можно добавить любые доп. поля при необходимости


@dataclass
class Job:
    # если у тебя был id, можешь вернуть:
    # id: int
    sender_id: int
    trigger_text: str
    letters: str
    created_ts: float = field(default_factory=lambda: time.time())
    chat_id: Optional[int] = None
    abilities: List[ParsedAbility] = field(default_factory=list)
    # флаг отмены
    cancelled: bool = False
    # произвольные доп. данные
    extra: Dict[str, Any] = field(default_factory=dict)

    def is_cancelled(self) -> bool:
        return bool(self.cancelled)

    def mark_cancelled(self) -> None:
        self.cancelled = True
