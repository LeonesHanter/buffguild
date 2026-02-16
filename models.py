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


@dataclass
class Job:
    sender_id: int
    trigger_text: str
    letters: str
    created_ts: float = field(default_factory=lambda: time.time())
    chat_id: Optional[int] = None
    abilities: List[ParsedAbility] = field(default_factory=list)
    cancelled: bool = False
    extra: Dict[str, Any] = field(default_factory=dict)
    registration_msg_id: Optional[int] = None  # ← ДОБАВЛЯЕМ ЭТО ПОЛЕ

    def is_cancelled(self) -> bool:
        return bool(self.cancelled)

    def mark_cancelled(self) -> None:
        self.cancelled = True
