# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Optional

@dataclass
class ParsedAbility:
    key: str
    text: str
    cooldown: int
    class_type: str
    uses_voices: bool
    processed: bool = False

@dataclass
class Job:
    sender_id: int
    trigger_text: str
    letters: str
    created_ts: float
