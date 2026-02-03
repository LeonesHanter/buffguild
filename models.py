# -*- coding: utf-8 -*-
from dataclasses import dataclass, field
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
    # При желании можно добавить сюда cmid, peer_id и др. поля
    cmid: Optional[int] = None
    peer_id: Optional[int] = None


@dataclass
class HealthSummary:
    """Опциональная модель для агрегированных health-отчётов."""
    total_tokens: int
    healthy_tokens: int
    warning_tokens: int
    error_tokens: int
    total_buffs: int = 0
    total_attempts: int = 0
    issues_top: list = field(default_factory=list)
