# -*- coding: utf-8 -*-
"""
buffguild - VK Buff Guild Bot для автоматизации баффов в игре
"""

from .token_handler import TokenHandler
from .token_manager import OptimizedTokenManager
from .executor import AbilityExecutor
from .observer import ObserverBot
from .scheduler import Scheduler
from .health import TokenHealthMonitor
from .profile_manager import ProfileManager
from .telegram_admin import TelegramAdmin
from .vk_client import ResilientVKClient
from .models import Job, ParsedAbility
from .commands import (
    parse_baf_letters,
    parse_golosa_cmd,
    parse_doprasa_cmd,
    is_apo_cmd,
    is_baf_cancel_cmd
)
from .regexes import (
    RE_SUCCESS,
    RE_ALREADY,
    RE_NOT_APOSTLE,
    RE_NO_VOICES,
    RE_COOLDOWN,
    RE_REMAINING_SEC,
    RE_VOICES_GENERIC,
    RE_VOICES_ANY,
    RE_VOICES_IN_PARENTHESES,
    RE_PROFILE_LEVEL,
    RE_NOT_APOSTLE_OF_RACE,
    RE_ALREADY_BUFF,
    RE_OTHER_RACE,
    RE_ALREADY_RACE,
    RE_REQUIRES_ANCIENT_VOICE,
)
from .notifications import (
    build_registration_text,
    build_final_text
)
from .constants import (
    CLASS_ORDER,
    CLASS_ABILITIES,
    RACE_NAMES,
    RACE_EMOJIS,
    VK_API_BASE,
    VK_API_VERSION
)
from .utils import (
    jitter_sleep,
    normalize_text,
    timestamp_to_moscow,
    now_moscow,
    format_moscow_time,
    now_ts
)
from .validators import InputValidator
from .ability import build_ability_text_and_cd
from .job_storage import JobStorage
from .state_store import JobStateStore
from .logging_setup import setup_logging

__version__ = "2.0.0"
__author__ = "Buff Guild Team"

__all__ = [
    'TokenHandler',
    'OptimizedTokenManager',
    'AbilityExecutor',
    'ObserverBot',
    'Scheduler',
    'TokenHealthMonitor',
    'ProfileManager',
    'TelegramAdmin',
    'ResilientVKClient',
    'Job',
    'ParsedAbility',
    'JobStorage',
    'JobStateStore',
    'setup_logging',
    'parse_baf_letters',
    'parse_golosa_cmd',
    'parse_doprasa_cmd',
    'is_apo_cmd',
    'is_baf_cancel_cmd',
    'build_registration_text',
    'build_final_text',
    'build_ability_text_and_cd',
    'InputValidator',
    'jitter_sleep',
    'normalize_text',
    'timestamp_to_moscow',
    'now_moscow',
    'format_moscow_time',
    'now_ts',
    'CLASS_ORDER',
    'CLASS_ABILITIES',
    'RACE_NAMES',
    'RACE_EMOJIS',
    'VK_API_BASE',
    'VK_API_VERSION',
    'RE_SUCCESS',
    'RE_ALREADY',
    'RE_NOT_APOSTLE',
    'RE_NO_VOICES',
    'RE_COOLDOWN',
    'RE_REMAINING_SEC',
    'RE_VOICES_GENERIC',
    'RE_VOICES_ANY',
    'RE_VOICES_IN_PARENTHESES',
    'RE_PROFILE_LEVEL',
    'RE_NOT_APOSTLE_OF_RACE',
    'RE_ALREADY_BUFF',
    'RE_OTHER_RACE',
    'RE_ALREADY_RACE',
    'RE_REQUIRES_ANCIENT_VOICE',
]
