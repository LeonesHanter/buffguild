# -*- coding: utf-8 -*-
from typing import Any, Dict, Optional, Tuple
from .constants import CLASS_ABILITIES

def build_ability_text_and_cd(class_type: str, key: str) -> Optional[Tuple[str, int, bool]]:
    c = CLASS_ABILITIES.get(class_type)
    if not c or key not in c["abilities"]:
        return None
    uses_voices = bool(c.get("uses_voices", False))
    v = c["abilities"][key]
    if isinstance(v, tuple):
        return str(v[0]), int(v[1]), uses_voices
    prefix = c.get("prefix", "")
    default_cd = int(c.get("default_cooldown", 61))
    text = f"{prefix} {v}".strip() if prefix else str(v)
    return text, default_cd, uses_voices
