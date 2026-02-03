# -*- coding: utf-8 -*-
from typing import Any, Dict

VK_API_BASE = "https://api.vk.com/method"
VK_API_VERSION = "5.131"

CLASS_ORDER = ["apostle", "warlock", "crusader", "light_incarnation"]

RACE_NAMES = {
    "ч": "человек",
    "г": "гоблин",
    "н": "нежить",
    "э": "эльф",
    "м": "гном",
    "д": "демон",
    "о": "орк",
}

CLASS_ABILITIES: Dict[str, Dict[str, Any]] = {
    "apostle": {
        "name": "Апостол",
        "prefix": "благословение",
        "uses_voices": True,
        "default_cooldown": 61,
        "abilities": {
            "а": "атаки",
            "з": "защиты",
            "у": "удачи",
            "ч": "человека",
            "г": "гоблина",
            "н": "нежити",
            "э": "эльфа",
            "м": "гнома",
            "д": "демона",
            "о": "орка",
        },
    },
    "warlock": {
        "name": "Проклинающий",
        "prefix": "проклятие",
        "uses_voices": True,
        "default_cooldown": 3600,
        "abilities": {
            "л": "неудачи",
            "б": "боли",
            "ю": "добычи",
        },
    },
    "crusader": {
        "name": "Паладин",
        "prefix": "",
        "uses_voices": True,
        "default_cooldown": None,
        "abilities": {
            "в": ("воскрешение", 6 * 60 * 60),
            "т": ("очищение огнем", 15 * 60 + 10),
        },
    },
    "light_incarnation": {
        "name": "Паладин",
        "prefix": "",
        "uses_voices": True,
        "default_cooldown": None,
        "abilities": {
            "и": ("очищение", 61),
            "в": ("воскрешение", 6 * 60 * 60),
            "с": ("очищение светом", 15 * 60 + 10),
        },
    },
    # ✅ ДОБАВЛЕНО: класс для Observer (не имеет способностей)
    "observer": {
        "name": "Наблюдатель",
        "prefix": "",
        "uses_voices": False,
        "default_cooldown": None,
        "abilities": {},
    },
}
