# -*- coding: utf-8 -*-
"""
Pure message builders (no network, no state mutations).

This file should be stable: changes here only affect texts/formatting.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .constants import RACE_NAMES, RACE_EMOJIS

logger = logging.getLogger(__name__)


def build_registration_text(letters: str) -> str:
    return (
        f"✅ Баф зарегистрирован: {letters}\n"
        f"📊 Ожидается бафов: {len(letters)}\n"
        f"📌 Для отмены напишите: /баф отмена"
    )


def _format_buff_line(user_id: int, info: Dict[str, Any], tm) -> Optional[str]:
    """
    Format one line for final notification.
    tm is used only to resolve token -> owner_vk_id for proper mentions.

    user_id трактуем как id того, кто заказывал баф (sender/target),
    но во всех строках бафа приоритет у owner_id токена.
    """
    token_name = info.get("token_name") or ""
    buff_name = (info.get("buff_name") or "").lower()
    buff_val = info.get("buff_value", 0)
    is_critical = info.get("is_critical", False)
    status = info.get("status", "SUCCESS")
    full_text = (info.get("full_text") or "")
    full_text_lower = full_text.lower()

    token = tm.get_token_by_name(token_name) if token_name else None

    owner_id = None
    if token:
        owner_id = token.owner_vk_id

        if not owner_id or owner_id <= 0:
            logger.debug(f"🔍 Токен {token.name}: owner_vk_id не установлен, вызываем fetch_owner_id_lazy()")
            owner_id = token.fetch_owner_id_lazy()

            if owner_id and owner_id > 0:
                token.owner_vk_id = owner_id
                token.mark_for_save()
                logger.debug(f"✅ Токен {token.name}: определен owner_vk_id={owner_id}")

    if not owner_id or owner_id <= 0:
        owner_id = user_id
        logger.debug(f"⚠️ Токен {token_name}: не удалось определить owner_vk_id, используем заказчика={owner_id}")

    base_link = f"[https://vk.ru/id{owner_id}|"

    if status == "GLOBAL_COOLDOWN":
        nice_name = buff_name or "баф"
        return f"{base_link}⏳] баф {nice_name} пропущен (КД)"

    if status == "ALREADY_BUFF":
        return f"{base_link}🚫] Благословений не было"

    # ============= ВОСКРЕШЕНИЕ =============
    if buff_name == "воскрешение" or "воскрешение" in full_text_lower:
        core, emoji = "Воскрешение", "♻️"
    # =======================================

    # ============= ОЧИЩЕНИЕ =============
    elif buff_name == "очищение огнем" or "очищение огнем" in full_text_lower:
        if is_critical or "критическое" in full_text_lower or "🍀" in full_text:
            core, emoji = "Очищение огнем!🍀", "🔥"
        else:
            core, emoji = "Очищение огнем", "🔥"

    elif buff_name == "очищение светом" or "очищение светом" in full_text_lower:
        if is_critical or "критическое" in full_text_lower or "🍀" in full_text:
            core, emoji = "Очищение светом!🍀", "✨"
        else:
            core, emoji = "Очищение светом", "✨"

    elif buff_name == "очищение" or full_text_lower.startswith("очищение"):
        core, emoji = "Очищение (сняты проклятия)", "☀️"
    # ====================================

    # ----- Warlock / Paladin / Races -----
    # СНАЧАЛА проверяем проклятия (чтобы "проклятие неудачи" не попало в "удач")

    # 1) Проклятия (warlock) - ТОЧНЫЕ ПРОВЕРКИ
    elif buff_name == "проклятие добычи" or "проклятие добычи" in full_text_lower:
        if "уменьшена на 30%" in full_text_lower or (is_critical and buff_val >= 150):
            core, emoji = "Проклятие добычи -30%!🍀", "📉"
        else:
            core, emoji = "Проклятие добычи -20%!", "📉"

    elif buff_name == "проклятие неудачи" or "проклятие неудачи" in full_text_lower:
        if "увеличена на 30%" in full_text_lower or (is_critical and buff_val >= 150):
            core, emoji = "Проклятие неудачи +30%!🍀", "🌀"
        else:
            core, emoji = "Проклятие неудачи +20%!", "🌀"

    elif buff_name == "проклятие боли" or "проклятие боли" in full_text_lower:
        if "увеличена на 30%" in full_text_lower or (is_critical and buff_val >= 150):
            core, emoji = "Проклятие боли +30%!🍀", "💢"
        else:
            core, emoji = "Проклятие боли +20%!", "💢"

    # ----- Non-race buffs (удача/атака/защита) -----
    elif buff_name == "благословение удачи" or "благословение удачи" in full_text_lower:
        if buff_val >= 150 or is_critical:
            core, emoji = "Удача +9!🍀", "🍀"
        else:
            core, emoji = "Удача +6!", "🍀"

    elif buff_name == "благословение атаки" or "благословение атаки" in full_text_lower:
        if buff_val >= 150 or is_critical:
            core, emoji = "Атака +30%!🍀", "🗡️"
        else:
            core, emoji = "Атака +20%!", "🗡️"

    elif buff_name == "благословение защиты" or "благословение защиты" in full_text_lower:
        if buff_val >= 150 or is_critical:
            core, emoji = "Защита +30%!🍀", "🛡️"
        else:
            core, emoji = "Защита +20%!", "🛡️"

    else:
        # 3) Races (unified table)
        ability_key = info.get("ability_key", "")
        
        if ability_key in RACE_NAMES:
            race_name = RACE_NAMES.get(ability_key, ability_key)
            core = f"{race_name.capitalize()}!"
            emoji = RACE_EMOJIS.get(ability_key, "✨")
            logger.debug(f"🏆 Определена раса по ability_key='{ability_key}': {core}, эмодзи={emoji}")
        else:
            found_race_key = None
            for rk, rn in RACE_NAMES.items():
                if buff_name == f"благословение {rn}" or f"благословение {rn}" in buff_name:
                    found_race_key = rk
                    break
                if f"благословение {rn}" in full_text_lower:
                    found_race_key = rk
                    break
            
            if found_race_key:
                race_name = RACE_NAMES.get(found_race_key, found_race_key)
                core = f"{race_name.capitalize()}!"
                emoji = RACE_EMOJIS.get(found_race_key, "✨")
                logger.debug(f"🏆 Определена раса по тексту: {core}, эмодзи={emoji}")
            else:
                core = f"{token_name or 'Благословение'} ({buff_val})"
                emoji = "✨"
                logger.debug(f"ℹ️ Не найдена раса для ability_key='{ability_key}', buff_name='{buff_name}'")

    if status == "SUCCESS":
        return f"{base_link}{emoji}]{core}"
    return f"{base_link}🚫]{core}"


def build_final_text(user_id: int, tokens_info: List[Dict[str, Any]], tm) -> str:
    """
    Build the final notification text from collected token results.

    user_id – тот, кто заказал баф (с него считаем "баллы" в конце).
    Ссылки бафов (_format_buff_line) приоритетно ведут на owner токена.
    """
    if not tokens_info:
        return ""

    all_already = True
    any_success = False
    for info in tokens_info:
        status = info.get("status", "SUCCESS")
        if status == "SUCCESS":
            any_success = True
            all_already = False
        elif status == "ALREADY_BUFF":
            pass
        else:
            all_already = False

    lines: List[str] = []
    lines.append(
        "🎉 Баф успешно выдан до этого!" if all_already and not any_success else "🎉 Баф успешно выдан!"
    )

    total_spent = 0
    for info in tokens_info:
        line = _format_buff_line(user_id, info, tm)
        if line:
            lines.append(line)
        if info.get("status", "SUCCESS") == "SUCCESS":
            try:
                total_spent += int(info.get("buff_value", 0) or 0)
            except Exception:
                pass

    lines.append(f"[https://vk.ru/id{user_id}|💰]Списано {total_spent} баллов")
    return "\n".join(lines).strip()
