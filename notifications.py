# -*- coding: utf-8 -*-
"""
Pure message builders (no network, no state mutations).

This file should be stable: changes here only affect texts/formatting.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from .constants import RACE_NAMES, RACE_EMOJIS


def build_registration_text(letters: str) -> str:
    return (
        f"✅ Баф зарегистрирован: {letters}\n"
        f"📊 Ожидается бафов: {len(letters)}\n"
        f"📌 Для отмены напишите: !баф отмена"
    )


def _format_buff_line(user_id: int, info: Dict[str, Any], tm) -> Optional[str]:
    """
    Format one line for final notification.
    tm is used only to resolve token -> owner_vk_id for proper mentions.
    """
    token_name = info.get("token_name") or ""
    buff_name = (info.get("buff_name") or "").lower()
    buff_val = info.get("buff_value", 0)
    is_critical = info.get("is_critical", False)
    status = info.get("status", "SUCCESS")
    full_text = (info.get("full_text") or "")
    full_text_lower = full_text.lower()

    token = tm.get_token_by_name(token_name) if token_name else None
    owner_id = token.owner_vk_id if token and token.owner_vk_id else None

    # Mentions: prefer owner (caster), else requester
    base_link = f"[https://vk.ru/id{owner_id}|" if owner_id else f"[https://vk.ru/id{user_id}|"

    # Глобальный КД по цели: баф пропущен
    if status == "GLOBAL_COOLDOWN":
        # пример: "баф неудачи пропущен (КД)"
        nice_name = buff_name or "баф"
        return f"{base_link}⏳] баф {nice_name} пропущен (КД)"

    if status == "ALREADY_BUFF":
        return f"{base_link}🚫] Благословений не было"

    # Non-race buffs (удача/атака/защита)
    if "удач" in buff_name or "благословение удачи" in full_text_lower:
        # Удача: базовая иконка 🍀, при крите — 🍀 в конце текста
        if buff_val >= 150 or is_critical:
            core, emoji = "Удача +9!🍀", "🍀"
        else:
            core, emoji = "Удача +6!", "🍀"

    elif "атак" in buff_name or "благословение атаки" in full_text_lower:
        # Атака: базовая 🗡️, при крите — +30% и хвостовой 🍀
        if buff_val >= 150 or is_critical:
            core, emoji = "Атака +30%!🍀", "🗡️"
        else:
            core, emoji = "Атака +20%!", "🗡️"

    elif "защит" in buff_name or "благословение защиты" in full_text_lower:
        # Защита: базовая 🛡️, при крите — +30% и хвостовой 🍀
        if buff_val >= 150 or is_critical:
            core, emoji = "Защита +30%!🍀", "🛡️"
        else:
            core, emoji = "Защита +20%!", "🛡️"

    else:
        # Дополняем проверкой проклятий и паладинских абилок

        # 1) Проклятия (warlock)
        if "проклятие добычи" in full_text_lower or "проклятие добычи" in buff_name:
            if is_critical or buff_val >= 150:
                core, emoji = "Проклятие добычи -30%!🍀", "📉"
            else:
                core, emoji = "Проклятие добычи -20%!", "📉"

        elif "проклятие неудачи" in full_text_lower or "проклятие неудачи" in buff_name:
            # меняем ⚠️ на 🌀
            if is_critical or buff_val >= 150:
                core, emoji = "Проклятие неудачи +30%!🍀", "🌀"
            else:
                core, emoji = "Проклятие неудачи +20%!", "🌀"

        elif "проклятие боли" in full_text_lower or "проклятие боли" in buff_name:
            if is_critical or buff_val >= 150:
                core, emoji = "Проклятие боли +30%!🍀", "💢"
            else:
                core, emoji = "Проклятие боли +20%!", "💢"

        # 2) Очищения и воскрешения (paladin)
        elif "очищение огнем" in full_text_lower or "очищение огнем" in buff_name:
            core, emoji = "Очищение огнем", "🔥"

        elif "очищение светом" in full_text_lower or "очищение светом" in buff_name:
            core, emoji = "Очищение светом", "✨"

        elif full_text_lower.startswith("очищение") or buff_name == "очищение":
            # Полное очищение всех проклятий (буква 'и'), без крита
            core, emoji = "Очищение (сняты проклятия)", "☀️"

        elif "воскрешение" in full_text_lower or "воскрешение" in buff_name:
            core, emoji = "Воскрешение", "♻️"

        else:
            # Races (unified table)
            found_race_key = None
            for rk, rn in RACE_NAMES.items():
                if rn in buff_name or f"благословение {rn}" in full_text_lower:
                    found_race_key = rk
                    break

            if found_race_key:
                core = f"{RACE_NAMES.get(found_race_key, found_race_key).capitalize()}!"
                emoji = RACE_EMOJIS.get(found_race_key, "✨")
            else:
                core = f"{token_name or 'Благословение'} ({buff_val})"
                emoji = "✨"

    if status == "SUCCESS":
        return f"{base_link}{emoji}]{core}"
    return f"{base_link}🚫]{core}"


def build_final_text(user_id: int, tokens_info: List[Dict[str, Any]], tm) -> str:
    """
    Build the final notification text from collected token results.
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
    lines.append("🎉 Баф успешно выдан до этого!" if all_already and not any_success else "🎉 Баф успешно выдан!")

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

    lines.append(f"[https://vk.ru/id{user_id}|💰]Пока тест не Списано {total_spent} баллов")
    return "\n".join(lines).strip()
