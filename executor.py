# -*- coding: utf-8 -*-
import logging
import re
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from .constants import RACE_NAMES
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
from .token_handler import TokenHandler
from .models import ParsedAbility, Job

logger = logging.getLogger(__name__)


class AbilityExecutor:
    def __init__(self, tm):
        self.tm = tm
        self._target_lock: Dict[int, threading.Lock] = {}

    def _lock_for_target(self, peer_id: int) -> threading.Lock:
        if peer_id not in self._target_lock:
            self._target_lock[peer_id] = threading.Lock()
        return self._target_lock[peer_id]

    def find_trigger_in_token_source(self, token: TokenHandler, job: Job) -> Tuple[Optional[int], Optional[int]]:
        want_text = (job.trigger_text or "").strip().lower()
        if not want_text:
            return None, None

        msgs = token.get_history_cached(token.source_peer_id, count=30)
        for m in msgs:
            from_id = int(m.get("from_id", 0))
            if from_id != job.sender_id:
                continue
            txt = (m.get("text", "") or "").strip().lower()
            if txt == want_text:
                mid = int(m.get("id", 0))
                cmid = m.get("conversation_message_id")
                cmid_int = (
                    int(cmid)
                    if isinstance(cmid, int) or (isinstance(cmid, str) and str(cmid).isdigit())
                    else None
                )
                return mid, cmid_int

        return None, None

    def _parse_new_messages(
        self,
        msgs: List[Dict[str, Any]]
    ) -> Tuple[str, Optional[int], Optional[int], str]:
        remaining = None
        voices_val = None
        cooldown_hint = False
        full_response_text = ""

        logger.debug(f"🔍 Начало парсинга {len(msgs)} сообщений")

        # Собираем ВСЕ тексты для анализа
        all_texts = []
        
        for m in msgs:
            text = str(m.get("text", "")).strip()
            text_l = text.lower()
            logger.debug(f"📝 Сообщение для парсинга: {text[:100]}...")
            
            all_texts.append(text)  # Сохраняем все тексты

            mm = RE_REMAINING_SEC.search(text)
            if mm:
                try:
                    remaining = int(mm.group(1))
                    if "социальные эффекты" in text_l:
                        cooldown_hint = True
                    logger.debug(f"⏰ Нашли remaining: {remaining}")
                except Exception as e:
                    logger.error(f"❌ Ошибка парсинга remaining: {e}")

            if voices_val is None:
                vm = RE_VOICES_GENERIC.search(text)
                if vm:
                    try:
                        voices_val = int(vm.group(1))
                        logger.info(f"✅ Нашли голоса ({voices_val}) с RE_VOICES_GENERIC")
                    except Exception as e:
                        logger.error(f"❌ Ошибка парсинга голосов с RE_VOICES_GENERIC: {e}")

            if voices_val is None:
                vm = RE_VOICES_ANY.search(text)
                if vm:
                    try:
                        voices_val = int(vm.group(1))
                        logger.info(f"✅ Нашли голоса ({voices_val}) с RE_VOICES_ANY")
                    except Exception as e:
                        logger.error(f"❌ Ошибка парсинга голосов с RE_VOICES_ANY: {e}")

            if voices_val is None:
                vm = RE_VOICES_IN_PARENTHESES.search(text)
                if vm:
                    try:
                        voices_val = int(vm.group(1))
                        logger.info(f"✅ Нашли голоса ({voices_val}) в скобках")
                    except Exception as e:
                        logger.error(f"❌ Ошибка парсинга голосов в скобках: {e}")

        # ПОСЛЕ парсинга всех сообщений, выбираем лучший текст
        # Ищем текст с результатом (содержит эмодзи или детали)
        result_candidates = []
        
        for text in all_texts:
            text_lower = text.lower()
            # Пропускаем триггеры (короткие или с ...)
            if len(text) < 20 or "..." in text:
                continue
                
            # Ищем тексты с результатом
            if ("🌀" in text or "✨" in text or "☀" in text or 
                "на вас наложено" in text_lower or "на Вас наложено" in text or
                "уменьшена на" in text_lower or "увеличена на" in text_lower or
                "повышена на" in text_lower or "🍀" in text):
                result_candidates.append(text)

        if result_candidates:
            # Берем самое длинное сообщение (скорее всего это полный результат)
            full_response_text = max(result_candidates, key=len)
            logger.debug(f"📋 Выбран текст результата ({len(full_response_text)} chars): {full_response_text[:200]}...")
        elif all_texts:
            # Иначе берем последнее сообщение
            full_response_text = all_texts[-1]
            logger.debug(f"📋 Взяли последнее сообщение: {full_response_text[:200]}...")

        # Порядок проверки ВАЖЕН: от специфичных ошибок к общим, успех - ближе к концу
        for m in msgs:
            text = str(m.get("text", "")).strip()
            logger.debug(f"🔍 Проверяем статус в сообщении: {text[:100]}...")

            # 1. Самые специфичные ошибки (проверять ПЕРВЫМИ!)
            if RE_NOT_APOSTLE_OF_RACE.search(text):
                matched = RE_NOT_APOSTLE_OF_RACE.search(text).group(0)
                logger.info(
                    f"🔍 Статус: NOT_APOSTLE_OF_RACE - "
                    f"'{RE_NOT_APOSTLE_OF_RACE.pattern}' сработало на '{matched}'"
                )
                return "PASS_TO_NEXT_APOSTLE", remaining, voices_val, full_response_text

            if RE_ALREADY_BUFF.search(text):
                matched = RE_ALREADY_BUFF.search(text).group(0)
                logger.info(
                    f"🔍 Статус: ALREADY_BUFF - "
                    f"'{RE_ALREADY_BUFF.pattern}' сработало на '{matched}'"
                )
                return "ALREADY_BUFF", remaining, voices_val, full_response_text

            if RE_OTHER_RACE.search(text):
                matched = RE_OTHER_RACE.search(text).group(0)
                logger.info(
                    f"🔍 Статус: OTHER_RACE - "
                    f"'{RE_OTHER_RACE.pattern}' сработало на '{matched}'"
                )
                return "PASS_TO_NEXT_APOSTLE", remaining, voices_val, full_response_text

            if RE_ALREADY_RACE.search(text):
                matched = RE_ALREADY_RACE.search(text).group(0)
                logger.info(
                    f"🔍 Статус: ALREADY_BUFF - "
                    f"'{RE_ALREADY_RACE.pattern}' сработало на '{matched}'"
                )
                return "ALREADY_BUFF", remaining, voices_val, full_response_text

            # 2. "Требуется Голос Древних" - специфичная ошибка для голосов
            if RE_REQUIRES_ANCIENT_VOICE.search(text):
                matched = RE_REQUIRES_ANCIENT_VOICE.search(text).group(0)
                logger.info(f"🔍 Статус: NO_VOICES - требуется Голос Древних")
                return "NO_VOICES", remaining, voices_val, full_response_text

            # 3. Общие ошибки (проверять ПОСЛЕ специфичных)
            if RE_NO_VOICES.search(text):
                matched = RE_NO_VOICES.search(text).group(0)
                logger.info(
                    f"🔍 Статус: NO_VOICES - '{RE_NO_VOICES.pattern}' сработало на '{matched}'"
                )
                return "NO_VOICES", remaining, voices_val, full_response_text

            if RE_NOT_APOSTLE.search(text):
                matched = RE_NOT_APOSTLE.search(text).group(0)
                logger.info(
                    f"🔍 Статус: NOT_APOSTLE - '{RE_NOT_APOSTLE.pattern}' сработало на '{matched}'"
                )
                return "NOT_APOSTLE", remaining, voices_val, full_response_text

            # 4. Уже/КД (проверять перед успехом, чтобы не спутать)
            if RE_ALREADY.search(text):
                matched = RE_ALREADY.search(text).group(0)
                logger.info(
                    f"🔍 Статус: ALREADY - '{RE_ALREADY.pattern}' сработало на '{matched}'"
                )
                return "ALREADY", remaining, voices_val, full_response_text

            if RE_COOLDOWN.search(text):
                matched = RE_COOLDOWN.search(text).group(0)
                if len(matched) > 50:
                    matched = matched[:50] + "..."
                logger.info(
                    f"🔍 Статус: COOLDOWN - '{RE_COOLDOWN.pattern}' сработало на '{matched}'"
                )
                return "COOLDOWN", remaining, voices_val, full_response_text

            # 5. УСПЕХ (проверять в САМОМ КОНЦЕ, после всех ошибок)
            if RE_SUCCESS.search(text):
                matched = RE_SUCCESS.search(text).group(0)
                logger.info(
                    f"🔍 Статус: SUCCESS - '{RE_SUCCESS.pattern}' сработало на '{matched}'"
                )
                return "SUCCESS", remaining, voices_val, full_response_text

        # Fallback: если ничего не нашли, но есть remaining и cooldown_hint
        if remaining is not None and cooldown_hint:
            logger.info(f"🔍 Статус: COOLDOWN (fallback, remaining={remaining})")
            return "COOLDOWN", remaining, voices_val, full_response_text

        # Если совсем ничего не распознали
        logger.info("🔍 Статус: UNKNOWN (ни одна регулярка не сработала)")
        return "UNKNOWN", remaining, voices_val, full_response_text

    def _parse_buff_value(self, text: str) -> Tuple[int, bool]:
        if not text:
            logger.debug("📭 Текст для анализа крита пустой")
            return 100, False

        text_lower = text.lower()
        is_critical = False
        buff_value = 100

        # 0. Проверяем на критические очищения
        if "очищение" in text_lower:
            if "критическое" in text_lower or "🍀" in text:
                is_critical = True
                buff_value = 150
                logger.info("✨ Критическое очищение: 150 голосов")
                return buff_value, is_critical
            else:
                buff_value = 100
                logger.info("✨ Обычное очищение: 100 голосов")
                return buff_value, is_critical

        # 0.5. Проверяем на критические проклятия (ДОБАВИТЬ ЭТО!)
        if "проклятие" in text_lower:
            if "критическое" in text_lower or "🍀" in text:
                is_critical = True
                buff_value = 150
                logger.info("👿 Критическое проклятие: 150 голосов")
            else:
                buff_value = 100
                logger.info("👿 Обычное проклятие: 100 голосов")
            
            # Пробуем найти процент для проклятий
            if "уменьшена на" in text_lower or "увеличена на" in text_lower:
                percent_patterns = [
                    r"уменьшена на\s+(\d{1,3})\s*%",
                    r"увеличена на\s+(\d{1,3})\s*%", 
                    r"на\s+(\d{1,3})\s*%",
                    r"(\+?\d{1,3})\s*%"
                ]
                
                for pattern in percent_patterns:
                    match = re.search(pattern, text_lower)
                    if match:
                        try:
                            percent = int(match.group(1))
                            if percent == 30:
                                is_critical = True
                                buff_value = 150
                                logger.info(f"👿 Критическое проклятие {percent}%: 150 голосов")
                            elif percent == 20:
                                is_critical = False
                                buff_value = 100
                                logger.info(f"👿 Обычное проклятие {percent}%: 100 голосов")
                            break
                        except Exception as e:
                            logger.debug(f"❌ Ошибка парсинга процента проклятия: {e}")
            
            return buff_value, is_critical

        # 1. Удача в единицах — приоритет
        luck_match = re.search(r"удача\s+повышена\s+на\s+(\d{1,3})", text_lower)
        if luck_match:
            try:
                luck_val = int(luck_match.group(1))
                if luck_val == 9:
                    logger.info("🍀 Баф удачи: 9 единиц = 150 голосов (крит)")
                    return 150, True
                if luck_val == 6:
                    logger.info("🍀 Баф удачи: 6 единиц = 100 голосов (обычный)")
                    return 100, False
                logger.info(
                    f"🍀 Баф удачи: {luck_val} единиц → по умолчанию 100 голосов (обычный)"
                )
                return 100, False
            except Exception as e:
                logger.debug(f"❌ Ошибка парсинга удачи в единицах: {e}")

        # 2. Расовые бафы — фиксированная стоимость
        race_keywords = [
            "человек",
            "гоблин",
            "нежить",
            "эльф",
            "гном",
            "демон",
            "орк",
            "людей",
            "гоблинов",
            "нежити",
            "эльфов",
            "гномов",
            "демонов",
            "орков",
        ]
        if any(race in text_lower for race in race_keywords):
            logger.debug(f"📊 Расовый баф: {text[:50]}...")
            return 100, False

        # 3. Общие проценты (атака/защита/прочее)
        percent_patterns = [
            r"(\+?\d{1,3})\s*%",
            r"на\s+(\d{1,3})\s*%",
            r"повышена\s+на\s+(\d{1,3})\s*%",
            r"увеличена\s+на\s+(\d{1,3})\s*%",
            r"повышена\s+(\d{1,3})\s*%",
            r"увеличена\s+(\d{1,3})\s*%",
            r"броня повышена на (\d{1,3})%",
            r"атака повышена на (\d{1,3})%",
        ]

        found_percent = None

        for pattern in percent_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    found_percent = int(match.group(1))
                    logger.info(
                        f"🔍 Найден процент в тексте: {found_percent}% (паттерн: {pattern})"
                    )
                    break
                except Exception as e:
                    logger.debug(f"❌ Ошибка парсинга процента: {e}")
                    continue

        if found_percent is not None:
            if found_percent == 30:
                is_critical = True
                buff_value = 150
                logger.info(
                    f"🎯 Определен крит баф: {found_percent}% = {buff_value} голосов"
                )
            elif found_percent == 20:
                is_critical = False
                buff_value = 100
                logger.info(
                    f"📊 Обычный баф: {found_percent}% = {buff_value} голосов"
                )
            else:
                buff_value = 100
                is_critical = "критический" in text_lower or "🍀" in text_lower
                logger.info(
                    f"📈 Баф {found_percent}%: значение={buff_value}, крит={is_critical}"
                )
        else:
            logger.debug(f"📝 Процент не найден в тексте: {text[:100]}...")

        if not is_critical and ("критический баф" in text_lower or "🍀" in text_lower):
            is_critical = True
            buff_value = 150
            logger.info("🍀 Определен крит баф по тексту: 'критический' или '🍀'")

        if any(x in text_lower for x in ["атаки", "защиты"]):
            buff_value = 150 if is_critical else 100
            logger.debug(
                f"⚔️ Баф атаки/защиты: значение={buff_value}, крит={is_critical}"
            )

        logger.info(
            f"📊 Итог парсинга бафа: значение={buff_value}, крит={is_critical}, "
            f"текст='{text[:80]}...'"
        )
        return buff_value, is_critical

    def execute_one(
        self,
        token: TokenHandler,
        ability: ParsedAbility,
        job: Job
    ) -> Tuple[bool, str, Optional[Dict]]:
        with token._lock:
            observer_token = self.tm.get_observer()
            if observer_token and (
                token.id == observer_token.id or token.name == "Observer"
            ):
                logger.warning(
                    f"⛔ {token.name} является Observer и не должен участвовать в бафах"
                )
                token.increment_buff_stats(False)
                return False, "OBSERVER_CANNOT_BUFF", None

            if not token.enabled:
                token.increment_buff_stats(False)
                return False, "DISABLED", None

            if token.is_captcha_paused():
                token.increment_buff_stats(False)
                return False, "CAPTCHA_PAUSED", None

            if token.needs_manual_voices:
                token.increment_buff_stats(False)
                return False, "NEEDS_MANUAL_VOICES", None

            # Автообновление профиля, если способность тратит голоса и локально 0
            if ability.uses_voices and token.voices <= 0:
                logger.info(f"🔄 {token.name}: voices=0, пробуем refresh_profile перед бафом")
                if self.refresh_profile(token) and token.voices > 0:
                    logger.info(f"✅ {token.name}: после refresh_profile голосов стало {token.voices}")
                else:
                    token.increment_buff_stats(False)
                    return False, "NO_VOICES_LOCAL", None

            can_social, rem_social = token.can_use_social()
            if not can_social:
                token.increment_buff_stats(False)
                return False, f"SOCIAL_COOLDOWN({int(rem_social)}s)", None

            can, rem = token.can_use_ability(ability.key)
            if not can:
                token.increment_buff_stats(False)
                return False, f"LOCAL_COOLDOWN({int(rem)}s)", None

            trigger_mid, trigger_cmid = self.find_trigger_in_token_source(token, job)
            if not trigger_mid:
                token.increment_buff_stats(False)
                return False, "TRIGGER_NOT_FOUND_IN_SOURCE", None

            target_lock = self._lock_for_target(token.target_peer_id)
            with target_lock:
                before = token.get_history_cached(token.target_peer_id, count=1)
                last_id_before = before[0]["id"] if before else 0

                ok, send_status = token.send_to_peer(
                    token.target_peer_id,
                    ability.text,
                    forward_msg_id=trigger_mid,
                )

                if not ok:
                    token.increment_buff_stats(False)
                    return False, send_status, None

                token.invalidate_cache(token.target_peer_id)

                poll_interval = float(self.tm.settings.get("poll_interval", 2.0))
                poll_count = int(self.tm.settings.get("poll_count", 20))

                buff_response_text = ""

                for i in range(poll_count):
                    time.sleep(poll_interval * (1 + i * 0.2))
                    history = token.get_history_cached(token.target_peer_id, count=25)
                    new_msgs = [
                        m
                        for m in history
                        if int(m.get("id", 0)) > last_id_before
                    ]
                    if not new_msgs:
                        continue

                    status, remaining, voices_val, full_response_text = (
                        self._parse_new_messages(list(reversed(new_msgs)))
                    )

                    if full_response_text and not buff_response_text:
                        buff_response_text = full_response_text
                        logger.debug(
                            f"📋 Получен полный текст ответа: {full_response_text[:200]}..."
                        )

                    if voices_val is not None:
                        logger.info(
                            f"🗣️ {token.name}: обновление голосов "
                            f"{token.voices} → {voices_val}"
                        )
                        token.update_voices_from_system(voices_val)
                        token.mark_for_save()

                    # 🆕 Обработка PASS_TO_NEXT_APOSTLE
                    if status == "PASS_TO_NEXT_APOSTLE":
                        # Это NOT_APOSTLE_OF_RACE или OTHER_RACE
                        # Не наказываем токен, просто сообщаем scheduler'у
                        return False, "PASS_TO_NEXT_APOSTLE", None

                    if status == "NOT_APOSTLE_OF_RACE":
                        if ability.key in RACE_NAMES:
                            before_cnt = len(token.temp_races)
                            token.temp_races = [
                                tr for tr in token.temp_races if tr["race"] != ability.key
                            ]
                            if len(token.temp_races) != before_cnt:
                                self.tm.mark_for_save()
                                self.tm.update_race_index(token)
                            logger.warning(
                                f"🗑️ {token.name}: удалена временная раса "
                                f"'{ability.key}' (NOT_APOSTLE_OF_RACE)"
                            )

                        token.set_ability_cooldown(ability.key, 300)
                        token.set_social_cooldown(300)
                        return False, "PASS_TO_NEXT_APOSTLE", None  # 🆕 Теперь PASS_TO_NEXT

                    if status == "ALREADY_BUFF":
                        token.set_social_cooldown(62)
                        return False, "ALREADY_BUFF", None

                    if status == "OTHER_RACE":
                        token.set_social_cooldown(62)
                        return False, "PASS_TO_NEXT_APOSTLE", None  # 🆕 Теперь PASS_TO_NEXT

                    if status == "NOT_APOSTLE":
                        if ability.key in RACE_NAMES:
                            before_cnt = len(token.temp_races)
                            token.temp_races = [
                                tr for tr in token.temp_races if tr["race"] != ability.key
                            ]
                            if len(token.temp_races) != before_cnt:
                                self.tm.mark_for_save()
                                self.tm.update_race_index(token)
                            logger.warning(
                                f"🗑️ {token.name}: удалена временная раса "
                                f"'{ability.key}' (NOT_APOSTLE)"
                            )

                        token.set_ability_cooldown(ability.key, 300)
                        token.set_social_cooldown(300)
                        token.increment_buff_stats(False)
                        return False, "NOT_APOSTLE", None

                    if status == "SUCCESS":
                        ability.processed = True
                        token.set_ability_cooldown(ability.key, ability.cooldown)
                        token.set_social_cooldown(62)

                        if ability.key in RACE_NAMES:
                            owner = self.tm.get_token_by_sender_id(job.sender_id)
                            if owner and owner.class_type == "apostle":
                                if observer_token and owner.id == observer_token.id:
                                    logger.debug(
                                        f"ℹ️ Observer получил баф {ability.key}, "
                                        f"но не добавляем временную расу"
                                    )
                                else:
                                    now_ts = time.time()
                                    expires_at = round(now_ts + 2 * 60 * 60)
                                    updated = owner.update_temp_race_expiry(
                                        ability.key,
                                        expires_at,
                                    )
                                    if not updated and not owner.has_race(ability.key):
                                        added = owner.add_temporary_race(
                                            ability.key,
                                            expires_at=expires_at,
                                        )
                                        if added:
                                            logger.info(
                                                f"🎯 {owner.name}: добавлена временная "
                                                f"раса '{ability.key}' "
                                                f"(владелец !баф id={job.sender_id})"
                                            )
                                        else:
                                            logger.warning(
                                                f"⚠️ {owner.name}: не удалось добавить "
                                                f"временную расы '{ability.key}'"
                                            )
                                    self.tm.update_race_index(owner)

                        if ability.uses_voices:
                            if token.voices > 0:
                                new_voices = token.voices - 1
                                logger.info(
                                    f"🗣️ {token.name}: списание голоса "
                                    f"{token.voices} → {new_voices}"
                                )
                                token.update_voices_from_system(new_voices)
                                token.mark_for_save()

                        logger.debug(
                            f"🔍 Анализ крита для бафа '{ability.text}':"
                        )
                        logger.debug(
                            f"📋 Текст ответа для анализа: {buff_response_text[:200]}..."
                        )

                        buff_value, is_critical = self._parse_buff_value(
                            buff_response_text
                        )

                        logger.info(
                            f"📊 Результат: {token.name}: {ability.text} "
                            f"(значение: {buff_value}, крит: {is_critical})"
                        )

                        token.successful_buffs += 1
                        token.total_attempts += 1
                        token.mark_for_save()

                        buff_info = {
                            "token_name": token.name,
                            "buff_value": buff_value,
                            "is_critical": is_critical,
                            "ability_key": ability.key,
                            "buff_name": ability.text,
                            "full_text": buff_response_text,
                        }
                        return True, "SUCCESS", buff_info

                    if status == "ALREADY":
                        token.set_social_cooldown(62)
                        token.successful_buffs += 1
                        token.total_attempts += 1
                        token.mark_for_save()
                        logger.info(
                            f"ℹ️ {token.name}: {ability.text} ALREADY"
                        )
                        return True, "ALREADY", None

                    if status == "NO_VOICES":
                        token.update_voices_from_system(0)
                        token.increment_buff_stats(False)
                        token.mark_for_save()
                        return False, "NO_VOICES", None

                    if status == "COOLDOWN":
                        if remaining is not None and remaining > 0:
                            rem_safe = int(remaining) + 1
                            token.set_ability_cooldown(ability.key, rem_safe)
                            token.set_social_cooldown(rem_safe)
                            token.increment_buff_stats(False)
                            logger.warning(
                                f"⚠️ {token.name}: COOLDOWN from VK => set {rem_safe}s"
                            )
                            return False, f"COOLDOWN({rem_safe}s)", None

                        token.set_ability_cooldown(ability.key, 62)
                        token.set_social_cooldown(62)
                        token.increment_buff_stats(False)
                        return False, "COOLDOWN(62s)", None

                token.increment_buff_stats(False)
                return False, "UNKNOWN", None

    def refresh_profile(self, token: TokenHandler) -> bool:
        if not token.enabled or token.is_captcha_paused() or token.needs_manual_voices:
            return False

        history_before = token.get_history_cached(token.target_peer_id, count=1)
        last_id_before = history_before[0]["id"] if history_before else 0

        ok, _ = token.send_to_peer(token.target_peer_id, "Мой профиль", None)
        if not ok:
            return False

        time.sleep(3.0)

        history = token.get_history_cached(token.target_peer_id, count=25)
        new_msgs = [m for m in history if int(m.get("id", 0)) > last_id_before]
        if not new_msgs:
            return False

        got_voices = False

        for m in reversed(new_msgs):
            text = str(m.get("text", "")).strip()
            logger.debug(f"📊 Парсим профиль: {text[:200]}")

            found_voices = None
            vm = RE_VOICES_GENERIC.search(text)
            if vm:
                try:
                    found_voices = int(vm.group(1))
                except Exception as e:
                    logger.error(
                        f"❌ {token.name}: ошибка парсинга голосов с RE_VOICES_GENERIC: {e}"
                    )

            if found_voices is None:
                vm = RE_VOICES_ANY.search(text)
                if vm:
                    try:
                        found_voices = int(vm.group(1))
                    except Exception as e:
                        logger.error(
                            f"❌ {token.name}: ошибка парсинга голосов с RE_VOICES_ANY: {e}"
                        )

            if found_voices is None:
                vm = RE_VOICES_IN_PARENTHESES.search(text)
                if vm:
                    try:
                        found_voices = int(vm.group(1))
                    except Exception as e:
                        logger.error(
                            f"❌ {token.name}: ошибка парсинга голосов в скобках: {e}"
                        )

            if found_voices is not None:
                token.update_voices_from_system(found_voices)
                token.mark_for_save()
                got_voices = True
                logger.info(
                    f"📊 {token.name}: обновлены голоса в профиле: {found_voices}"
                )

            level_match = RE_PROFILE_LEVEL.search(text)
            if level_match:
                try:
                    level = int(level_match.group(1))
                    token.update_level(level)
                    token.mark_for_save()
                    logger.info(
                        f"📊 {token.name}: обновлен уровень: {level}"
                    )
                except Exception as e:
                    logger.error(
                        f"❌ {token.name}: ошибка парсинга уровня: {e}"
                    )

        return got_voices
