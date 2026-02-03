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
    RE_PROFILE_VOICES,
    RE_PROFILE_LEVEL,
    RE_NOT_APOSTLE_OF_RACE,      # ✅ НОВОЕ
    RE_ALREADY_BUFF,             # ✅ НОВОЕ
    RE_OTHER_RACE,               # ✅ НОВОЕ
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
                cmid_int = int(cmid) if isinstance(cmid, int) or (isinstance(cmid, str) and str(cmid).isdigit()) else None
                return mid, cmid_int

        return None, None

    def _parse_new_messages(self, msgs: List[Dict[str, Any]]) -> Tuple[str, Optional[int], Optional[int], str]:
        """Возвращает (статус, remaining, voices_val, full_response_text)"""
        remaining = None
        voices_val = None
        cooldown_hint = False
        full_response_text = ""  # ✅ Сохраняем полный текст ответа

        logger.debug(f"🔍 Начало парсинга {len(msgs)} сообщений")

        # вытаскиваем remaining и голоса из всех новых сообщений
        for m in msgs:
            text = str(m.get("text", "")).strip()
            text_l = text.lower()

            logger.debug(f"📝 Сообщение для парсинга: {text[:100]}...")

            # ✅ Сохраняем полный текст если это ответ от ВК с деталями бафа
            if "✨" in text or "повышена" in text or "увеличена" in text or "удача" in text:
                full_response_text = text
                logger.debug(f"📋 Сохранен полный текст ответа: {text[:200]}...")

            mm = RE_REMAINING_SEC.search(text)
            if mm:
                try:
                    remaining = int(mm.group(1))
                    if "социальные эффекты" in text_l:
                        cooldown_hint = True
                    logger.debug(f"⏰ Нашли remaining: {remaining}")
                except Exception as e:
                    logger.error(f"❌ Ошибка парсинга remaining: {e}")

            # Пробуем универсальную регулярку сначала (она должна сработать)
            if voices_val is None:
                vm = RE_VOICES_GENERIC.search(text)
                if vm:
                    try:
                        voices_val = int(vm.group(1))
                        logger.info(f"✅ Нашли голоса ({voices_val}) с RE_VOICES_GENERIC")
                    except Exception as e:
                        logger.error(f"❌ Ошибка парсинга голосов с RE_VOICES_GENERIC: {e}")

            # Если не нашли, пробуем RE_VOICES_ANY
            if voices_val is None:
                vm = RE_VOICES_ANY.search(text)
                if vm:
                    try:
                        voices_val = int(vm.group(1))
                        logger.info(f"✅ Нашли голоса ({voices_val}) с RE_VOICES_ANY")
                    except Exception as e:
                        logger.error(f"❌ Ошибка парсинга голосов с RE_VOICES_ANY: {e}")

            # Если все еще не нашли, пробуем скобочный формат
            if voices_val is None:
                vm = RE_VOICES_IN_PARENTHESES.search(text)
                if vm:
                    try:
                        voices_val = int(vm.group(1))
                        logger.info(f"✅ Нашли голоса ({voices_val}) в скобках")
                    except Exception as e:
                        logger.error(f"❌ Ошибка парсинга голосов в скобках: {e}")

        # ✅ ИСПРАВЛЕНО: приоритет ошибок над успехом
        for m in msgs:
            text = str(m.get("text", "")).strip()
            logger.debug(f"🔍 Проверяем статус в сообщении: {text[:100]}...")

            # 1. Ошибки (🚫) имеют высший приоритет
            if RE_NOT_APOSTLE_OF_RACE.search(text):
                matched = RE_NOT_APOSTLE_OF_RACE.search(text).group(0)
                logger.info(f"🔍 Статус: NOT_APOSTLE_OF_RACE - '{RE_NOT_APOSTLE_OF_RACE.pattern}' сработало на '{matched}'")
                return "NOT_APOSTLE_OF_RACE", remaining, voices_val, full_response_text

            if RE_ALREADY_BUFF.search(text):
                matched = RE_ALREADY_BUFF.search(text).group(0)
                logger.info(f"🔍 Статус: ALREADY_BUFF - '{RE_ALREADY_BUFF.pattern}' сработало на '{matched}'")
                return "ALREADY_BUFF", remaining, voices_val, full_response_text

            if RE_OTHER_RACE.search(text):
                matched = RE_OTHER_RACE.search(text).group(0)
                logger.info(f"🔍 Статус: OTHER_RACE - '{RE_OTHER_RACE.pattern}' сработало на '{matched}'")
                return "OTHER_RACE", remaining, voices_val, full_response_text

            if RE_NOT_APOSTLE.search(text):
                matched = RE_NOT_APOSTLE.search(text).group(0)
                logger.info(f"🔍 Статус: NOT_APOSTLE - '{RE_NOT_APOSTLE.pattern}' сработало на '{matched}'")
                return "NOT_APOSTLE", remaining, voices_val, full_response_text

            # 2. Успех (✨) только если нет 🚫
            if "✨" in text and RE_SUCCESS.search(text):
                matched = RE_SUCCESS.search(text).group(0)
                logger.info(f"🔍 Статус: SUCCESS - '{RE_SUCCESS.pattern}' сработало на '{matched}'")
                return "SUCCESS", remaining, voices_val, full_response_text

            if RE_ALREADY.search(text):
                matched = RE_ALREADY.search(text).group(0)
                logger.info(f"🔍 Статус: ALREADY - '{RE_ALREADY.pattern}' сработало на '{matched}'")
                return "ALREADY", remaining, voices_val, full_response_text

            if RE_NO_VOICES.search(text):
                matched = RE_NO_VOICES.search(text).group(0)
                logger.info(f"🔍 Статус: NO_VOICES - '{RE_NO_VOICES.pattern}' сработало на '{matched}'")
                return "NO_VOICES", remaining, voices_val, full_response_text

            if RE_COOLDOWN.search(text):
                matched = RE_COOLDOWN.search(text).group(0)
                if len(matched) > 50:
                    matched = matched[:50] + "..."
                logger.info(f"🔍 Статус: COOLDOWN - '{RE_COOLDOWN.pattern}' сработало на '{matched}'")
                return "COOLDOWN", remaining, voices_val, full_response_text

        # ✅ fallback: если regex COOLDOWN не сработал, но remaining найден и похоже на соц-кд
        if remaining is not None and cooldown_hint:
            logger.info(f"🔍 Статус: COOLDOWN (fallback, remaining={remaining})")
            return "COOLDOWN", remaining, voices_val, full_response_text

        logger.info("🔍 Статус: UNKNOWN (ни одна регулярка не сработала)")
        return "UNKNOWN", remaining, voices_val, full_response_text

    def _parse_buff_value(self, text: str) -> Tuple[int, bool]:
        """Определяет значение бафа и был ли крит"""
        if not text:
            logger.debug("📭 Текст для анализа крита пустой")
            return 100, False

        text_lower = text.lower()

        # ✅ ПРОВЕРКА ПРОЦЕНТОВ В ТЕКСТЕ
        is_critical = False
        buff_value = 100  # по умолчанию

        # Поиск процентов в тексте - БОЛЕЕ АГРЕССИВНЫЙ ПОИСК
        percent_patterns = [
            r'(\+?\d{1,3})\s*%',  # 30%, +30%
            r'на\s+(\d{1,3})\s*%',  # на 30%
            r'повышена\s+на\s+(\d{1,3})\s*%',  # повышена на 30%
            r'увеличена\s+на\s+(\d{1,3})\s*%',  # увеличена на 30%
            r'повышена\s+(\d{1,3})\s*%',  # повышена 30%
            r'увеличена\s+(\d{1,3})\s*%',  # увеличена 30%
            r'Броня повышена на (\d{1,3})%',  # Броня повышена на 20%
            r'Атака повышена на (\d{1,3})%',  # Атака повышена на 30%
            r'Удача повышена на (\d{1,3})',  # Удача повышена на 9
        ]

        found_percent = None
        for pattern in percent_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    found_percent = int(match.group(1))
                    logger.info(f"🔍 Найден процент в тексте: {found_percent}% (паттерн: {pattern})")
                    break
                except Exception as e:
                    logger.debug(f"❌ Ошибка парсинга процента: {e}")
                    continue

        # Для рас всегда 100
        race_keywords = ["человек", "гоблин", "нежить", "эльф", "гном", "демон", "орк", "людей", "гоблинов", "нежити", "эльфов", "гномов", "демонов", "орков"]
        if any(race in text_lower for race in race_keywords):
            logger.debug(f"📊 Расовый баф: {text[:50]}...")
            return 100, False

        # Если нашли процент
        if found_percent is not None:
            if found_percent == 30:
                # 30% = крит = 150 голосов
                is_critical = True
                buff_value = 150
                logger.info(f"🎯 Определен крит баф: {found_percent}% = {buff_value} голосов")
            elif found_percent == 20:
                # 20% = обычный = 100 голосов
                is_critical = False
                buff_value = 100
                logger.info(f"📊 Обычный баф: {found_percent}% = {buff_value} голосов")
            elif found_percent == 6 or found_percent == 9:
                # Удача = 100 голосов (6 или 9 единиц)
                is_critical = False
                buff_value = 100
                logger.info(f"🍀 Баф удачи: {found_percent} единиц = {buff_value} голосов")
            else:
                # Другой процент
                buff_value = 100
                is_critical = "критический" in text_lower or "🍀" in text
                logger.info(f"📈 Баф {found_percent}%: значение={buff_value}, крит={is_critical}")
        else:
            # Не нашли процент, пытаемся определить по ключевым словам
            logger.debug(f"📝 Процент не найден в тексте: {text[:100]}...")

        # Дополнительная проверка на крит по тексту
        if not is_critical and ("критический баф" in text_lower or "🍀" in text):
            is_critical = True
            buff_value = 150
            logger.info(f"🍀 Определен крит баф по тексту: 'критический' или '🍀'")

        # Если не нашли процент, но есть ключевые слова атаки/защиты
        if found_percent is None and any(x in text_lower for x in ["атаки", "защиты"]):
            # По умолчанию для атаки/защиты
            buff_value = 150 if is_critical else 100
            logger.debug(f"⚔️ Баф атаки/защиты без процента: значение={buff_value}, крит={is_critical}")

        logger.info(f"📊 Итог парсинга бафа: значение={buff_value}, крит={is_critical}, текст='{text[:80]}...'")
        return buff_value, is_critical

    def execute_one(self, token: TokenHandler, ability: ParsedAbility, job: Job) -> Tuple[bool, str, Optional[Dict]]:
        """Выполнить баф, возвращает (успех, статус, информация_о_бафе)"""
        # ✅ КРИТИЧНО: запрет параллельного исполнения ОДНИМ токеном
        with token._lock:
            # ✅ ДОБАВИТЬ: Observer не должен бафать (более строгая проверка)
            observer_token = self.tm.get_observer()
            if observer_token and (token.id == observer_token.id or token.name == "Observer"):
                logger.warning(f"⛔ {token.name} является Observer и не должен участвовать в бафах")
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
            if ability.uses_voices and token.voices <= 0:
                token.increment_buff_stats(False)
                return False, "NO_VOICES_LOCAL", None

            # ✅ Глобальный КД соц. эффектов
            can_social, rem_social = token.can_use_social()
            if not can_social:
                token.increment_buff_stats(False)
                return False, f"SOCIAL_COOLDOWN({int(rem_social)}s)", None

            # Локальный КД по способности
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

                buff_response_text = ""  # Сохраним текст ответа для анализа крита

                for i in range(poll_count):
                    time.sleep(poll_interval * (1 + i * 0.2))

                    history = token.get_history_cached(token.target_peer_id, count=25)
                    new_msgs = [m for m in history if int(m.get("id", 0)) > last_id_before]
                    if not new_msgs:
                        continue

                    # ✅ ИСПРАВЛЕНО: получаем полный текст ответа
                    status, remaining, voices_val, full_response_text = self._parse_new_messages(list(reversed(new_msgs)))

                    # ✅ Используем полный текст ответа для анализа крита
                    if full_response_text and not buff_response_text:
                        buff_response_text = full_response_text
                        logger.debug(f"📋 Получен полный текст ответа: {full_response_text[:200]}...")

                    if voices_val is not None:
                        logger.info(f"🗣️ {token.name}: обновление голосов {token.voices} → {voices_val}")
                        token.update_voices_from_system(voices_val)

                    # ✅ ИСПРАВЛЕНО: Обработка ошибок - не начисляем, не списываем
                    if status == "NOT_APOSTLE_OF_RACE":
                        # если токен не апостол расы — удаляем временную расу если она была
                        if ability.key in RACE_NAMES:
                            before_cnt = len(token.temp_races)
                            token.temp_races = [tr for tr in token.temp_races if tr["race"] != ability.key]
                            if len(token.temp_races) != before_cnt:
                                self.tm.mark_for_save()
                                self.tm.update_race_index(token)
                                logging.warning(f"🗑️ {token.name}: удалена временная раса '{ability.key}' (NOT_APOSTLE_OF_RACE)")

                        token.set_ability_cooldown(ability.key, 300)
                        token.set_social_cooldown(300)
                        # ✅ НЕ инкрементируем статистику при ошибке
                        return False, "NOT_APOSTLE_OF_RACE", None

                    if status == "ALREADY_BUFF":
                        # Уже есть такой баф - не списываем голоса
                        token.set_social_cooldown(62)
                        # ✅ НЕ инкрементируем статистику при ошибке
                        return False, "ALREADY_BUFF", None

                    if status == "OTHER_RACE":
                        # Уже есть другая расовая - не списываем голоса
                        token.set_social_cooldown(62)
                        # ✅ НЕ инкрементируем статистику при ошибке
                        return False, "OTHER_RACE", None

                    if status == "NOT_APOSTLE":
                        # если токен не апостол — чистим его временную расу (если она была ошибочно)
                        if ability.key in RACE_NAMES:
                            before_cnt = len(token.temp_races)
                            token.temp_races = [tr for tr in token.temp_races if tr["race"] != ability.key]
                            if len(token.temp_races) != before_cnt:
                                self.tm.mark_for_save()
                                self.tm.update_race_index(token)
                                logging.warning(f"🗑️ {token.name}: удалена временная раса '{ability.key}' (NOT_APOSTLE)")

                        token.set_ability_cooldown(ability.key, 300)
                        token.set_social_cooldown(300)
                        token.increment_buff_stats(False)
                        return False, "NOT_APOSTLE", None

                    if status == "SUCCESS":
                        ability.processed = True
                        token.set_ability_cooldown(ability.key, ability.cooldown)

                        # ✅ соц-КД после успеха
                        token.set_social_cooldown(62)

                        # ✅ ПЕРЕНЕСЕНО: добавление временной расы ПОСЛЕ успешного бафа
                        if ability.key in RACE_NAMES:
                            owner = self.tm.get_token_by_sender_id(job.sender_id)
                            # ✅ ПРОВЕРКА: owner должен быть апостолом и НЕ быть Observer'ом
                            if owner and owner.class_type == "apostle":
                                # Проверяем что это не Observer
                                if observer_token and owner.id == observer_token.id:
                                    logger.debug(f"ℹ️ Observer получил баф {ability.key}, но не добавляем временную расу (Observer не апостол)")
                                else:
                                    now = time.time()
                                    expires_at = round(now + 2 * 60 * 60)

                                    # 1) если такая временная раса уже есть — просто продлим
                                    updated = owner.update_temp_race_expiry(ability.key, expires_at)

                                    # 2) если не было — попробуем добавить (только если можно)
                                    if not updated and not owner.has_race(ability.key):
                                        added = owner.add_temporary_race(ability.key, expires_at=expires_at)
                                        if added:
                                            logging.info(
                                                f"🎯 {owner.name}: добавлена временная раса '{ability.key}' "
                                                f"(владелец !баф id={job.sender_id})"
                                            )
                                        else:
                                            logging.warning(
                                                f"⚠️ {owner.name}: не удалось добавить временную расу '{ability.key}' "
                                                f"(возможно уже есть другая временная)"
                                            )

                                    # индекс рас обновляем для владельца
                                    self.tm.update_race_index(owner)

                        if ability.uses_voices:
                            # Обновляем голоса только если их изменила игра
                            # Игра уже сама списывает голос, нам нужно только обновить счетчик
                            if token.voices > 0:
                                new_voices = token.voices - 1
                                logger.info(f"🗣️ {token.name}: списание голоса {token.voices} → {new_voices}")
                                token.update_voices_from_system(new_voices)

                        # ✅ Определяем значение бафа и крит с подробным логом
                        logger.debug(f"🔍 Анализ крита для бафа '{ability.text}':")
                        logger.debug(f"📋 Текст ответа для анализа: {buff_response_text[:200]}...")
                        buff_value, is_critical = self._parse_buff_value(buff_response_text)
                        logger.info(f"📊 Результат: {token.name}: {ability.text} (значение: {buff_value}, крит: {is_critical})")

                        # ✅ Обновляем статистику
                        token.successful_buffs += 1
                        token.total_attempts += 1

                        # ✅ ОДИН РАЗ сохраняем все изменения
                        try:
                            self.tm.save(force=True)
                            logger.info(f"💾 Конфигурация сохранена (после успешного бафа)")
                        except Exception as e:
                            logger.error(f"❌ Ошибка сохранения конфигурации: {e}")

                        # ✅ Информация о бафе для уведомления
                        buff_info = {
                            "token_name": token.name,
                            "buff_value": buff_value,
                            "is_critical": is_critical,
                            "ability_key": ability.key,
                            "buff_name": ability.text
                        }

                        return True, "SUCCESS", buff_info

                    if status == "ALREADY":
                        token.set_social_cooldown(62)
                        token.successful_buffs += 1
                        token.total_attempts += 1
                        # ✅ Сохраняем статистику
                        try:
                            self.tm.save(force=True)
                            logger.info(f"💾 Конфигурация сохранена (ALREADY)")
                        except Exception as e:
                            logger.error(f"❌ Ошибка сохранения конфигурации: {e}")
                        logging.info(f"ℹ️ {token.name}: {ability.text} ALREADY")
                        return True, "ALREADY", None

                    if status == "NO_VOICES":
                        token.update_voices_from_system(0)
                        token.increment_buff_stats(False)
                        try:
                            self.tm.save(force=True)
                        except Exception as e:
                            logger.error(f"❌ Ошибка сохранения конфигурации: {e}")
                        return False, "NO_VOICES", None

                    if status == "COOLDOWN":
                        # ✅ если VK сказал "осталось N сек" → ставим N+1 (и локальный, и соц)
                        if remaining is not None and remaining > 0:
                            rem_safe = int(remaining) + 1
                            token.set_ability_cooldown(ability.key, rem_safe)
                            token.set_social_cooldown(rem_safe)
                            token.increment_buff_stats(False)
                            logging.warning(f"⚠️ {token.name}: COOLDOWN from VK => set {rem_safe}s")
                            return False, f"COOLDOWN({rem_safe}s)", None

                        # fallback
                        token.set_ability_cooldown(ability.key, 62)
                        token.set_social_cooldown(62)
                        token.increment_buff_stats(False)
                        return False, "COOLDOWN(62s)", None

                token.increment_buff_stats(False)
                return False, "UNKNOWN", None

    def refresh_profile(self, token: TokenHandler) -> bool:
        """Обновление профиля (голоса, уровень) через команду 'Мой профиль'"""
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

            # Пробуем найти голоса в профиле
            found_voices = None

            # Сначала универсальная регулярка
            vm = RE_VOICES_GENERIC.search(text)
            if vm:
                try:
                    found_voices = int(vm.group(1))
                except Exception as e:
                    logger.error(f"❌ {token.name}: ошибка парсинга голосов с RE_VOICES_GENERIC: {e}")

            # Если не нашли, пробуем RE_VOICES_ANY
            if found_voices is None:
                vm = RE_VOICES_ANY.search(text)
                if vm:
                    try:
                        found_voices = int(vm.group(1))
                    except Exception as e:
                        logger.error(f"❌ {token.name}: ошибка парсинга голосов с RE_VOICES_ANY: {e}")

            # Если не нашли, пробуем скобочный формат
            if found_voices is None:
                vm = RE_VOICES_IN_PARENTHESES.search(text)
                if vm:
                    try:
                        found_voices = int(vm.group(1))
                    except Exception as e:
                        logger.error(f"❌ {token.name}: ошибка парсинга голосов в скобках: {e}")

            # Если нашли голоса - обновляем
            if found_voices is not None:
                token.update_voices_from_system(found_voices)
                got_voices = True
                logger.info(f"📊 {token.name}: обновлены голоса в профиле: {found_voices}")

            # Пробуем найти уровень
            level_match = RE_PROFILE_LEVEL.search(text)
            if level_match:
                try:
                    level = int(level_match.group(1))
                    token.update_level(level)
                    logger.info(f"📊 {token.name}: обновлен уровень: {level}")
                except Exception as e:
                    logger.error(f"❌ {token.name}: ошибка парсинга уровня: {e}")

        # ✅ Сохраняем только если что-то изменилось
        if got_voices:
            try:
                self.tm.save(force=True)
            except Exception as e:
                logger.error(f"❌ Ошибка сохранения конфигурации: {e}")

        return got_voices
