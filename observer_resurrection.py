# -*- coding: utf-8 -*-
import logging
import time
import random
import re
from typing import Tuple, Optional  # ← ДОБАВЛЯЕМ НЕДОСТАЮЩИЙ ИМПОРТ

from .constants import RESURRECTION_CONFIG
from .commands import parse_resurrection_cmd
from .regexes import (
    RE_RESURRECTION_SUCCESS, 
    RE_RESURRECTION,
    RE_VOICES_GENERIC, 
    RE_VOICES_ANY, 
    RE_VOICES_IN_PARENTHESES
)

logger = logging.getLogger(__name__)


class ResurrectionHandler:
    def __init__(self, bot):
        self.bot = bot

    def find_trigger_in_token_source(self, token, from_id: int, trigger_text: str) -> Tuple[Optional[int], Optional[int]]:
        """
        Находит оригинальное сообщение пользователя в истории source_peer_id токена.
        Точно как в executor.py для /баф.
        """
        want_text = (trigger_text or "").strip().lower()
        if not want_text:
            return None, None

        # Получаем историю из source_peer_id токена
        msgs = token.get_history_cached(token.source_peer_id, count=30)
        for m in msgs:
            msg_from_id = int(m.get("from_id", 0))
            if msg_from_id != from_id:
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
                logger.info(f"🔍 Найдено сообщение в history токена {token.name}: id={mid}, cmid={cmid_int}")
                return mid, cmid_int

        logger.debug(f"⚠️ Сообщение '{want_text}' не найдено в history токена {token.name}")
        return None, None

    def handle(self, text: str, from_id: int):
        """Обработка команды /воскрешение"""
        # Парсим уровень цели
        lvl = parse_resurrection_cmd(text)
        if not lvl:
            self.bot.send_to_peer(
                self.bot.source_peer_id, 
                "❌ Неверный формат. Пример: /воскрешение 25"
            )
            return

        logger.info(f"♻️ Воскрешение: цель уровень {lvl}")
        
        # ============= Поиск подходящих паладинов =============
        candidates = []
        for t in self.bot.tm.tokens:
            # Только паладины
            if t.class_type not in ["crusader", "light_incarnation"]:
                continue
                
            # Проверка на включенность и капчу
            if not t.enabled or t.is_captcha_paused():
                logger.debug(f"⏭️ {t.name}: отключен или в капче")
                continue
                
            # Уровень должен быть СТРОГО ВЫШЕ
            if t.level <= lvl:
                logger.debug(f"⏭️ {t.name}: уровень {t.level} <= {lvl}")
                continue
                
            # Минимум голосов
            if t.voices < RESURRECTION_CONFIG["min_voices"]:
                logger.debug(f"⏭️ {t.name}: голосов {t.voices} < {RESURRECTION_CONFIG['min_voices']}")
                continue
                
            # Проверка кулдаунов
            can_social, rem_social = t.can_use_social()
            can_ability, rem_ability = t.can_use_ability("воскрешение")
            
            if not can_social:
                logger.debug(f"⏭️ {t.name}: социальное КД {rem_social:.0f}с")
                continue
                
            if not can_ability:
                logger.debug(f"⏭️ {t.name}: способность КД {rem_ability:.0f}с")
                continue
                
            candidates.append(t)
        # ======================================================

        if not candidates:
            self.bot.send_to_peer(
                self.bot.source_peer_id, 
                f"❌ Нет паладинов для уровня {lvl}"
            )
            return

        # Выбираем СЛУЧАЙНОГО паладина
        chosen = random.choice(candidates)
        logger.info(f"✅ Выбран {chosen.name} (lvl {chosen.level})")
        logger.info(f"📌 Паладин: source_peer_id={chosen.source_peer_id}, target_peer_id={chosen.target_peer_id}")

        # ============= НАХОДИМ ТРИГГЕР В ИСТОРИИ ТОКЕНА (как в /баф) =============
        trigger_mid, trigger_cmid = self.find_trigger_in_token_source(chosen, from_id, text)
        
        if not trigger_mid and not trigger_cmid:
            logger.warning(f"⚠️ Не найдено оригинальное сообщение для {from_id} в истории {chosen.name}")
            # Продолжаем без пересылки, но с предупреждением
        # ==========================================================================

        # ============= Отправка команды с пересылкой (как в /баф) =============
        ok, status = chosen.send_to_peer(
            chosen.target_peer_id, 
            RESURRECTION_CONFIG["command_text"], 
            forward_msg_id=trigger_mid  # Пересылаем найденное сообщение
        )
        
        if not ok:
            self.bot.send_to_peer(
                self.bot.source_peer_id, 
                f"❌ Ошибка отправки: {status}"
            )
            return
        # ============================================

        # ============= ОЖИДАНИЕ И ПАРСИНГ ОТВЕТА =============
        logger.info(f"⏳ Ожидание подтверждения воскрешения для {from_id}...")
        
        poll_interval = float(self.bot.tm.settings.get("poll_interval", 2.0))
        poll_count = int(self.bot.tm.settings.get("poll_count", 20))
        
        found = False
        response_text = ""
        voices_val = None
        is_critical = False
        
        # Запоминаем последний ID сообщения перед отправкой
        before = chosen.get_history_cached(chosen.target_peer_id, count=1)
        last_id_before = before[0]["id"] if before else 0
        
        for i in range(poll_count):
            time.sleep(poll_interval * (1 + i * 0.2))
            
            # Получаем новые сообщения из чата игры
            chosen.invalidate_cache(chosen.target_peer_id)
            history = chosen.get_history_cached(chosen.target_peer_id, count=25)
            new_msgs = [m for m in history if int(m.get("id", 0)) > last_id_before]
            
            if not new_msgs:
                logger.debug(f"⏳ Попытка {i+1}/{poll_count}: новых сообщений нет")
                continue
            
            logger.info(f"📥 Попытка {i+1}/{poll_count}: получено {len(new_msgs)} новых сообщений")
            
            # Парсим новые сообщения
            for msg in reversed(new_msgs):
                msg_text = msg.get("text", "")
                msg_id = msg.get("id", 0)
                msg_from = msg.get("from_id", 0)
                
                logger.debug(f"📄 Проверка сообщения ID={msg_id} от {msg_from}: {msg_text[:100]}...")
                
                # Пропускаем свои сообщения
                if msg_from == chosen.owner_vk_id:
                    continue
                
                # Парсим голоса
                if voices_val is None:
                    vm = RE_VOICES_GENERIC.search(msg_text)
                    if vm:
                        try:
                            voices_val = int(vm.group(1))
                            logger.info(f"🗣️ Найдены голоса: {voices_val}")
                        except:
                            pass
                
                if voices_val is None:
                    vm = RE_VOICES_ANY.search(msg_text)
                    if vm:
                        try:
                            voices_val = int(vm.group(1))
                            logger.info(f"🗣️ Найдены голоса: {voices_val}")
                        except:
                            pass
                
                if voices_val is None:
                    vm = RE_VOICES_IN_PARENTHESES.search(msg_text)
                    if vm:
                        try:
                            voices_val = int(vm.group(1))
                            logger.info(f"🗣️ Найдены голоса: {voices_val}")
                        except:
                            pass
                
                # Проверяем на воскрешение
                if RE_RESURRECTION_SUCCESS.search(msg_text) or RE_RESURRECTION.search(msg_text):
                    # Проверяем, что сообщение адресовано нашему пользователю
                    if str(from_id) in msg_text or f"id{from_id}" in msg_text:
                        found = True
                        response_text = msg_text
                        logger.info(f"✅ Найдено подтверждение воскрешения для {from_id}")
                        
                        # Проверяем критичность
                        if "критический" in msg_text.lower() or "🍀" in msg_text:
                            is_critical = True
                            logger.info(f"🍀 Критическое воскрешение!")
                        
                        break
            
            if found:
                break
        # ====================================================================

        if not found:
            logger.warning(f"❌ Подтверждение воскрешения для {from_id} не найдено")
            self.bot.send_to_peer(
                self.bot.source_peer_id, 
                "❌ Нет подтверждения"
            )
            return

        # ============= ТОЛЬКО ПОСЛЕ УСПЕШНОГО ПОДТВЕРЖДЕНИЯ =============
        
        # 1. Списание голосов
        old_voices = chosen.voices
        for _ in range(RESURRECTION_CONFIG["cost_voices"]):
            chosen.spend_voice()
            
        logger.info(
            f"🗣️ {chosen.name}: списано {RESURRECTION_CONFIG['cost_voices']} голосов "
            f"({old_voices}→{chosen.voices})"
        )

        # 2. Установка кулдаунов (ТОЛЬКО после успеха)
        chosen.set_social_cooldown(RESURRECTION_CONFIG["social_cooldown"])
        chosen.set_ability_cooldown("воскрешение", RESURRECTION_CONFIG["cooldown"])
        
        logger.info(
            f"⏳ Установлены КД для {chosen.name}: "
            f"социальное {RESURRECTION_CONFIG['social_cooldown']}с, "
            f"способность {RESURRECTION_CONFIG['cooldown']}с"
        )

        # 3. Получение владельца токена для ссылки
        owner = chosen.owner_vk_id or chosen.fetch_owner_id_lazy()
        
        # 4. Формирование уведомления
        crit_emoji = " 🍀" if is_critical else ""
        notif = (
            f"🎉 Воскрешение успешно!{crit_emoji}\n"
            f"[https://vk.ru/id{owner}|♻️]Воскрешение\n"
            f"[https://vk.ru/id{from_id}|💰]Списано {RESURRECTION_CONFIG['cost_balance']} баллов"
        )
        
        # 5. Отправка уведомления
        self.bot.send_to_peer(self.bot.source_peer_id, notif)
        logger.info(f"✅ Воскрешение для {from_id} успешно завершено")
        # ====================================================================
