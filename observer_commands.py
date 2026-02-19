# -*- coding: utf-8 -*-
import logging
import time
import re
import threading  # <-- ИМПОРТ ПЕРЕМЕЩЁН СЮДА
from typing import Optional, Dict, Any

from .utils import normalize_text
from .commands import (
    parse_baf_letters, parse_golosa_cmd, parse_doprasa_cmd,
    is_apo_cmd, is_baf_cancel_cmd, is_prof_cmd
)
from .notifications import build_registration_text
from .models import Job
from .constants import RACE_NAMES
from .regexes import RE_PROFILE_LEVEL, RE_VOICES_GENERIC, RE_VOICES_ANY

logger = logging.getLogger(__name__)


class CommandHandler:
    def __init__(self, bot):
        self.bot = bot
        # Регулярка для голосов в скобках у класса
        self.RE_VOICES_FROM_CLASS_PARENS = re.compile(
            r"👤\s*Класс:\s*[^\(\n]*\((\d+)\)", re.IGNORECASE
        )
        # ID Observer-а
        self.OBSERVER_ID = 92900278

    def handle(self, text: str, from_id: int, msg: dict) -> bool:
        norm = normalize_text(text)
        logger.debug(f"handle: norm='{norm}', from_id={from_id}, original='{text}'")

        # Отмена бафов
        if is_baf_cancel_cmd(norm):
            logger.info("Обнаружена команда отмены бафов")
            return self._cancel(from_id)

        # Команда /проф для проверки профиля
        if is_prof_cmd(norm):
            logger.info(f"✅ Обнаружена команда /проф: '{text}'")
            return self._profile_check(text, from_id)

        if norm in ["/здоровье", "/health", "/статус"]:
            self._health(from_id)
            return True

        if norm.startswith("/диагностика"):
            self._diag(text, from_id)
            return True

        if norm.startswith("/апо "):
            self._apo_toggle(text, from_id)
            return True

        if norm.startswith("/сменарасы"):
            self._change_races(text, from_id)
            return True

        pg = parse_golosa_cmd(text)
        if pg:
            self._voices(from_id, pg[1])
            return True

        if norm.startswith("/допраса"):
            self._doprasa(text, from_id, msg)
            return True

        if is_apo_cmd(norm):
            self._apo_status(from_id)
            return True

        letters = parse_baf_letters(text)
        if letters:
            self._baf(
                letters, from_id, text,
                msg.get("conversation_message_id"),
                msg.get("id")
            )
            return True

        return False

    def _cancel(self, from_id: int) -> bool:
        """Обработка команды отмены бафов"""
        logger.debug(f"_cancel: from_id={from_id}")
        had_job, pending_letters, completed_count = self.bot.state.cancel_and_clear(from_id)
        
        if not had_job:
            if completed_count > 0:
                self.bot.send_to_peer(
                    self.bot.source_peer_id,
                    f"ℹ️ Ваши бафы уже выполнены ({completed_count} шт.). Нечего отменять."
                )
            else:
                self.bot.send_to_peer(
                    self.bot.source_peer_id,
                    "❌ У вас нет активных бафов для отмены."
                )
            return True
        
        cancelled = self.bot.scheduler.cancel_user_jobs(from_id)
        
        if cancelled and pending_letters:
            if completed_count == 0:
                msg = f"✅ Все ваши бафы ({pending_letters}) отменены."
            else:
                msg = (
                    f"✅ Бафы частично отменены.\n"
                    f"• Отменено: {pending_letters}\n"
                    f"• Уже выполнено: {completed_count} шт.\n"
                    f"• Итоговое уведомление по выполненным будет отправлено отдельно."
                )
        elif not cancelled and pending_letters:
            msg = f"⚠️ Не удалось найти бафы '{pending_letters}' в очереди выполнения."
        else:
            msg = "⚠️ Не удалось выполнить отмену. Попробуйте позже."
        
        self.bot.send_to_peer(self.bot.source_peer_id, msg)
        return True

    # ============= КОМАНДА /ПРОФ =============
    def _profile_check(self, text: str, from_id: int) -> bool:
        """
        Ручная проверка профиля токена по команде /проф
        
        Примеры использования:
        /проф                    - проверить свой токен
        /проф ИмяТокена          - проверить любой токен (для Observer-а)
        """
        logger.info(f"📋 _profile_check: получена команда /проф от пользователя {from_id}, текст: '{text}'")
        
        # Разбираем аргументы
        parts = text.strip().split()
        token_name = None
        if len(parts) > 1:
            token_name = " ".join(parts[1:]).strip()
            logger.info(f"📋 Запрошен токен по имени: '{token_name}'")
        
        # Определяем токен для проверки
        token = None
        
        # Проверяем, является ли отправитель Observer-ом
        is_observer = (from_id == self.OBSERVER_ID)
        
        if token_name:
            # Если указано имя токена - ищем по имени (для всех)
            logger.info(f"🔍 Поиск токена по имени: '{token_name}'")
            token = self.bot.tm.get_token_by_name(token_name)
            if not token:
                logger.warning(f"❌ Токен '{token_name}' не найден")
                self.bot.send_to_peer(
                    self.bot.source_peer_id,
                    f"❌ Токен '{token_name}' не найден."
                )
                return True
            logger.info(f"✅ Найден токен: {token.name} (ID: {token.id})")
        else:
            # Если имя не указано
            if is_observer:
                # Observer без имени токена - ошибка, нужно указать имя
                logger.warning("⛔ Observer не указал имя токена")
                self.bot.send_to_peer(
                    self.bot.source_peer_id,
                    "❌ Для Observer-а нужно указать имя токена: /проф ИмяТокена"
                )
                return True
            else:
                # Обычный пользователь - ищем по ID
                logger.info(f"🔍 Поиск токена по ID отправителя: {from_id}")
                token = self.bot.tm.get_token_by_sender_id(from_id)
                if not token:
                    logger.warning(f"❌ Токен для пользователя {from_id} не найден")
                    self.bot.send_to_peer(
                        self.bot.source_peer_id,
                        "❌ Токен пользователя не найден."
                    )
                    return True
                logger.info(f"✅ Найден токен: {token.name} (ID: {token.id})")
        
        # Запускаем проверку в отдельном потоке
        logger.info(f"🚀 Запуск потока проверки профиля для {token.name}")
        thread = threading.Thread(
            target=self._run_profile_check,
            args=(token, from_id),
            daemon=True
        )
        thread.start()
        logger.info(f"✅ Поток проверки запущен для {token.name}")
        
        return True

    def _run_profile_check(self, token, from_id):
        """
        Выполняет проверку профиля и отправляет результат
        """
        thread_id = threading.get_ident()
        logger.info(f"🔍 [Поток {thread_id}] Запуск проверки профиля для {token.name}")
        
        try:
            # Запоминаем старые значения
            old_voices = token.voices
            old_level = token.level
            old_races = token.races.copy() if token.races else []
            logger.info(f"📊 [Поток {thread_id}] Текущее состояние {token.name}: голоса={old_voices}, уровень={old_level}, расы={old_races}")
            
            # 1. Отправляем команду в чат игры
            logger.info(f"📤 [Поток {thread_id}] Отправка 'Мой профиль' в чат {token.target_peer_id}")
            ok, status = token.send_to_peer(token.target_peer_id, "Мой профиль", None)
            if not ok:
                logger.error(f"❌ [Поток {thread_id}] Ошибка отправки запроса: {status}")
                self.bot.send_to_peer(
                    self.bot.source_peer_id,
                    "❌ Ошибка отправки запроса."
                )
                return
            logger.info(f"✅ [Поток {thread_id}] Запрос отправлен, статус: {status}")

            # 2. Ждём ответ
            logger.info(f"⏳ [Поток {thread_id}] Ожидание 3 секунды...")
            time.sleep(3.0)

            # 3. Получаем историю
            logger.info(f"📥 [Поток {thread_id}] Инвалидация кэша и получение истории")
            token.invalidate_cache(token.target_peer_id)
            history = token.get_history_cached(token.target_peer_id, count=25)
            if not history:
                logger.error(f"❌ [Поток {thread_id}] История пуста")
                self.bot.send_to_peer(
                    self.bot.source_peer_id,
                    "❌ Не найден ответ на запрос профиля."
                )
                return
            logger.info(f"✅ [Поток {thread_id}] Получено {len(history)} сообщений")

            # 4. Ищем ответ и парсим
            found_any = False
            changes = []
            
            logger.info(f"🔍 [Поток {thread_id}] Поиск ответа в последних 5 сообщениях")
            for idx, msg in enumerate(history[:5]):
                msg_text = str(msg.get("text", "") or "").strip()
                msg_id = msg.get("id", 0)
                logger.debug(f"📄 [Поток {thread_id}] Сообщение {idx+1}: ID={msg_id}, текст='{msg_text[:100]}...'")

                if not msg_text:
                    continue

                if "мой профиль" in msg_text.lower():
                    logger.debug(f"⏭️ [Поток {thread_id}] Пропускаем своё сообщение")
                    continue

                profile_data = self._parse_profile_response(msg_text)
                logger.debug(f"📊 [Поток {thread_id}] Распарсенные данные: {profile_data}")
                
                if profile_data["level"] is None and profile_data["voices"] is None and not profile_data["races"]:
                    logger.debug(f"⏭️ [Поток {thread_id}] Не похоже на ответ профиля")
                    continue

                found_any = True
                logger.info(f"✅ [Поток {thread_id}] Найден ответ профиля в сообщении {idx+1}")

                # Обновляем голоса
                if profile_data["voices"] is not None and token.voices != profile_data["voices"]:
                    old = token.voices
                    token.update_voices_from_system(profile_data["voices"])
                    changes.append(f"голоса: {old}→{token.voices}")
                    logger.info(f"🗣 [Поток {thread_id}] Обновлены голоса: {old}→{token.voices}")

                # Обновляем уровень
                if profile_data["level"] is not None and token.level != profile_data["level"]:
                    old = token.level
                    token.update_level(profile_data["level"])
                    changes.append(f"уровень: {old}→{token.level}")
                    logger.info(f"📊 [Поток {thread_id}] Обновлён уровень: {old}→{token.level}")

                # Обновляем расы для апостолов
                if token.class_type == "apostle" and profile_data["races"]:
                    if set(profile_data["races"]) != set(token.races):
                        old = token.races.copy()
                        token.races = profile_data["races"]
                        token.mark_for_save()
                        self.bot.tm.update_race_index(token)
                        changes.append(f"расы: {old}→{token.races}")
                        logger.info(f"🎭 [Поток {thread_id}] Обновлены расы: {old}→{token.races}")

                break

            # 5. Отправляем результат
            if not found_any:
                logger.warning(f"⚠️ [Поток {thread_id}] Ответ профиля не найден")
                self.bot.send_to_peer(
                    self.bot.source_peer_id,
                    "❌ Не найден ответ на запрос профиля."
                )
                return

            if changes:
                result_msg = f"✅ Профиль обновлен:\n"
                for change in changes:
                    result_msg += f"   • {change}\n"
                result_msg = result_msg.rstrip()
                logger.info(f"✅ [Поток {thread_id}] Изменения: {', '.join(changes)}")
            else:
                result_msg = "✅ Профиль обновлен:\n   • без изменений"
                logger.info(f"ℹ️ [Поток {thread_id}] Изменений нет")
            
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                result_msg
            )
            
            logger.info(f"✅ [Поток {thread_id}] Проверка профиля {token.name} завершена")

        except Exception as e:
            logger.error(f"❌ [Поток {thread_id}] Ошибка при проверке профиля {token.name}: {e}", exc_info=True)
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                "❌ Ошибка отправки запроса."
            )
    # ========================================

    def _parse_profile_response(self, text: str) -> Dict[str, Any]:
        """
        Парсер ответа на "Мой профиль"
        """
        result = {"level": None, "voices": None, "races": []}

        if not text:
            return result

        # Уровень
        m = RE_PROFILE_LEVEL.search(text)
        if m:
            try:
                result["level"] = int(m.group(1))
            except Exception:
                pass

        # Голоса из скобок у класса
        voices = None
        vm = self.RE_VOICES_FROM_CLASS_PARENS.search(text)
        if vm:
            try:
                voices = int(vm.group(1))
            except Exception:
                voices = None

        # fallback: старые regexes
        if voices is None:
            vm = RE_VOICES_GENERIC.search(text)
            if vm:
                try:
                    voices = int(vm.group(1))
                except Exception:
                    voices = None

        if voices is None:
            vm = RE_VOICES_ANY.search(text)
            if vm:
                try:
                    voices = int(vm.group(1))
                except Exception:
                    voices = None

        result["voices"] = voices

        # Расы
        text_lower = text.lower()
        race_mapping = {
            "человек": "ч", "гоблин": "г", "нежить": "н",
            "эльф": "э", "гном": "м", "демон": "д", "орк": "о",
            "людей": "ч", "гоблинов": "г", "нежити": "н",
            "эльфов": "э", "гномов": "м", "демонов": "д", "орков": "о",
        }

        races = []
        for race_name, race_key in race_mapping.items():
            if race_name in text_lower:
                races.append(race_key)

        result["races"] = sorted(list(set(races)))
        return result

    def _health(self, from_id: int):
        report = self.bot.health_monitor.get_detailed_report()
        if len(report) > 4000:
            report = report[:4000] + "\n..."
        self.bot.send_to_peer(self.bot.source_peer_id, report)

    def _diag(self, text: str, from_id: int):
        parts = text.split()
        if len(parts) == 1:
            report = [
                "📊 **ДИАГНОСТИКА**",
                f"🕒 Время: {time.strftime('%H:%M:%S')}",
                f"🤖 Тип: пользователь",
                f"📡 LongPoll: {'✅' if self.bot.user_longpoll._ready else '❌'}",
                f"📨 Очередь: {self.bot.user_message_queue.qsize()}",
                "",
                "Используй /диагностика [токен]"
            ]
            self.bot.send_to_peer(
                self.bot.source_peer_id, "\n".join(report)
            )
            return

        token_name = parts[1].strip()
        report = self.bot.health_monitor.get_detailed_report(token_name)
        self.bot.send_to_peer(self.bot.source_peer_id, report)

    def _apo_toggle(self, text: str, from_id: int):
        parts = text.strip().split()
        if len(parts) < 3:
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                "❌ Использование: /апо вкл|выкл ИмяТокена"
            )
            return

        action = parts[1].lower()
        name = " ".join(parts[2:]).strip()

        if action not in ("вкл", "выкл"):
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                "❌ Второй аргумент: 'вкл' или 'выкл'"
            )
            return

        token = self.bot.tm.get_token_by_name(name)
        if not token:
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                f"❌ Токен '{name}' не найден"
            )
            return

        if token.owner_vk_id == 0:
            token.fetch_owner_id_lazy()

        if token.owner_vk_id != from_id and from_id != self.OBSERVER_ID:
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                f"❌ Нет прав на токен '{name}'"
            )
            return

        new_state = (action == "вкл")
        if token.enabled == new_state:
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                f"ℹ️ {token.name} уже {'включен' if new_state else 'выключен'}"
            )
            return

        token.enabled = new_state
        token.mark_for_save()
        self.bot.tm.mark_for_save()
        self.bot.send_to_peer(
            self.bot.source_peer_id,
            f"✅ {token.name}: {'включен' if new_state else 'выключен'}"
        )

    def _change_races(self, text: str, from_id: int):
        parts = text.strip().split(maxsplit=2)
        if len(parts) < 3:
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                "❌ Использование: /сменарасы ИмяТокена ч,н"
            )
            return

        name = parts[1].strip()
        races_str = parts[2].replace(" ", "").replace(";", ",")
        race_keys_raw = [r for r in races_str.split(",") if r]

        if not race_keys_raw:
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                "❌ Не указаны новые расы"
            )
            return

        seen = set()
        race_keys = []
        for rk in race_keys_raw:
            if rk in seen:
                self.bot.send_to_peer(
                    self.bot.source_peer_id,
                    f"❌ Дубликат расы ('{rk}')"
                )
                return
            seen.add(rk)
            race_keys.append(rk)

        for rk in race_keys:
            if rk not in RACE_NAMES:
                self.bot.send_to_peer(
                    self.bot.source_peer_id,
                    f"❌ Неизвестная раса '{rk}'"
                )
                return

        token = self.bot.tm.get_token_by_name(name)
        if not token:
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                f"❌ Токен '{name}' не найден"
            )
            return

        if token.owner_vk_id == 0:
            token.fetch_owner_id_lazy()

        if token.owner_vk_id != from_id and from_id != self.OBSERVER_ID:
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                f"❌ Нет прав на токен '{name}'"
            )
            return

        if token.class_type != "apostle":
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                f"❌ {token.name} не апостол"
            )
            return

        token.races = race_keys
        token.temp_races = []
        token.mark_for_save()
        self.bot.tm.update_race_index(token)
        self.bot.tm.mark_for_save()

        human = "/".join(RACE_NAMES.get(r, r) for r in race_keys)
        self.bot.send_to_peer(
            self.bot.source_peer_id,
            f"✅ {token.name}: расы изменены на {human}"
        )

    def _voices(self, from_id: int, voices: int):
        token = self.bot.tm.get_token_by_sender_id(from_id)
        if not token:
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                f"❌ Апостол с ID {from_id} не найден"
            )
            return

        token.update_voices_manual(voices)
        self.bot.send_to_peer(
            self.bot.source_peer_id,
            f"✅ {token.name}: голоса = {voices}"
        )

    def _doprasa(self, text: str, from_id: int, msg: dict):
        from .commands import parse_doprasa_cmd
        from .utils import (
            timestamp_to_moscow, now_moscow, format_moscow_time
        )

        parsed = parse_doprasa_cmd(text, msg)
        if not parsed:
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                "❌ Использование: /допраса [раса] [имя_токена]\n"
                "📌 Нужно переслать сообщение с бафом"
            )
            return

        race_key, token_name, original_timestamp, _ = parsed

        token = None
        if token_name:
            token = self.bot.tm.get_token_by_name(token_name)
            if not token:
                self.bot.send_to_peer(
                    self.bot.source_peer_id,
                    f"❌ Токен '{token_name}' не найден"
                )
                return
            if token.owner_vk_id == 0:
                token.fetch_owner_id_lazy()
            if token.owner_vk_id != from_id and from_id != self.OBSERVER_ID:
                self.bot.send_to_peer(
                    self.bot.source_peer_id,
                    f"❌ Нет прав на '{token_name}'"
                )
                return
        else:
            token = self.bot.tm.get_token_by_sender_id(from_id)
            if not token:
                self.bot.send_to_peer(
                    self.bot.source_peer_id,
                    f"❌ Апостол с ID ({from_id}) не найден"
                )
                return

        obs_token = self.bot.tm.get_observer_token_object()
        if obs_token and token.id == obs_token.id:
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                "❌ Observer не может получать расы"
            )
            return

        if token.class_type != "apostle":
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                f"❌ {token.name} не апостол"
            )
            return

        token._cleanup_expired_temp_races(force=True)

        if race_key in token.races:
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                f"⚠️ У {token.name} уже есть постоянная раса"
            )
            return

        for tr in token.temp_races:
            if tr["race"] == race_key:
                self.bot.send_to_peer(
                    self.bot.source_peer_id,
                    f"⚠️ У {token.name} уже есть эта временная раса"
                )
                return

        if len(token.temp_races) >= 1:
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                f"⚠️ У {token.name} уже есть временная раса"
            )
            return

        if not original_timestamp:
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                "❌ Нужно переслать сообщение с бафом"
            )
            return

        start_moscow = timestamp_to_moscow(original_timestamp)
        end_moscow = timestamp_to_moscow(original_timestamp + 2 * 3600)

        if end_moscow < now_moscow():
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                f"❌ Время бафа истекло ({format_moscow_time(start_moscow)})"
            )
            return

        success = token.add_temporary_race(
            race_key, expires_at=original_timestamp + 2 * 3600
        )
        if success:
            self.bot.tm.update_race_index(token)
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                f"✅ {token.name}: временная раса '{RACE_NAMES.get(race_key, race_key)}'\n"
                f"⏰ {format_moscow_time(start_moscow)} → {format_moscow_time(end_moscow)}\n"
                f"📌 Можно использовать !баф{race_key}"
            )
        else:
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                f"❌ Не удалось добавить расу"
            )

    def _apo_status(self, from_id: int):
        status = self.bot._format_apo_status()
        self.bot.send_to_peer(self.bot.source_peer_id, status)

    def _baf(
        self, letters: str, from_id: int, text: str,
        user_cmid: Optional[int], msg_id: Optional[int]
    ):
        """Команда /баф"""
        logger.info(f"🔍 _baf: from_id={from_id}, letters={letters}, user_cmid={user_cmid}")

        if self.bot.state.has_active(from_id):
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                "❌ У вас уже есть активные бафы"
            )
            return

        job = Job(
            sender_id=from_id,
            trigger_text=text,
            letters=letters,
            created_ts=time.time(),
            registration_msg_id=None
        )

        self.bot.state.register_job(from_id, job, letters, user_cmid)
        registration_text = build_registration_text(letters)
        
        success, result = self.bot.send_to_peer(
            self.bot.source_peer_id,
            registration_text
        )

        if success and result and isinstance(result, dict):
            message_id = result.get('message_id', 0)
            cmid = result.get('cmid', 0)
            effective_id = message_id if message_id > 0 else cmid

            if effective_id and effective_id > 0:
                self.bot.state.update_message_id(from_id, effective_id)
                job.registration_msg_id = effective_id
                if cmid:
                    self.bot.message_cmids[effective_id] = cmid
                logger.info(f"✅ registration_msg_id={effective_id} для user_id={from_id}")

        self.bot.scheduler.enqueue_letters(job, letters)
