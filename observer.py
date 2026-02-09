# -*- coding: utf-8 -*-
import logging
import time
import asyncio
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from .constants import CLASS_ABILITIES, RACE_NAMES, RACE_EMOJIS
from .models import Job
from .scheduler import Scheduler
from .health import TokenHealthMonitor
from .utils import timestamp_to_moscow, now_moscow, format_moscow_time, normalize_text
from .commands import (
    parse_baf_letters,
    parse_golosa_cmd,
    parse_doprasa_cmd,
    is_apo_cmd,
    is_baf_cancel_cmd,
)
from .notifications import build_registration_text, build_final_text
from .state_store import JobStateStore


logger = logging.getLogger(__name__)


class ObserverBot:
    def __init__(self, tm, executor):
        self.tm = tm
        self.executor = executor
        self.scheduler = Scheduler(tm, executor, on_buff_complete=self._handle_buff_completion)
        self.health_monitor = TokenHealthMonitor(tm)

        # Добавляем ссылку на profile_manager (будет установлен в main.py)
        self.profile_manager = None

        self.observer = self.tm.get_observer()

        # Проверяем, является ли Observer группой
        self.is_group = hasattr(self.observer, 'group_handler')

        if self.is_group:
            logger.info(f"👥 Observer работает как группа: {self.observer.name}")
            self.source_peer_id = self.observer.source_peer_id
            # Если source_peer_id 0, вычисляем из настроек
            if not self.source_peer_id or self.source_peer_id == 0:
                source_chat_id = self.tm.settings.get("observer_source_chat_id", 120)
                self.source_peer_id = 2000000000 + source_chat_id if source_chat_id else 0
                logger.info(f"📌 Вычисленный source_peer_id: {self.source_peer_id}")
        else:
            logger.info(f"👤 Observer работает как пользовательский токен: {self.observer.name}")
            if not self.observer.access_token:
                raise RuntimeError("Observer token has empty access_token")
            if not self.observer.source_peer_id:
                raise RuntimeError("Observer source_chat_id is missing")
            self.source_peer_id = self.observer.source_peer_id

        self.poll_interval = float(self.tm.settings.get("poll_interval", 2.0))
        self.poll_count = int(self.tm.settings.get("poll_count", 20))

        # Thread-safe state
        self.state = JobStateStore(storage_path="jobs.json")
        self.state.restore_and_enqueue(self.scheduler)

        logging.info("🤖 MultiTokenBot STARTED (Observer=LongPoll)")
        logging.info(f"📋 Tokens: {len(self.tm.tokens)}")
        logging.info(f"🛰️ Target poll: interval={self.poll_interval}s, count={self.poll_count}")
        logging.info(f"📌 Source peer ID: {self.source_peer_id}")

        # LongPoll переменные (для пользовательского токена)
        self._lp_server: str = ""
        self._lp_key: str = ""
        self._lp_ts: str = ""

    # -------------------- Commands --------------------

    def _handle_health_command(self, from_id: int, text: str) -> None:
        report = self.health_monitor.get_detailed_report()
        if len(report) > 4000:
            report = report[:4000] + "\n... (сообщение обрезано)"
        self.observer.send_to_peer(self.source_peer_id, report, None)

    def _handle_diagnostic_command(self, from_id: int, text: str) -> None:
        parts = (text or "").split()
        if len(parts) == 1:
            self.observer.send_to_peer(
                self.source_peer_id,
                "❌ Укажите имя токена: !диагностика [имя_токена]",
                None,
            )
            return

        token_name = parts[1].strip()
        report = self.health_monitor.get_detailed_report(token_name)
        self.observer.send_to_peer(self.source_peer_id, report, None)

    def _apply_manual_voices_by_name(self, name: str, n: int) -> str:
        token = self.tm.get_token_by_name(name)
        if not token:
            return f"❌ Токен с именем '{name}' не найден."
        token.update_voices_manual(n)
        return f"✅ {token.name}: голоса выставлены = {n}"

    def _format_races_simple(self, token) -> str:
        token._cleanup_expired_temp_races(force=True)
        parts: List[str] = []
        if token.races:
            parts.append("/".join(sorted(token.races)))

        temp_parts: List[str] = []
        for tr in token.temp_races:
            race_key = tr["race"]
            expires = tr["expires"]
            remaining = int(expires - time.time())
            if remaining > 0:
                if remaining >= 3600:
                    hours = remaining // 3600
                    minutes = (remaining % 3600) // 60
                    time_str = f"{hours}ч{minutes:02d}м"
                else:
                    minutes = remaining // 60
                    seconds = remaining % 60
                    time_str = f"{minutes}м{seconds:02d}с"
                temp_parts.append(f"{race_key}({time_str})")

        if temp_parts:
            parts.append("/".join(sorted(temp_parts)))

        return "/".join(parts) if parts else "-"

    def _format_apo_status(self) -> str:
        apostles = [t for t in self.tm.all_buffers() if t.class_type == "apostle"]
        warlocks = [t for t in self.tm.all_buffers() if t.class_type == "warlock"]
        paladins = [t for t in self.tm.all_buffers() if t.class_type in ("crusader", "light_incarnation")]

        lines: List[str] = []

        if apostles:
            lines.append("🎭 Апостолы")
            for t in apostles:
                races_str = self._format_races_simple(t)
                manual = " ⚠️" if t.needs_manual_voices else ""
                lines.append(f" {t.name}: {races_str} | 🗣️ {t.voices}{manual}")
            lines.append("")

        if warlocks:
            lines.append("🧙 Проклинающие")
            for t in warlocks:
                manual = " ⚠️" if t.needs_manual_voices else ""
                lines.append(f" {t.name} | 🗣️ {t.voices}{manual}")
            lines.append("")

        if paladins:
            lines.append("⚔️ Паладины")
            for t in paladins:
                manual = " ⚠️" if t.needs_manual_voices else ""
                lines.append(f" {t.name} (lvl {t.level}) | 🗣️ {t.voices}{manual}")
            lines.append("")

        if not lines:
            return "Нет баферов в конфиге."

        return "\n".join(lines).strip()

    def _handle_doprasa_command(self, from_id: int, text: str, msg_item: Dict[str, Any]) -> None:
        parsed = parse_doprasa_cmd(text, msg_item)
        if not parsed:
            self.observer.send_to_peer(
                self.source_peer_id,
                "❌ Использование: /допраса [раса] [имя_токена_опционально]\n"
                "📌 Команда для добавления расы, если апостол УЖЕ получил баф в другом месте\n"
                "📌 Нужно переслать сообщение с успешным бафом\n"
                "Примеры:\n"
                " /допраса ч\n"
                " /допраса ч Апостол2",
                None,
            )
            return

        race_key, token_name, original_timestamp, _ = parsed

        token = None
        if token_name:
            token = self.tm.get_token_by_name(token_name)
            if not token:
                self.observer.send_to_peer(self.source_peer_id, f"❌ Токен '{token_name}' не найден.", None)
                return
            if token.owner_vk_id == 0:
                token.fetch_owner_id_lazy()
            if token.owner_vk_id != 0 and token.owner_vk_id != from_id:
                self.observer.send_to_peer(self.source_peer_id, f"❌ Нет прав на '{token_name}'.", None)
                return
        else:
            token = self.tm.get_token_by_sender_id(from_id)
            if not token:
                self.observer.send_to_peer(
                    self.source_peer_id,
                    f"❌ Апостол с вашим ID ({from_id}) не найден.",
                    None,
                )
                return

        obs_token = self.tm.get_observer_token_object()
        if obs_token and token.id == obs_token.id:
            self.observer.send_to_peer(
                self.source_peer_id,
                "❌ Observer токен не является апостолом и не может получать расы.",
                None,
            )
            return

        if token.class_type != "apostle":
            self.observer.send_to_peer(self.source_peer_id, f"❌ {token.name} не апостол.", None)
            return

        token._cleanup_expired_temp_races(force=True)

        if race_key in token.races:
            self.observer.send_to_peer(self.source_peer_id, f"⚠️ У {token.name} уже есть постоянная раса.", None)
            return

        if any(tr["race"] == race_key for tr in token.temp_races):
            self.observer.send_to_peer(self.source_peer_id, f"⚠️ У {token.name} уже есть эта временная раса.", None)
            return

        if token.get_temp_race_count() >= 1:
            self.observer.send_to_peer(
                self.source_peer_id,
                f"⚠️ У {token.name} уже есть временная раса (можно только одну).",
                None,
            )
            return

        if not original_timestamp:
            self.observer.send_to_peer(
                self.source_peer_id,
                "❌ Нужно переслать сообщение с успешным бафом.\n"
                "📌 Ответьте на сообщение с бафом или перешлите его.",
                None,
            )
            return

        start_moscow = timestamp_to_moscow(original_timestamp)
        end_moscow = timestamp_to_moscow(original_timestamp + 2 * 3600)

        if end_moscow < now_moscow():
            self.observer.send_to_peer(
                self.source_peer_id,
                f"❌ Время бафа уже истекло (сообщение от {format_moscow_time(start_moscow)}).",
                None,
            )
            return

        success = token.add_temporary_race(race_key, expires_at=original_timestamp + 2 * 3600)
        if success:
            self.tm.update_race_index(token)
            self.observer.send_to_peer(
                self.source_peer_id,
                f"✅ {token.name}: добавлена временная раса '{RACE_NAMES.get(race_key, race_key)}'\n"
                f"⏰ {format_moscow_time(start_moscow)} → {format_moscow_time(end_moscow)}\n"
                f"📌 Теперь можно использовать !баф{race_key}",
                None,
            )
        else:
            self.observer.send_to_peer(
                self.source_peer_id,
                f"❌ Не удалось добавить временную расы для {token.name}.",
                None,
            )

    def _find_owned_token_by_name(self, owner_id: int, name: str):
        token = self.tm.get_token_by_name(name)
        if not token:
            return None, f"❌ Токен с именем '{name}' не найден в конфиге."

        if token.owner_vk_id == 0:
            token.fetch_owner_id_lazy()

        if token.owner_vk_id != 0 and token.owner_vk_id != owner_id:
            return None, f"❌ У вас нет прав на токен '{name}'."

        return token, None

    def _handle_apo_toggle(self, from_id: int, norm: str, text: str) -> None:
        # !апо вкл Ник  /  !апо выкл Ник
        parts = text.strip().split()
        if len(parts) < 3:
            self.observer.send_to_peer(
                self.source_peer_id,
                "❌ Использование: !апо вкл|выкл ИмяТокена",
                None,
            )
            return

        action = parts[1].lower()
        name = " ".join(parts[2:]).strip()

        if action not in ("вкл", "выкл"):
            self.observer.send_to_peer(
                self.source_peer_id,
                "❌ Второй аргумент должен быть 'вкл' или 'выкл'.",
                None,
            )
            return

        token, err = self._find_owned_token_by_name(from_id, name)
        if err:
            self.observer.send_to_peer(self.source_peer_id, err, None)
            return

        new_state = (action == "вкл")
        if token.enabled == new_state:
            self.observer.send_to_peer(
                self.source_peer_id,
                f"ℹ️ {token.name} уже {'включен' if new_state else 'выключен'}.",
                None,
            )
            return

        token.enabled = new_state
        token.mark_for_save()
        self.tm.mark_for_save()

        self.observer.send_to_peer(
            self.source_peer_id,
            f"✅ {token.name}: {'включен' if new_state else 'выключен'}.",
            None,
        )

    def _handle_change_races(self, from_id: int, text: str) -> None:
        # !сменарасы ИмяТокена ч,н
        parts = text.strip().split(maxsplit=2)
        if len(parts) < 3:
            self.observer.send_to_peer(
                self.source_peer_id,
                "❌ Использование: !сменарасы ИмяТокена ч,н",
                None,
            )
            return

        name = parts[1].strip()
        races_str = parts[2].replace(" ", "")
        races_str = races_str.replace(";", ",")
        race_keys_raw = [r for r in races_str.split(",") if r]

        if not race_keys_raw:
            self.observer.send_to_peer(
                self.source_peer_id,
                "❌ Не указаны новые расы.",
                None,
            )
            return

        # убираем дубли и сразу режем по первой повторяющейся
        seen = set()
        race_keys: List[str] = []
        for rk in race_keys_raw:
            if rk in seen:
                self.observer.send_to_peer(
                    self.source_peer_id,
                    f"❌ Нельзя указывать одну и ту же расу несколько раз ('{rk}').",
                    None,
                )
                return
            seen.add(rk)
            race_keys.append(rk)

        # проверяем, что все расы существуют
        for rk in race_keys:
            if rk not in RACE_NAMES:
                self.observer.send_to_peer(
                    self.source_peer_id,
                    f"❌ Неизвестная раса '{rk}'.",
                    None,
                )
                return

        token, err = self._find_owned_token_by_name(from_id, name)
        if err:
            self.observer.send_to_peer(self.source_peer_id, err, None)
            return

        if token.class_type != "apostle":
            self.observer.send_to_peer(
                self.source_peer_id,
                f"❌ {token.name} не апостол.",
                None,
            )
            return

        token.races = race_keys
        token.temp_races = []
        token.mark_for_save()
        self.tm.update_race_index(token)
        self.tm.mark_for_save()

        human = "/".join(RACE_NAMES.get(r, r) for r in race_keys)
        self.observer.send_to_peer(
            self.source_peer_id,
            f"✅ {token.name}: основные расы изменены на {human}.",
            None,
        )

    # -------------------- LongPoll --------------------

    def _lp_get_server(self) -> bool:
        """Получение LongPoll сервера (работает для обоих типов)"""
        if self.is_group:
            # Для группы
            logger.info(f"🔍 Получение LongPoll сервера для группы {self.observer.name}")
            
            # Пробуем несколько раз получить сервер
            for attempt in range(3):
                try:
                    success = self.observer.group_handler.get_long_poll_server()
                    if success:
                        logger.info(f"✅ LongPoll для группы инициализирован")
                        return True
                    else:
                        logger.warning(f"⚠️ Попытка {attempt + 1}/3 не удалась")
                        if attempt < 2:
                            time.sleep(2)
                except Exception as e:
                    logger.error(f"❌ Ошибка получения LongPoll сервера: {e}")
                    if attempt < 2:
                        time.sleep(2)
            
            logger.error(f"❌ Не удалось получить LongPoll для группы после 3 попыток")
            return False
        else:
            # Для пользовательского токена
            data = {"access_token": self.observer.access_token, "v": "5.131", "lp_version": 3}
            ret = self.observer._vk.call(self.observer._vk.post("messages.getLongPollServer", data))

            if "error" in ret:
                err = ret["error"]
                logging.error(f"❌ LongPollServer error {err.get('error_code')} {err.get('error_msg')}")
                return False

            resp = ret.get("response", {})
            self._lp_server = str(resp.get("server", "")).strip()
            self._lp_key = str(resp.get("key", "")).strip()
            self._lp_ts = str(resp.get("ts", "")).strip()

            if not self._lp_server or not self._lp_key or not self._lp_ts:
                logging.error("❌ LongPollServer: missing server/key/ts")
                return False

            logging.info(f"✅ LongPoll initialized: server={self._lp_server}, ts={self._lp_ts}")
            return True

    def _lp_check(self) -> Optional[Dict[str, Any]]:
        """Проверка LongPoll (работает для обоих типов)"""
        if self.is_group:
            # Для группы
            if not hasattr(self.observer.group_handler, '_lp_server') or not self.observer.group_handler._lp_server:
                logger.error("❌ Group LongPoll не инициализирован")
                return None

            server_raw = self.observer.group_handler._lp_server

            # Убедимся, что URL правильный
            if not server_raw.startswith("http"):
                # Если нет протокола, добавляем https://
                server = "https://" + server_raw
            elif server_raw.startswith("http://"):
                # Заменяем http:// на https://
                server = "https://" + server_raw[7:]
            else:
                server = server_raw

            # Проверяем, что URL валидный
            if "://" not in server or len(server) < 10:
                logger.error(f"❌ Некорректный LongPoll сервер: {server}")
                # Пробуем стандартный сервер VK
                server = "https://lp.vk.com"
                logger.info(f"🔄 Используем стандартный сервер: {server}")

            data = {"act": "a_check", "key": self.observer.group_handler._lp_key,
                   "ts": self.observer.group_handler._lp_ts, "wait": 25, "mode": 2, "version": 3}
        else:
            # Для пользовательского токена
            if not self._lp_server:
                logger.error("❌ LongPoll не инициализирован")
                return None

            server = "https://" + self._lp_server
            data = {"act": "a_check", "key": self._lp_key, "ts": self._lp_ts,
                   "wait": 25, "mode": 2, "version": 3}

        try:
            logger.debug(f"🔍 LongPoll запрос к: {server}")
            
            # Синхронный запрос через aiohttp
            timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=20)
            
            async def make_request():
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(server, params=data) as resp:
                        # Проверяем content-type
                        content_type = resp.headers.get('Content-Type', '').lower()
                        if 'application/json' not in content_type:
                            # Пробуем прочитать как текст
                            text = await resp.text()
                            logger.warning(f"⚠️ LongPoll вернул не JSON: {text[:100]}")
                            if 'failed' in text:
                                try:
                                    # Пробуем распарсить несмотря на content-type
                                    return await resp.json(content_type=None)
                                except:
                                    pass
                            # Возвращаем ошибку
                            return {"failed": 2, "reason": f"Invalid content-type: {content_type}"}
                        
                        return await resp.json()
            
            # Запускаем асинхронный запрос
            return asyncio.run(make_request())
            
        except aiohttp.ClientError as e:
            logger.error(f"📡 Сетевая ошибка LongPoll: {e}")
            
            # При определенных ошибках пробуем другой сервер
            if any(err in str(e) for err in ["Name or service not known", "Cannot connect", "Timeout"]):
                logger.warning(f"⚠️ Проблема с сервером, пробуем стандартный VK сервер")
                # Пробуем стандартный сервер VK
                alt_server = "https://lp.vk.com"
                try:
                    timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=20)
                    
                    async def alt_request():
                        async with aiohttp.ClientSession(timeout=timeout) as session:
                            async with session.get(alt_server, params=data) as resp:
                                content_type = resp.headers.get('Content-Type', '').lower()
                                if 'application/json' not in content_type:
                                    text = await resp.text()
                                    logger.warning(f"⚠️ Alt сервер вернул не JSON: {text[:100]}")
                                    return {"failed": 2, "reason": f"Invalid content-type from alt: {content_type}"}
                                return await resp.json()
                    
                    return asyncio.run(alt_request())
                except Exception as e2:
                    logger.error(f"❌ Ошибка альтернативного сервера: {e2}")
                    return {"failed": 2, "reason": str(e2)}
            
            # Возвращаем failed: 2 для переподключения
            return {"failed": 2, "reason": str(e)}
            
        except Exception as e:
            logger.error(f"❌ LongPoll a_check exception: {e}", exc_info=True)
            return {"failed": 2, "reason": str(e)}

    # -------------------- Scheduler callback --------------------

    def _handle_buff_completion(self, job: Job, buff_info: Dict[str, Any]) -> None:
        # Keep this minimal: persist & maybe finalize, then send outside.
        should_finalize, snapshot = self.state.apply_completion(job, buff_info)
        if should_finalize and snapshot:
            txt = build_final_text(job.sender_id, snapshot, self.tm)
            if txt:
                sent_ok, send_status = self.observer.send_to_peer(self.source_peer_id, txt)
                if not sent_ok:
                    logging.error(
                        f"❌ Не удалось отправить финальное уведомление {job.sender_id}: {send_status}"
                    )

    # -------------------- Message dispatch --------------------

    def _handle_new_message(self, msg_item: Dict[str, Any]) -> None:
        text = (msg_item.get("text") or "").strip()
        from_id = int(msg_item.get("from_id", 0))
        peer_id = int(msg_item.get("peer_id", 0))
        cmid = msg_item.get("conversation_message_id")
        
        logger.info(f"🎯 Обработка сообщения: от={from_id}, чат={peer_id}, текст='{text}', cmid={cmid}")

        if peer_id != self.source_peer_id:
            logger.warning(f"⚠️ Сообщение не из целевого чата: {peer_id} != {self.source_peer_id}")
            return
        if from_id <= 0 or not text:
            logger.warning(f"⚠️ Пустое сообщение или from_id={from_id}")
            return

        norm = normalize_text(text)

        if is_baf_cancel_cmd(norm):
            had_job, letters = self.state.cancel_and_clear(from_id)
            if not had_job:
                self.observer.send_to_peer(
                    self.source_peer_id,
                    "❌ У вас нет активных бафов для отмены.",
                    None,
                )
                return
            cancelled = self.scheduler.cancel_user_jobs(from_id)
            self.observer.send_to_peer(
                self.source_peer_id,
                (
                    f"✅ Все ваши бафы ({letters}) отменены."
                    if cancelled
                    else "⚠️ Не удалось найти ваши бафы в очереди."
                ),
                None,
            )
            return

        if norm in ["!здоровье", "!health", "!статус"]:
            self._handle_health_command(from_id, text)
            return

        if norm.startswith("!диагностика"):
            self._handle_diagnostic_command(from_id, text)
            return

        # !апо вкл/выкл ИмяТокена
        if norm.startswith("!апо "):
            self._handle_apo_toggle(from_id, norm, text)
            return

        # !сменарасы ИмяТокена ч,н
        if norm.startswith("!сменарасы"):
            self._handle_change_races(from_id, text)
            return

        parsed_g = parse_golosa_cmd(text)
        if parsed_g is not None:
            _, n = parsed_g
            token = self.tm.get_token_by_sender_id(from_id)
            if not token:
                self.observer.send_to_peer(
                    self.source_peer_id,
                    f"❌ Апостол с вашим ID ({from_id}) не найден в конфиге.",
                    None,
                )
                return

            reply = self._apply_manual_voices_by_name(token.name, n)
            self.observer.send_to_peer(self.source_peer_id, reply, None)
            return

        if norm.startswith("/допраса"):
            self._handle_doprasa_command(from_id, text, msg_item)
            return

        if is_apo_cmd(norm):
            logger.info(f"📋 Обработка команды !апо от {from_id}")
            status = self._format_apo_status()
            logger.info(f"📤 Отправка статуса апостолов ({len(status)} символов)")
            self.observer.send_to_peer(self.source_peer_id, status, None)
            logger.info(f"✅ Статус апостолов отправлен")
            return

        letters = parse_baf_letters(text)
        if letters:
            if self.state.has_active(from_id):
                self.observer.send_to_peer(
                    self.source_peer_id,
                    "❌ У вас уже есть активные бафы. Дождитесь их выполнения или отмените командой '!баф отмена'.",
                    None,
                )
                return

            job = Job(sender_id=from_id, trigger_text=text, letters=letters, created_ts=time.time())
            self.state.register_job(from_id, job, letters, cmid)

            # try to send registration notice (outside any locks)
            if cmid:
                sent_ok, send_status = self.observer.send_to_peer(
                    self.source_peer_id,
                    build_registration_text(letters),
                )
                if sent_ok and "OK:" in (send_status or ""):
                    try:
                        mid = int(send_status.split(":")[1])
                        self.state.update_message_id(from_id, mid)
                    except Exception:
                        pass

            # enqueue
            self.scheduler.enqueue_letters(job, letters)

    def run(self) -> None:
        retry_count = 0
        max_retries = 10
        retry_delay = 5

        while True:
            try:
                if not self._lp_get_server():
                    logging.error(
                        f"❌ Не удалось получить LongPoll сервер (попытка {retry_count + 1}/{max_retries})"
                    )
                    retry_count += 1
                    if retry_count >= max_retries:
                        logging.critical("💥 Превышено максимальное количество попыток получения LongPoll сервера")
                        break

                    time.sleep(min(retry_delay * retry_count, 300))
                    continue

                retry_count = 0
                logging.info(f"✅ LongPoll готов. Слушаю чат {self.source_peer_id}")

                while True:
                    try:
                        lp = self._lp_check()
                        if not lp:
                            time.sleep(2)
                            continue

                        if "failed" in lp:
                            error_code = lp.get("failed")
                            reason = lp.get("reason", "")
                            logging.warning(f"⚠️ LongPoll failed with code: {error_code}, reason: {reason}")

                            if error_code == 1:
                                new_ts = lp.get("ts")
                                if new_ts:
                                    if self.is_group:
                                        self.observer.group_handler._lp_ts = str(new_ts)
                                    else:
                                        self._lp_ts = str(new_ts)
                                    logging.info(f"🔄 LongPoll: обновлен ts на {new_ts}")
                                continue
                            elif error_code == 2:
                                logging.error("❌ LongPoll: ключ устарел, обновляю...")
                                # Ждем немного перед переподключением
                                time.sleep(2)
                                
                                # Очищаем текущие LongPoll данные
                                if self.is_group:
                                    self.observer.group_handler._lp_server = ""
                                    self.observer.group_handler._lp_key = ""
                                    self.observer.group_handler._lp_ts = ""
                                else:
                                    self._lp_server = ""
                                    self._lp_key = ""
                                    self._lp_ts = ""
                                
                                # Пробуем переподключиться
                                if not self._lp_get_server():
                                    logging.error("❌ Не удалось переподключить LongPoll, ждем 10 секунд")
                                    time.sleep(10)
                                else:
                                    logging.info("✅ LongPoll успешно переподключен")
                                break  # Выходим из внутреннего цикла для переподключения
                            elif error_code == 3:
                                logging.info("🔄 LongPoll: информация устарела, обновляю...")
                                if not self._lp_get_server():
                                    time.sleep(5)
                                else:
                                    logging.info("✅ LongPoll обновлен")
                                continue
                            elif error_code == 4:
                                logging.error("❌ LongPoll: неверная версия протокола")
                                time.sleep(60)
                                continue
                            else:
                                logging.error(f"❌ LongPoll: неизвестная ошибка {error_code}")
                                time.sleep(5)
                                continue

                        new_ts = lp.get("ts")
                        if new_ts is not None:
                            if self.is_group:
                                self.observer.group_handler._lp_ts = str(new_ts)
                            else:
                                self._lp_ts = str(new_ts)

                        updates = lp.get("updates", []) or []
                        
                        # Детальное логирование
                        if updates:
                            logger.info(f"📨 Получено обновлений: {len(updates)}")
                            for i, u in enumerate(updates[:3]):
                                logger.info(f"  Обновление {i}: тип данных = {type(u)}, данные: {u}")

                        if not updates:
                            continue

                        msg_ids: List[int] = []
                        messages_to_process = []  # Новый список для хранения сообщений в новом формате

                        for update in updates:
                            if self.is_group:
                                # НОВЫЙ ФОРМАТ для группы (Callback API стиль)
                                if isinstance(update, dict):
                                    # Проверяем тип события
                                    event_type = update.get("type")
                                    if event_type == "message_new":
                                        message_obj = update.get("object", {}).get("message", {})
                                        if message_obj:
                                            text = message_obj.get("text", "")
                                            from_id = message_obj.get("from_id", 0)
                                            peer_id = message_obj.get("peer_id", 0)
                                            msg_id = message_obj.get("id", 0)
                                            
                                            logger.info(f"  Сообщение от группы: ID={msg_id}, от={from_id}, чат={peer_id}, текст='{text[:50]}...'")
                                            
                                            # Проверяем что сообщение из нужного чата
                                            if peer_id == self.source_peer_id:
                                                msg_ids.append(msg_id)
                                                messages_to_process.append(message_obj)  # Сохраняем сообщение для обработки
                                    else:
                                        logger.debug(f"  Пропускаем событие типа: {event_type}")
                                else:
                                    logger.warning(f"  Неизвестный формат обновления: {type(update)}")
                            else:
                                # СТАРЫЙ ФОРМАТ для пользовательского токена
                                if not isinstance(update, list) or not update:
                                    continue
                                if int(update[0]) != 4:  # Код 4 = новое сообщение
                                    logger.debug(f"  Пропускаем код события: {update[0]}")
                                    continue
                                try:
                                    msg_id = int(update[1])
                                    p_id = int(update[3])
                                    from_id = int(update[6]) if len(update) > 6 else 0
                                    logger.info(f"  Сообщение ID: {msg_id}, чат: {p_id}, от: {from_id}")
                                except Exception as e:
                                    logger.error(f"  Ошибка парсинга события: {e}")
                                    continue
                                if p_id == self.source_peer_id:
                                    msg_ids.append(msg_id)

                        logger.info(f"  Сообщений для обработки: {len(msg_ids)} (группа={len(messages_to_process)})")

                        # Обработка сообщений
                        if self.is_group and messages_to_process:
                            # Для группы: используем сообщения уже полученные из события
                            logger.info(f"  Обрабатываем {len(messages_to_process)} сообщений из событий группы")
                            for message_obj in messages_to_process:
                                self._handle_new_message(message_obj)
                        elif msg_ids:
                            # Для пользовательского токена: получаем сообщения по ID
                            items = self.observer.get_by_id(msg_ids)
                            logger.info(f"  Получено сообщений из API: {len(items)}")
                            for it in items:
                                self._handle_new_message(it)

                    except aiohttp.ClientError as e:
                        logging.error(f"📡 Сетевая ошибка LongPoll: {e}")
                        time.sleep(5)
                        continue
                    except Exception as e:
                        logging.error(f"❌ Ошибка в LongPoll цикле: {e}", exc_info=True)
                        time.sleep(5)
                        continue

            except Exception as e:
                logging.error(f"❌ Критическая ошибка в Observer: {e}", exc_info=True)
                retry_count += 1
                if retry_count >= max_retries:
                    logging.critical("💥 Превышено максимальное количество попыток переподключения")
                    break

                delay = min(retry_delay * (2**retry_count), 300)
                logging.info(
                    f"🔄 Переподключение через {delay} секунд (попытка {retry_count}/{max_retries})"
                )
                time.sleep(delay)
