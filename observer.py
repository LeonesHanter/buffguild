# -*- coding: utf-8 -*-
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from .constants import CLASS_ABILITIES, RACE_NAMES
from .models import Job
from .scheduler import Scheduler
from .health import TokenHealthMonitor
from .validators import InputValidator
from .utils import timestamp_to_moscow, now_moscow, format_moscow_time, normalize_text

logger = logging.getLogger(__name__)


class ObserverBot:
    def __init__(self, tm, executor):
        self.tm = tm
        self.executor = executor

        # ✅ Создаем Scheduler с колбэком для уведомлений
        self.scheduler = Scheduler(tm, executor, on_buff_complete=self._handle_buff_completion)

        self.health_monitor = TokenHealthMonitor(tm)

        self.observer = tm.get_observer()
        if not self.observer.access_token:
            raise RuntimeError("Observer token has empty access_token")
        if not self.observer.source_peer_id:
            raise RuntimeError("Observer source_chat_id is missing")

        self.poll_interval = float(tm.settings.get("poll_interval", 2.0))
        self.poll_count = int(tm.settings.get("poll_count", 20))

        # ✅ НОВОЕ: Хранилище для уведомлений и отслеживания бафов
        self._active_jobs: Dict[int, Dict] = {}  # user_id -> {job, letters, registration_info}
        self._buff_results: Dict[int, Dict] = {}  # user_id -> {tokens_info, total_value, expected_count, completed_count}

        logging.info("🤖 MultiTokenBot STARTED (Observer=LongPoll)")
        logging.info(f"📋 Tokens: {len(tm.tokens)}")
        logging.info(f"🛰️ Target poll: interval={self.poll_interval}s, count={self.poll_count}")

        self._lp_server: str = ""
        self._lp_key: str = ""
        self._lp_ts: str = ""

    def _parse_baf_letters(self, text: str) -> str:
        text_n = normalize_text(text)
        if not text_n.startswith("!баф"):
            return ""
        s = text_n[4:].strip()
        if not s:
            return ""
        s = s[:4]

        allowed = set()
        for cls in CLASS_ABILITIES.values():
            allowed.update(cls["abilities"].keys())
        out = "".join([ch for ch in s if ch in allowed])
        return out[:4]

    def _is_apo_cmd(self, text: str) -> bool:
        return normalize_text(text).startswith("!апо")

    def _is_baf_cancel_cmd(self, text: str) -> bool:
        return normalize_text(text) == "!баф отмена"

    def _parse_golosa_cmd(self, text: str) -> Optional[Tuple[str, int]]:
        """Парсинг команды !голоса"""
        t = (text or "").strip()
        if not normalize_text(t).startswith("!голоса"):
            return None
        parts = t.split()
        if len(parts) != 3:
            return None
        name = parts[1].strip()
        try:
            n = int(parts[2].strip())
        except Exception:
            return None
        if not name:
            return None
        return name, max(0, n)

    def _apply_manual_voices_by_name(self, name: str, n: int) -> str:
        """Применить ручное обновление голосов"""
        token = self.tm.get_token_by_name(name)
        if not token:
            return f"❌ Токен с именем '{name}' не найден."
        token.update_voices_manual(n)
        # ✅ Теперь сохранение будет через отложенный механизм
        return f"✅ {token.name}: голоса выставлены = {n}"

    def _format_races_simple(self, token) -> str:
        # ✅ force cleanup чтобы !апо показывало корректно
        token._cleanup_expired_temp_races(force=True)

        parts = []
        if token.races:
            parts.append("/".join(sorted(token.races)))

        temp_parts = []
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
                lines.append(f"  {t.name}: {races_str} | 🗣️ {t.voices}{manual}")
            lines.append("")

        if warlocks:
            lines.append("🧙 Проклинающие")
            for t in warlocks:
                manual = " ⚠️" if t.needs_manual_voices else ""
                lines.append(f"  {t.name} | 🗣️ {t.voices}{manual}")
            lines.append("")

        if paladins:
            lines.append("⚔️ Паладины")
            for t in paladins:
                manual = " ⚠️" if t.needs_manual_voices else ""
                lines.append(f"  {t.name} (lvl {t.level}) | 🗣️ {t.voices}{manual}")
            lines.append("")

        if not lines:
            return "Нет баферов в конфиге."
        return "\n".join(lines).strip()

    def _parse_doprasa_cmd(self, text: str, msg_item: Dict[str, Any]) -> Optional[Tuple[str, Optional[str], Optional[int], str]]:
        t = InputValidator.sanitize_text(text, max_length=50)
        if not normalize_text(t).startswith("/допраса"):
            return None

        parts = t.split()
        if len(parts) < 2 or len(parts) > 3:
            return None

        race = parts[1].strip().lower()
        if not InputValidator.validate_race_key(race):
            return None

        token_name = None
        if len(parts) == 3:
            token_name = parts[2].strip()
            if not InputValidator.validate_token_name(token_name):
                return None

        original_timestamp = None
        if "reply_message" in msg_item:
            original_timestamp = InputValidator.validate_timestamp(msg_item["reply_message"].get("date"))
        elif "fwd_messages" in msg_item and msg_item["fwd_messages"]:
            original_timestamp = InputValidator.validate_timestamp(msg_item["fwd_messages"][0].get("date"))

        return race, token_name, original_timestamp, text

    def _handle_doprasa_command(self, from_id: int, text: str, msg_item: Dict[str, Any]) -> None:
        parsed = self._parse_doprasa_cmd(text, msg_item)
        if not parsed:
            self.observer.send_to_peer(
                self.observer.source_peer_id,
                "❌ Использование: /допраса [раса] [имя_токена_опционально]\n"
                "📌 Команда для добавления расы, если апостол УЖЕ получил баф в другом месте\n"
                "📌 Нужно переслать сообщение с успешным бафом\n"
                "Примеры:\n"
                "  /допраса ч\n"
                "  /допраса ч Апостол2",
                None,
            )
            return

        race_key, token_name, original_timestamp, _ = parsed

        token = None
        if token_name:
            token = self.tm.get_token_by_name(token_name)
            if not token:
                self.observer.send_to_peer(self.observer.source_peer_id, f"❌ Токен '{token_name}' не найден.", None)
                return

            if token.owner_vk_id == 0:
                token.fetch_owner_id_lazy()

            if token.owner_vk_id != 0 and token.owner_vk_id != from_id:
                self.observer.send_to_peer(self.observer.source_peer_id, f"❌ Нет прав на '{token_name}'.", None)
                return
        else:
            token = self.tm.get_token_by_sender_id(from_id)
            if not token:
                self.observer.send_to_peer(self.observer.source_peer_id, f"❌ Апостол с вашим ID ({from_id}) не найден.", None)
                return

        # ✅ ДОБАВИТЬ: проверка что это не Observer
        if token.id == self.observer.id:
            self.observer.send_to_peer(
                self.observer.source_peer_id,
                "❌ Observer токен не является апостолом и не может получать расы.",
                None
            )
            return

        if token.class_type != "apostle":
            self.observer.send_to_peer(self.observer.source_peer_id, f"❌ {token.name} не апостол.", None)
            return

        token._cleanup_expired_temp_races(force=True)

        if race_key in token.races:
            self.observer.send_to_peer(self.observer.source_peer_id, f"⚠️ У {token.name} уже есть постоянная раса.", None)
            return

        if any(tr["race"] == race_key for tr in token.temp_races):
            self.observer.send_to_peer(self.observer.source_peer_id, f"⚠️ У {token.name} уже есть эта временная раса.", None)
            return

        if token.get_temp_race_count() >= 1:
            self.observer.send_to_peer(self.observer.source_peer_id, f"⚠️ У {token.name} уже есть временная раса (можно только одну).", None)
            return

        if not original_timestamp:
            self.observer.send_to_peer(
                self.observer.source_peer_id,
                "❌ Нужно переслать сообщение с успешным бафом.\n📌 Ответьте на сообщение с бафом или перешлите его.",
                None,
            )
            return

        start_moscow = timestamp_to_moscow(original_timestamp)
        end_moscow = timestamp_to_moscow(original_timestamp + 2 * 3600)
        if end_moscow < now_moscow():
            self.observer.send_to_peer(self.observer.source_peer_id, f"❌ Время бафа уже истекло (сообщение от {format_moscow_time(start_moscow)}).", None)
            return

        success = token.add_temporary_race(race_key, expires_at=original_timestamp + 2 * 3600)
        if success:
            self.tm.update_race_index(token)
            self.observer.send_to_peer(
                self.observer.source_peer_id,
                f"✅ {token.name}: добавлена временная раса '{RACE_NAMES.get(race_key, race_key)}'\n"
                f"⏰ {format_moscow_time(start_moscow)} → {format_moscow_time(end_moscow)}\n"
                f"📌 Теперь можно использовать !баф{race_key}",
                None,
            )
        else:
            self.observer.send_to_peer(self.observer.source_peer_id, f"❌ Не удалось добавить временную расу для {token.name}.", None)

    def _handle_health_command(self, from_id: int, text: str) -> None:
        report = self.health_monitor.get_detailed_report()
        if len(report) > 4000:
            report = report[:4000] + "\n... (сообщение обрезано)"
        self.observer.send_to_peer(self.observer.source_peer_id, report, None)

    def _handle_diagnostic_command(self, from_id: int, text: str) -> None:
        parts = text.split()
        if len(parts) == 1:
            self.observer.send_to_peer(self.observer.source_peer_id, "❌ Укажите имя токена: !диагностика [имя_токена]", None)
            return
        token_name = parts[1].strip()
        report = self.health_monitor.get_detailed_report(token_name)
        self.observer.send_to_peer(self.observer.source_peer_id, report, None)

    def _lp_get_server(self) -> bool:
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
        server = "https://" + self._lp_server
        data = {"act": "a_check", "key": self._lp_key, "ts": self._lp_ts, "wait": 25, "mode": 2, "version": 3}
        try:
            return self.observer._vk.call(self.observer._vk.raw_post(server, data))
        except aiohttp.ClientError as e:
            logging.error(f"📡 Сетевая ошибка LongPoll: {e}")
            return None
        except Exception as e:
            logging.error(f"❌ LongPoll a_check exception: {e}", exc_info=True)
            return None

    def _send_registration_notification(self, from_id: int, letters: str, cmid: int) -> int:
        """Отправляет уведомление о регистрации бафа"""
        queue_size = self.scheduler.get_queue_size()

        notification_text = (
            f"✅ Баф зарегистрирован: {letters}\n"
            f"📊 Ожидается бафов: {len(letters)}\n"
            f"📌 Для отмены напишите: !баф отмена"
        )

        # Отправляем простое сообщение
        sent_ok, send_status = self.observer.send_to_peer(
            self.observer.source_peer_id,
            notification_text
        )

        if sent_ok and "OK:" in send_status:
            try:
                message_id = int(send_status.split(":")[1])
                return message_id
            except:
                pass

        return 0

    def _send_final_notification(self, user_id: int):
        """Отправляет финальное уведомление о бафе когда все бафы выполнены"""
        if user_id not in self._buff_results:
            return

        user_data = self._buff_results[user_id]

        if not user_data.get("tokens_info"):
            return

        # Формируем список токенов в нужном формате
        tokens_text = []
        for token_info in user_data["tokens_info"]:
            token_text = f"{token_info['token_name']}({token_info['buff_value']}"
            if token_info['is_critical']:
                token_text += "🍀"
            token_text += ")"
            tokens_text.append(token_text)

        # Форматируем итоговое сообщение
        notification_text = (
            f"🎉 Баф успешно выдан!\n"
            f"📈 Начислено: {', '.join(tokens_text)}\n"
            f"📉 Вычтено: ({user_data['total_value']}) баланса"
        )

        # Отправляем новое сообщение
        self.observer.send_to_peer(
            self.observer.source_peer_id,
            notification_text
        )

        # Очищаем данные пользователя
        if user_id in self._buff_results:
            del self._buff_results[user_id]

        if user_id in self._active_jobs:
            del self._active_jobs[user_id]

    def _handle_buff_completion(self, job: Job, buff_info: Dict):
        """Обработка завершения бафа - вызывается из Scheduler"""
        if not job or not buff_info:
            return

        user_id = job.sender_id

        # Если у пользователя нет активного job, значит это старый баф или ошибка
        if user_id not in self._active_jobs:
            logging.warning(f"⚠️ Получен баф для неактивного пользователя {user_id}")
            return

        # Получаем информацию о бафе
        buff_value = buff_info.get("buff_value", 0)
        is_critical = buff_info.get("is_critical", False)
        buff_name = buff_info.get("buff_name", "")
        token_name = buff_info.get("token_name", "")

        # ✅ УЛУЧШЕННАЯ проверка на крит - исправляем если executor ошибся
        original_buff_value = buff_value
        original_is_critical = is_critical

        # Проверяем текст бафа на наличие 30%
        if buff_name and ("30%" in buff_name or "+30%" in buff_name):
            is_critical = True
            buff_value = 150  # Принудительно ставим 150 для крита
            logging.info(f"🎯 Observer: ПЕРЕОПРЕДЕЛЕНО как крит по проценту в названии: {buff_name}")
        elif buff_name and ("20%" in buff_name or "+20%" in buff_name):
            is_critical = False
            buff_value = 100  # Принудительно ставим 100 для обычного
            logging.info(f"📊 Observer: ПЕРЕОПРЕДЕЛЕНО как обычный по проценту в названии: {buff_name}")

        # Логируем что получили
        if original_buff_value != buff_value or original_is_critical != is_critical:
            logging.info(f"🔄 Observer: Исправлены параметры бафа {token_name}: {original_buff_value}->{buff_value}, крит {original_is_critical}->{is_critical}")

        logging.info(f"📥 Observer: Получен баф от {token_name}: значение={buff_value}, крит={is_critical}, название='{buff_name}'")

        # Инициализируем структуру если нужно
        if user_id not in self._buff_results:
            self._buff_results[user_id] = {
                "tokens_info": [],
                "total_value": 0,
                "expected_count": len(self._active_jobs[user_id]["letters"]),
                "completed_count": 0
            }

        # Добавляем результат
        self._buff_results[user_id]["tokens_info"].append({
            "token_name": token_name,
            "buff_value": buff_value,
            "is_critical": is_critical
        })
        self._buff_results[user_id]["total_value"] += buff_value
        self._buff_results[user_id]["completed_count"] += 1

        logging.info(f"📊 Observer: Пользователь {user_id}: выполнено {self._buff_results[user_id]['completed_count']}/{self._buff_results[user_id]['expected_count']} бафов")

        # Проверяем, все ли бафы выполнены
        if self._buff_results[user_id]["completed_count"] >= self._buff_results[user_id]["expected_count"]:
            # Все бафы выполнены - отправляем финальное уведомление
            self._send_final_notification(user_id)

    def _handle_baf_cancel_command(self, from_id: int, cmid: int) -> None:
        """Обработка команды !баф отмена"""
        if from_id not in self._active_jobs:
            self.observer.send_to_peer(
                self.observer.source_peer_id,
                "❌ У вас нет активных бафов для отмены.",
                None
            )
            return

        # Получаем информацию о job
        job_info = self._active_jobs[from_id]
        letters = job_info.get("letters", "")

        # Отменяем бафы в scheduler
        cancelled = self.scheduler.cancel_user_jobs(from_id)

        # Очищаем данные
        if from_id in self._buff_results:
            del self._buff_results[from_id]

        if from_id in self._active_jobs:
            del self._active_jobs[from_id]

        if cancelled:
            self.observer.send_to_peer(
                self.observer.source_peer_id,
                f"✅ Все ваши бафы ({letters}) отменены.",
                None
            )
        else:
            self.observer.send_to_peer(
                self.observer.source_peer_id,
                "⚠️ Не удалось найти ваши бафы в очереди.",
                None
            )

    def _handle_new_message(self, msg_item: Dict[str, Any]) -> None:
        text = (msg_item.get("text") or "").strip()
        from_id = int(msg_item.get("from_id", 0))
        peer_id = int(msg_item.get("peer_id", 0))
        cmid = msg_item.get("conversation_message_id")

        if peer_id != self.observer.source_peer_id:
            return
        if from_id <= 0 or not text:
            return

        # Команда !баф отмена
        if self._is_baf_cancel_cmd(text):
            self._handle_baf_cancel_command(from_id, cmid)
            return

        # Команда !здоровье
        if normalize_text(text) in ["!здоровье", "!health", "!статус"]:
            self._handle_health_command(from_id, text)
            return

        # Команда !диагностика
        if normalize_text(text).startswith("!диагностика"):
            self._handle_diagnostic_command(from_id, text)
            return

        # Команда !голоса
        parsed = self._parse_golosa_cmd(text)
        if parsed is not None:
            name, n = parsed
            token = self.tm.get_token_by_name(name)
            if not token:
                self.observer.send_to_peer(
                    self.observer.source_peer_id,
                    f"❌ Токен с именем '{name}' не найден.",
                    None
                )
                return

            # Проверка прав (если настроен owner_vk_id)
            if token.owner_vk_id == 0:
                token.fetch_owner_id_lazy()

            if token.owner_vk_id != 0 and token.owner_vk_id != from_id:
                logging.warning(
                    f"⚠️ Неавторизованная команда !голоса от {from_id} для токена {token.name} (владелец={token.owner_vk_id})"
                )
                self.observer.send_to_peer(
                    self.observer.source_peer_id,
                    f"❌ У вас нет прав на токен '{name}'.",
                    None
                )
                return

            reply = self._apply_manual_voices_by_name(name, n)
            self.observer.send_to_peer(self.observer.source_peer_id, reply, None)
            return

        # Команда /допраса
        if normalize_text(text).startswith("/допраса"):
            self._handle_doprasa_command(from_id, text, msg_item)
            return

        # Команда !апо
        if self._is_apo_cmd(text):
            status = self._format_apo_status()
            self.observer.send_to_peer(self.observer.source_peer_id, status, None)
            return

        # Команда !баф
        letters = self._parse_baf_letters(text)
        if letters:
            # Проверяем, нет ли уже активных бафов у пользователя
            if from_id in self._active_jobs:
                self.observer.send_to_peer(
                    self.observer.source_peer_id,
                    "❌ У вас уже есть активные бафы. Дождитесь их выполнения или отмените командой '!баф отмена'.",
                    None
                )
                return

            job = Job(sender_id=from_id, trigger_text=text, letters=letters, created_ts=time.time())
            logging.info(f"🎯 !баф from {from_id}: {letters} [observer={self.observer.name}]")

            # Отправляем уведомление о регистрации
            if cmid:
                message_id = self._send_registration_notification(from_id, letters, cmid)
                if message_id > 0:
                    # Сохраняем информацию о job
                    self._active_jobs[from_id] = {
                        "job": job,
                        "letters": letters,
                        "cmid": cmid,
                        "message_id": message_id,
                        "registration_time": time.time()
                    }

                    # Инициализируем структуру для результатов
                    self._buff_results[from_id] = {
                        "tokens_info": [],
                        "total_value": 0,
                        "expected_count": len(letters),
                        "completed_count": 0
                    }

            self.scheduler.enqueue_letters(job, letters)

    def run(self):
        retry_count = 0
        max_retries = 10
        retry_delay = 5

        while True:
            try:
                if not self._lp_get_server():
                    logging.error(f"❌ Не удалось получить LongPoll сервер (попытка {retry_count + 1}/{max_retries})")
                    retry_count += 1
                    if retry_count >= max_retries:
                        logging.critical("💥 Превышено максимальное количество попыток получения LongPoll сервера")
                        break

                    time.sleep(min(retry_delay * retry_count, 300))
                    continue

                retry_count = 0
                logging.info(f"✅ LongPoll готов. Слушаю чат {self.observer.source_peer_id}")

                while True:
                    try:
                        lp = self._lp_check()
                        if not lp:
                            time.sleep(2)
                            continue

                        if "failed" in lp:
                            error_code = lp.get("failed")
                            logging.warning(f"⚠️ LongPoll failed with code: {error_code}")

                            if error_code == 1:  # История устарела или была частично утеряна
                                new_ts = lp.get("ts")
                                if new_ts:
                                    self._lp_ts = str(new_ts)
                                    logging.info(f"🔄 LongPoll: обновлен ts на {new_ts}")
                                continue

                            elif error_code == 2:  # Ключ устарел
                                logging.info("🔄 LongPoll: ключ устарел, обновляю...")
                                if not self._lp_get_server():
                                    time.sleep(5)
                                continue

                            elif error_code == 3:  # Информация устарела
                                logging.info("🔄 LongPoll: информация устарела, обновляю...")
                                if not self._lp_get_server():
                                    time.sleep(5)
                                continue

                            elif error_code == 4:  # Неверная версия протокола
                                logging.error("❌ LongPoll: неверная версия протокола")
                                time.sleep(60)
                                continue

                            else:
                                logging.error(f"❌ LongPoll: неизвестная ошибка {error_code}")
                                time.sleep(5)
                                continue

                        new_ts = lp.get("ts")
                        if new_ts is not None:
                            self._lp_ts = str(new_ts)

                        updates = lp.get("updates", []) or []
                        if not updates:
                            continue

                        msg_ids: List[int] = []
                        for u in updates:
                            if not isinstance(u, list) or not u:
                                continue
                            if int(u[0]) != 4:
                                continue
                            try:
                                msg_id = int(u[1])
                                p_id = int(u[3])
                            except Exception:
                                continue
                            if p_id == self.observer.source_peer_id:
                                msg_ids.append(msg_id)

                        if not msg_ids:
                            continue

                        items = self.observer.get_by_id(msg_ids)
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

                delay = min(retry_delay * (2 ** retry_count), 300)  # Экспоненциальная задержка
                logging.info(f"🔄 Переподключение через {delay} секунд (попытка {retry_count}/{max_retries})")
                time.sleep(delay
