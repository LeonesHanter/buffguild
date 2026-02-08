# -*- coding: utf-8 -*-
import logging
import time
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
        self.observer = self.tm.get_observer()

        if not self.observer.access_token:
            raise RuntimeError("Observer token has empty access_token")
        if not self.observer.source_peer_id:
            raise RuntimeError("Observer source_chat_id is missing")

        self.poll_interval = float(self.tm.settings.get("poll_interval", 2.0))
        self.poll_count = int(self.tm.settings.get("poll_count", 20))

        # Thread-safe state
        self.state = JobStateStore(storage_path="jobs.json")
        self.state.restore_and_enqueue(self.scheduler)

        logging.info("🤖 MultiTokenBot STARTED (Observer=LongPoll)")
        logging.info(f"📋 Tokens: {len(self.tm.tokens)}")
        logging.info(
            f"🛰️ Target poll: interval={self.poll_interval}s, count={self.poll_count}"
        )

        self._lp_server: str = ""
        self._lp_key: str = ""
        self._lp_ts: str = ""

        # ... тут пропущен код _lp_get_server, _lp_check, _restore_active_jobs и т.п. ...

    # -------------------- Commands --------------------

    def _handle_health_command(self, from_id: int, text: str) -> None:
        report = self.health_monitor.get_detailed_report()
        if len(report) > 4000:
            report = report[:4000] + "\n... (сообщение обрезано)"
        self.observer.send_to_peer(self.observer.source_peer_id, report, None)

    def _handle_diagnostic_command(self, from_id: int, text: str) -> None:
        parts = (text or "").split()
        if len(parts) == 1:
            self.observer.send_to_peer(
                self.observer.source_peer_id,
                "❌ Укажите имя токена: !диагностика [имя_токена]",
                None,
            )
            return

        token_name = parts[1].strip()
        report = self.health_monitor.get_detailed_report(token_name)
        self.observer.send_to_peer(self.observer.source_peer_id, report, None)

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
        paladins = [
            t
            for t in self.tm.all_buffers()
            if t.class_type in ("crusader", "light_incarnation")
        ]

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

    def _handle_doprasa_command(
        self,
        from_id: int,
        text: str,
        msg_item: Dict[str, Any],
    ) -> None:
        parsed = parse_doprasa_cmd(text, msg_item)
        if not parsed:
            self.observer.send_to_peer(
                self.observer.source_peer_id,
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
                self.observer.send_to_peer(
                    self.observer.source_peer_id,
                    f"❌ Токен '{token_name}' не найден.",
                    None,
                )
                return
            if token.owner_vk_id == 0:
                token.fetch_owner_id_lazy()
            if token.owner_vk_id != 0 and token.owner_vk_id != from_id:
                self.observer.send_to_peer(
                    self.observer.source_peer_id,
                    f"❌ Нет прав на '{token_name}'.",
                    None,
                )
                return
        else:
            token = self.tm.get_token_by_sender_id(from_id)
            if not token:
                self.observer.send_to_peer(
                    self.observer.source_peer_id,
                    f"❌ Апостол с вашим ID ({from_id}) не найден.",
                    None,
                )
                return

        if token.id == self.observer.id:
            self.observer.send_to_peer(
                self.observer.source_peer_id,
                "❌ Observer токен не является апостолом и не может получать расы.",
                None,
            )
            return

        if token.class_type != "apostle":
            self.observer.send_to_peer(
                self.observer.source_peer_id,
                f"❌ {token.name} не апостол.",
                None,
            )
            return

        token._cleanup_expired_temp_races(force=True)

        if race_key in token.races:
            self.observer.send_to_peer(
                self.observer.source_peer_id,
                f"⚠️ У {token.name} уже есть постоянная раса.",
                None,
            )
            return

        if any(tr["race"] == race_key for tr in token.temp_races):
            self.observer.send_to_peer(
                self.observer.source_peer_id,
                f"⚠️ У {token.name} уже есть эта временная раса.",
                None,
            )
            return

        if token.get_temp_race_count() >= 1:
            self.observer.send_to_peer(
                self.observer.source_peer_id,
                f"⚠️ У {token.name} уже есть временная раса (можно только одну).",
                None,
            )
            return

        if not original_timestamp:
            self.observer.send_to_peer(
                self.observer.source_peer_id,
                "❌ Нужно переслать сообщение с успешным бафом.\n"
                "📌 Ответьте на сообщение с бафом или перешлите его.",
                None,
            )
            return

        start_moscow = timestamp_to_moscow(original_timestamp)
        end_moscow = timestamp_to_moscow(original_timestamp + 2 * 3600)

        if end_moscow < now_moscow():
            self.observer.send_to_peer(
                self.observer.source_peer_id,
                f"❌ Время бафа уже истекло (сообщение от {format_moscow_time(start_moscow)}).",
                None,
            )
            return

        success = token.add_temporary_race(
            race_key, expires_at=original_timestamp + 2 * 3600
        )
        if success:
            self.tm.update_race_index(token)
            self.observer.send_to_peer(
                self.observer.source_peer_id,
                f"✅ {token.name}: добавлена временная раса "
                f"'{RACE_NAMES.get(race_key, race_key)}'\n"
                f"⏰ {format_moscow_time(start_moscow)} → {format_moscow_time(end_moscow)}\n"
                f"📌 Теперь можно использовать !баф{race_key}",
                None,
            )
        else:
            self.observer.send_to_peer(
                self.observer.source_peer_id,
                f"❌ Не удалось добавить временную расу для {token.name}.",
                None,
            )

    # -------------------- Scheduler callback --------------------

    def _handle_buff_completion(self, job: Job, buff_info: Dict[str, Any]) -> None:
        should_finalize, snapshot = self.state.apply_completion(job, buff_info)
        if should_finalize and snapshot:
            txt = build_final_text(job.sender_id, snapshot, self.tm)
            if txt:
                sent_ok, send_status = self.observer.send_to_peer(
                    self.observer.source_peer_id, txt
                )
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

        if peer_id != self.observer.source_peer_id:
            return
        if from_id <= 0 or not text:
            return

        norm = normalize_text(text)

        if is_baf_cancel_cmd(norm):
            had_job, letters = self.state.cancel_and_clear(from_id)
            if not had_job:
                self.observer.send_to_peer(
                    self.observer.source_peer_id,
                    "❌ У вас нет активных бафов для отмены.",
                    None,
                )
                return
            cancelled = self.scheduler.cancel_user_jobs(from_id)
            self.observer.send_to_peer(
                self.observer.source_peer_id,
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

        # ---- !голоса N: только по отправителю ----
        parsed_g = parse_golosa_cmd(text)
        if parsed_g is not None:
            _, n = parsed_g

            token = self.tm.get_token_by_sender_id(from_id)
            if not token:
                self.observer.send_to_peer(
                    self.observer.source_peer_id,
                    f"❌ Для вашего VK ID ({from_id}) не найден токен.",
                    None,
                )
                return

            reply = self._apply_manual_voices_by_name(token.name, n)
            self.observer.send_to_peer(self.observer.source_peer_id, reply, None)
            return

        if norm.startswith("/допраса"):
            self._handle_doprasa_command(from_id, text, msg_item)
            return

        if is_apo_cmd(norm):
            status = self._format_apo_status()
            self.observer.send_to_peer(self.observer.source_peer_id, status, None)
            return

        letters = parse_baf_letters(text)
        if letters:
            if self.state.has_active(from_id):
                self.observer.send_to_peer(
                    self.observer.source_peer_id,
                    "❌ У вас уже есть активные бафы. Дождитесь их выполнения или отмените командой '!баф отмена'.",
                    None,
                )
                return

            job = Job(
                sender_id=from_id,
                trigger_text=text,
                letters=letters,
                created_ts=time.time(),
            )
            self.state.register_job(from_id, job, letters, cmid)

            if cmid:
                sent_ok, send_status = self.observer.send_to_peer(
                    self.observer.source_peer_id,
                    build_registration_text(letters),
                )
                if sent_ok and "OK:" in (send_status or ""):
                    try:
                        mid = int(send_status.split(":")[1])
                        self.state.update_message_id(from_id, mid)
                    except Exception:
                        pass

            self.scheduler.enqueue_letters(job, letters)
