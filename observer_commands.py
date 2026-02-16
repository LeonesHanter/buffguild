# -*- coding: utf-8 -*-
import logging
import time
from typing import Optional

from .utils import normalize_text
from .commands import (
    parse_baf_letters, parse_golosa_cmd, parse_doprasa_cmd,
    is_apo_cmd, is_baf_cancel_cmd
)
from .notifications import build_registration_text
from .models import Job
from .constants import RACE_NAMES

logger = logging.getLogger(__name__)


class CommandHandler:
    def __init__(self, bot):
        self.bot = bot

    def handle(self, text: str, from_id: int, msg: dict) -> bool:
        norm = normalize_text(text)

        if is_baf_cancel_cmd(norm):
            return self._cancel(from_id)

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
        had, letters = self.bot.state.cancel_and_clear(from_id)
        if not had:
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                "❌ У вас нет активных бафов"
            )
            return True
        cancelled = self.bot.scheduler.cancel_user_jobs(from_id)
        msg = (
            f"✅ Ваши бафы ({letters}) отменены."
            if cancelled
            else "⚠️ Не удалось найти бафы"
        )
        self.bot.send_to_peer(self.bot.source_peer_id, msg)
        return True

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
                f"🤖 Тип: пользователь (гибридный)",
                f"📡 LongPoll (user): "
                f"{'✅' if self.bot.user_longpoll._ready else '❌'}",
                f"📨 Очередь user: "
                f"{self.bot.user_message_queue.qsize()}",
                "",
                "Используй /диагностика [токен]"
            ]
            self.bot.send_to_peer(
                self.bot.source_peer_id, "\n".join(report)
            )
            return

        token_name = parts[1].strip()
        report = self.bot.health_monitor.get_detailed_report(
            token_name
        )
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

        if token.owner_vk_id != from_id:
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                f"❌ Нет прав на токен '{name}'"
            )
            return

        new_state = (action == "вкл")
        if token.enabled == new_state:
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                f"ℹ️ {token.name} уже "
                f"{'включен' if new_state else 'выключен'}"
            )
            return

        token.enabled = new_state
        token.mark_for_save()
        self.bot.tm.mark_for_save()
        self.bot.send_to_peer(
            self.bot.source_peer_id,
            f"✅ {token.name}: "
            f"{'включен' if new_state else 'выключен'}"
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

        if token.owner_vk_id != from_id:
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

        human = "/".join(
            RACE_NAMES.get(r, r) for r in race_keys
        )
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
                "❌ Использование: /допраса [раса] "
                "[имя_токена]\n"
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
            if token.owner_vk_id != from_id:
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
                    f"⚠️ У {token.name} уже есть "
                    f"эта временная раса"
                )
                return

        if len(token.temp_races) >= 1:
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                f"⚠️ У {token.name} уже есть "
                f"временная раса"
            )
            return

        if not original_timestamp:
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                "❌ Нужно переслать сообщение с бафом"
            )
            return

        start_moscow = timestamp_to_moscow(original_timestamp)
        end_moscow = timestamp_to_moscow(
            original_timestamp + 2 * 3600
        )

        if end_moscow < now_moscow():
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                f"❌ Время бафа истекло "
                f"({format_moscow_time(start_moscow)})"
            )
            return

        success = token.add_temporary_race(
            race_key, expires_at=original_timestamp + 2 * 3600
        )
        if success:
            self.bot.tm.update_race_index(token)
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                f"✅ {token.name}: временная раса "
                f"'{RACE_NAMES.get(race_key, race_key)}'\n"
                f"⏰ {format_moscow_time(start_moscow)} → "
                f"{format_moscow_time(end_moscow)}\n"
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
        logger.info(
            f"🔍 _baf: from_id={from_id}, letters={letters}, "
            f"user_cmid={user_cmid}, msg_id={msg_id}"
        )

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

        info = self.bot.state.register_job(
            from_id, job, letters, user_cmid
        )

        registration_text = build_registration_text(letters)
        logger.info(
            f"📝 Отправляем регистрацию "
            f"для user_id={from_id}"
        )

        # Отправка БЕЗ reply (чаты разные)
        success, result = self.bot.send_to_peer(
            self.bot.source_peer_id,
            registration_text
        )

        if success and result and isinstance(result, dict):
            message_id = result.get('message_id', 0)
            cmid = result.get('cmid', 0)

            # Для группового токена message_id=cmid
            # (effective_id уже подставлен в send_message)
            effective_id = (
                message_id if message_id > 0 else cmid
            )

            if effective_id and effective_id > 0:
                self.bot.state.update_message_id(
                    from_id, effective_id
                )
                job.registration_msg_id = effective_id

                if cmid:
                    self.bot.message_cmids[effective_id] = cmid

                logger.info(
                    f"✅ registration_msg_id={effective_id}, "
                    f"cmid={cmid} для user_id={from_id}"
                )
            else:
                logger.error(f"❌ Нет ID! result={result}")
        else:
            logger.error(
                f"❌ Отправка не удалась: "
                f"success={success}, result={result}"
            )

        self.bot.scheduler.enqueue_letters(job, letters)
