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

        # Отмена бафов
        if is_baf_cancel_cmd(norm):
            return self._cancel(from_id)

        # Здоровье/статус
        if norm in ["/здоровье", "/health", "/статус"]:
            self._health(from_id)
            return True

        # Диагностика
        if norm.startswith("/диагностика"):
            self._diag(text, from_id)
            return True

        # Апо вкл/выкл
        if norm.startswith("/апо "):
            self._apo_toggle(text, from_id)
            return True

        # Смена рас
        if norm.startswith("/сменарасы"):
            self._change_races(text, from_id)
            return True

        # Голоса
        pg = parse_golosa_cmd(text)
        if pg:
            self._voices(from_id, pg[1])
            return True

        # Допраса
        if norm.startswith("/допраса"):
            self._doprasa(text, from_id, msg)
            return True

        # Статус апостолов
        if is_apo_cmd(norm):
            self._apo_status(from_id)
            return True

        # Баф
        letters = parse_baf_letters(text)
        if letters:
            self._baf(letters, from_id, text, msg.get("conversation_message_id"))
            return True

        return False

    def _cancel(self, from_id: int) -> bool:
        """Отмена активных бафов"""
        had, letters = self.bot.state.cancel_and_clear(from_id)
        if not had:
            self.bot.send_to_peer(self.bot.source_peer_id, "❌ У вас нет активных бафов")
            return True
        cancelled = self.bot.scheduler.cancel_user_jobs(from_id)
        msg = f"✅ Ваши бафы ({letters}) отменены." if cancelled else "⚠️ Не удалось найти бафы"
        self.bot.send_to_peer(self.bot.source_peer_id, msg)
        return True

    def _health(self, from_id: int):
        """Команда /здоровье"""
        report = self.bot.health_monitor.get_detailed_report()
        if len(report) > 4000:
            report = report[:4000] + "\n..."
        self.bot.send_to_peer(self.bot.source_peer_id, report)

    def _diag(self, text: str, from_id: int):
        """Команда /диагностика"""
        parts = text.split()
        if len(parts) == 1:
            # Общая диагностика
            report = [
                "📊 **ДИАГНОСТИКА**",
                f"🕒 Время: {time.strftime('%H:%M:%S')}",
                f"🤖 Тип: пользователь (гибридный)",
                f"📡 LongPoll: {'✅' if self.bot.longpoll._ready else '❌'}",
                f"📨 Очередь: {self.bot.message_queue.qsize()}",
                "",
                "Используй /диагностика [токен] для детальной информации"
            ]
            self.bot.send_to_peer(self.bot.source_peer_id, "\n".join(report))
            return
        
        token_name = parts[1].strip()
        report = self.bot.health_monitor.get_detailed_report(token_name)
        self.bot.send_to_peer(self.bot.source_peer_id, report)

    def _apo_toggle(self, text: str, from_id: int):
        """Команда /апо вкл|выкл ИмяТокена"""
        parts = text.strip().split()
        if len(parts) < 3:
            self.bot.send_to_peer(self.bot.source_peer_id, "❌ Использование: /апо вкл|выкл ИмяТокена")
            return
        
        action = parts[1].lower()
        name = " ".join(parts[2:]).strip()
        
        if action not in ("вкл", "выкл"):
            self.bot.send_to_peer(self.bot.source_peer_id, "❌ Второй аргумент должен быть 'вкл' или 'выкл'")
            return
        
        token = self.bot.tm.get_token_by_name(name)
        if not token:
            self.bot.send_to_peer(self.bot.source_peer_id, f"❌ Токен '{name}' не найден")
            return
        
        if token.owner_vk_id == 0:
            token.fetch_owner_id_lazy()
        
        if token.owner_vk_id != from_id:
            self.bot.send_to_peer(self.bot.source_peer_id, f"❌ Нет прав на токен '{name}'")
            return
        
        new_state = (action == "вкл")
        if token.enabled == new_state:
            self.bot.send_to_peer(self.bot.source_peer_id, f"ℹ️ {token.name} уже {'включен' if new_state else 'выключен'}")
            return
        
        token.enabled = new_state
        token.mark_for_save()
        self.bot.tm.mark_for_save()
        self.bot.send_to_peer(self.bot.source_peer_id, f"✅ {token.name}: {'включен' if new_state else 'выключен'}")

    def _change_races(self, text: str, from_id: int):
        """Команда /сменарасы ИмяТокена ч,н"""
        parts = text.strip().split(maxsplit=2)
        if len(parts) < 3:
            self.bot.send_to_peer(self.bot.source_peer_id, "❌ Использование: /сменарасы ИмяТокена ч,н")
            return
        
        name = parts[1].strip()
        races_str = parts[2].replace(" ", "").replace(";", ",")
        race_keys_raw = [r for r in races_str.split(",") if r]
        
        if not race_keys_raw:
            self.bot.send_to_peer(self.bot.source_peer_id, "❌ Не указаны новые расы")
            return
        
        seen = set()
        race_keys = []
        for rk in race_keys_raw:
            if rk in seen:
                self.bot.send_to_peer(self.bot.source_peer_id, f"❌ Нельзя указывать одну расу несколько раз ('{rk}')")
                return
            seen.add(rk)
            race_keys.append(rk)
        
        for rk in race_keys:
            if rk not in RACE_NAMES:
                self.bot.send_to_peer(self.bot.source_peer_id, f"❌ Неизвестная раса '{rk}'")
                return
        
        token = self.bot.tm.get_token_by_name(name)
        if not token:
            self.bot.send_to_peer(self.bot.source_peer_id, f"❌ Токен '{name}' не найден")
            return
        
        if token.owner_vk_id == 0:
            token.fetch_owner_id_lazy()
        
        if token.owner_vk_id != from_id:
            self.bot.send_to_peer(self.bot.source_peer_id, f"❌ Нет прав на токен '{name}'")
            return
        
        if token.class_type != "apostle":
            self.bot.send_to_peer(self.bot.source_peer_id, f"❌ {token.name} не апостол")
            return
        
        token.races = race_keys
        token.temp_races = []
        token.mark_for_save()
        self.bot.tm.update_race_index(token)
        self.bot.tm.mark_for_save()
        
        human = "/".join(RACE_NAMES.get(r, r) for r in race_keys)
        self.bot.send_to_peer(self.bot.source_peer_id, f"✅ {token.name}: основные расы изменены на {human}")

    def _voices(self, from_id: int, voices: int):
        """Команда /голоса N"""
        token = self.bot.tm.get_token_by_sender_id(from_id)
        if not token:
            self.bot.send_to_peer(self.bot.source_peer_id, f"❌ Апостол с ID {from_id} не найден")
            return
        
        token.update_voices_manual(voices)
        self.bot.send_to_peer(self.bot.source_peer_id, f"✅ {token.name}: голоса выставлены = {voices}")

    def _doprasa(self, text: str, from_id: int, msg: dict):
        """Команда /допраса [раса] [имя_токена_опционально]"""
        from .commands import parse_doprasa_cmd
        from .utils import timestamp_to_moscow, now_moscow, format_moscow_time
        
        parsed = parse_doprasa_cmd(text, msg)
        if not parsed:
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                "❌ Использование: /допраса [раса] [имя_токена_опционально]\n"
                "📌 Нужно переслать сообщение с успешным бафом"
            )
            return
        
        race_key, token_name, original_timestamp, _ = parsed
        
        token = None
        if token_name:
            token = self.bot.tm.get_token_by_name(token_name)
            if not token:
                self.bot.send_to_peer(self.bot.source_peer_id, f"❌ Токен '{token_name}' не найден")
                return
            if token.owner_vk_id == 0:
                token.fetch_owner_id_lazy()
            if token.owner_vk_id != from_id:
                self.bot.send_to_peer(self.bot.source_peer_id, f"❌ Нет прав на '{token_name}'")
                return
        else:
            token = self.bot.tm.get_token_by_sender_id(from_id)
            if not token:
                self.bot.send_to_peer(self.bot.source_peer_id, f"❌ Апостол с вашим ID ({from_id}) не найден")
                return
        
        obs_token = self.bot.tm.get_observer_token_object()
        if obs_token and token.id == obs_token.id:
            self.bot.send_to_peer(self.bot.source_peer_id, "❌ Observer токен не может получать расы")
            return
        
        if token.class_type != "apostle":
            self.bot.send_to_peer(self.bot.source_peer_id, f"❌ {token.name} не апостол")
            return
        
        token._cleanup_expired_temp_races(force=True)
        
        if race_key in token.races:
            self.bot.send_to_peer(self.bot.source_peer_id, f"⚠️ У {token.name} уже есть постоянная раса")
            return
        
        for tr in token.temp_races:
            if tr["race"] == race_key:
                self.bot.send_to_peer(self.bot.source_peer_id, f"⚠️ У {token.name} уже есть эта временная раса")
                return
        
        if len(token.temp_races) >= 1:
            self.bot.send_to_peer(self.bot.source_peer_id, f"⚠️ У {token.name} уже есть временная раса (можно только одну)")
            return
        
        if not original_timestamp:
            self.bot.send_to_peer(self.bot.source_peer_id, "❌ Нужно переслать сообщение с успешным бафом")
            return
        
        start_moscow = timestamp_to_moscow(original_timestamp)
        end_moscow = timestamp_to_moscow(original_timestamp + 2 * 3600)
        
        if end_moscow < now_moscow():
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                f"❌ Время бафа истекло (сообщение от {format_moscow_time(start_moscow)})"
            )
            return
        
        success = token.add_temporary_race(race_key, expires_at=original_timestamp + 2 * 3600)
        if success:
            self.bot.tm.update_race_index(token)
            self.bot.send_to_peer(
                self.bot.source_peer_id,
                f"✅ {token.name}: добавлена временная раса '{RACE_NAMES.get(race_key, race_key)}'\n"
                f"⏰ {format_moscow_time(start_moscow)} → {format_moscow_time(end_moscow)}\n"
                f"📌 Теперь можно использовать !баф{race_key}"
            )
        else:
            self.bot.send_to_peer(self.bot.source_peer_id, f"❌ Не удалось добавить временную расу")

    def _apo_status(self, from_id: int):
        """Команда /апо - статус апостолов"""
        status = self.bot._format_apo_status()
        self.bot.send_to_peer(self.bot.source_peer_id, status)

    def _baf(self, letters: str, from_id: int, text: str, cmid: Optional[int]):
        """Команда /баф ..."""
        if self.bot.state.has_active(from_id):
            self.bot.send_to_peer(self.bot.source_peer_id, "❌ У вас уже есть активные бафы")
            return
        
        job = Job(sender_id=from_id, trigger_text=text, letters=letters, created_ts=time.time())
        self.bot.state.register_job(from_id, job, letters, cmid)
        
        if cmid:
            self.bot.send_to_peer(self.bot.source_peer_id, build_registration_text(letters))
        
        self.bot.scheduler.enqueue_letters(job, letters)
