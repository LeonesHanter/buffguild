# -*- coding: utf-8 -*-
"""
ObserverBot - главный класс наблюдателя
"""
import logging
import threading
import queue
import time
from typing import Any, Dict, List, Optional, Tuple

from .constants import VK_API_VERSION
from .state_store import JobStateStore
from .models import Job
from .notifications import build_final_text
from .custom_triggers import custom_storage
from .group_handler import GroupHandler, GroupProxy
from .observer_longpoll import LongPollWorker
from .observer_processor import MessageProcessor
from .observer_commands import CommandHandler
from .observer_resurrection import ResurrectionHandler
from .observer_triggers import CustomTriggerHandler
from .observer_scheduler import SchedulerCallback

logger = logging.getLogger(__name__)


class ObserverBot:
    """Главный класс Observer"""

    def __init__(self, tm, executor, scheduler, health_monitor):
        self.tm = tm
        self.executor = executor
        self.scheduler = scheduler
        self.health_monitor = health_monitor

        self._init_tokens()

        self.poll_interval = float(
            self.tm.settings.get("poll_interval", 2.0)
        )
        self.poll_count = int(
            self.tm.settings.get("poll_count", 20)
        )

        self.state = JobStateStore(storage_path="jobs.json")
        self.state.restore_and_enqueue(self.scheduler)

        # Очереди для сообщений
        self.user_message_queue = queue.Queue()
        self.group_message_queue = queue.Queue()
        # Алиас для LongPollWorker
        self.message_queue = self.user_message_queue

        self._running = True

        # Обработчики
        self.cmd_handler = CommandHandler(self)
        self.res_handler = ResurrectionHandler(self)
        self.triggers_handler = CustomTriggerHandler(self)
        self.scheduler_callback = SchedulerCallback(self)

        # LongPoll + Processor
        self.user_longpoll = LongPollWorker(self)
        self.user_processor = MessageProcessor(
            self, queue_type='user'
        )

        # Кэш cmid для редактирования
        # Ключ: effective_id (cmid), Значение: cmid
        self.message_cmids = {}

        # Колбэк scheduler
        self.scheduler._on_buff_complete = (
            self.scheduler_callback.on_buff_complete
        )

        logger.info("🤖 ObserverBot инициализирован")
        logger.info(f"📋 Tokens: {len(self.tm.tokens)}")
        logger.info(
            f"📌 Читаем команды: чат {self.source_peer_id}"
        )
        if self._is_group_sender:
            logger.info(
                f"📌 Группа пишет: чат {self.group_peer_id}"
            )

    def _init_tokens(self):
        """Инициализация двух токенов"""
        self.reader_token = self.tm.get_token_by_id(
            self.tm.observer_token_id
        )
        if not self.reader_token:
            raise RuntimeError(
                f"❌ Нет токена: {self.tm.observer_token_id}"
            )

        self._is_group_sender = False
        self.group_peer_id = 0

        # Отладка
        has_gh = hasattr(self.tm, 'group_handler')
        gh = getattr(self.tm, 'group_handler', None)
        logger.info(
            f"🔧 has group_handler: {has_gh}, "
            f"value: {gh}"
        )

        if has_gh and gh is not None:
            source_chat_id = self.tm.settings.get(
                "observer_source_chat_id", 7
            )
            self.sender_token = GroupProxy(
                gh, source_chat_id, self.tm._vk
            )
            self._is_group_sender = True
            self.group_peer_id = self.sender_token.source_peer_id
            logger.info(
                f"👥 Отправка через группу: {gh.name} "
                f"→ чат {source_chat_id} "
                f"(peer: {self.group_peer_id})"
            )
        else:
            self.sender_token = self.reader_token
            logger.warning("⚠️ Нет группового токена!")

        self.observer = self.reader_token
        self.is_group = False
        self.source_peer_id = self.reader_token.source_peer_id

        logger.info(f"👤 Читаем: {self.reader_token.name}")
        logger.info(f"👥 Пишем: {self.sender_token.name}")

    # ──────────────────────────────────────────────
    #  peer_id для отправки
    # ──────────────────────────────────────────────
    def _get_send_peer_id(self) -> int:
        """Куда отправлять"""
        if self._is_group_sender:
            return self.group_peer_id
        return self.source_peer_id

    # ──────────────────────────────────────────────
    #  Отправка сообщения
    # ──────────────────────────────────────────────
    def send_to_peer(
        self,
        peer_id: int,
        text: str,
        forward_msg_id: Optional[int] = None,
        reply_to_cmid: Optional[int] = None
    ) -> Tuple[bool, Optional[Dict]]:
        """
        Отправка сообщения.
        Если sender — группа, peer_id заменяется на чат группы.
        """
        send_peer = self._get_send_peer_id()

        # Reply между разными чатами невозможен
        safe_reply = None
        if reply_to_cmid:
            if send_peer == peer_id:
                safe_reply = reply_to_cmid
            else:
                logger.warning(
                    f"⚠️ reply пропущен: разные чаты "
                    f"({peer_id} vs {send_peer})"
                )

        logger.info(
            f"📤 send_to_peer: target={send_peer} "
            f"(запрошено={peer_id}, reply={safe_reply})"
        )

        if isinstance(self.sender_token, GroupProxy):
            success, result = self.sender_token.send_to_peer(
                send_peer, text, forward_msg_id, safe_reply
            )
            if success and result and isinstance(result, dict):
                msg_id = result.get('message_id', 0)
                cmid = result.get('cmid', 0)
                is_cmid = result.get('is_cmid', False)

                # Сохраняем cmid для редактирования
                if msg_id and cmid:
                    self.message_cmids[msg_id] = cmid
                    logger.info(
                        f"💾 cmid={cmid} для "
                        f"effective_id={msg_id} "
                        f"(is_cmid={is_cmid})"
                    )

            return success, result
        else:
            ok, status = self.sender_token.send_to_peer(
                send_peer, text, forward_msg_id, safe_reply
            )
            if ok:
                return True, {
                    'message_id': 0,
                    'cmid': 0,
                    'peer_id': send_peer,
                    'is_cmid': False
                }
            return False, None

    # ──────────────────────────────────────────────
    #  Редактирование сообщения
    # ──────────────────────────────────────────────
    def edit_message(
        self,
        peer_id: int,
        message_id: int,
        text: str
    ) -> Tuple[bool, str]:
        """
        Редактирование сообщения.
        Для группового токена message_id может быть cmid
        (т.к. VK не возвращает настоящий message_id).
        """
        edit_peer = self._get_send_peer_id()

        # message_id может быть cmid (is_cmid=True)
        cached_cmid = self.message_cmids.get(message_id, 0)

        # Определяем что передать в edit
        # Если cached_cmid есть — используем его
        # Если message_id == cmid (is_cmid), передаём как cmid
        cmid = cached_cmid if cached_cmid else message_id
        real_msg_id = (
            0 if (cached_cmid or message_id == cmid)
            else message_id
        )

        logger.info(
            f"✏️ edit: peer={edit_peer}, "
            f"original_id={message_id}, "
            f"cmid={cmid}, real_msg_id={real_msg_id}"
        )

        if isinstance(self.sender_token, GroupProxy):
            return self.sender_token.edit_message(
                peer_id=edit_peer,
                message_id=real_msg_id,
                text=text,
                cmid=cmid
            )
        elif hasattr(self.sender_token, 'edit_message'):
            return self.sender_token.edit_message(
                edit_peer, message_id, text
            )
        else:
            return False, "METHOD_NOT_FOUND"

    # ──────────────────────────────────────────────
    #  Завершение бафа
    # ──────────────────────────────────────────────
    def _handle_buff_completion(
        self, job: Job, buff_info: Dict[str, Any]
    ) -> None:
        should_finalize, snapshot = self.state.apply_completion(
            job, buff_info
        )
        if should_finalize and snapshot:
            txt = build_final_text(
                job.sender_id, snapshot, self.tm
            )
            if txt:
                self.send_to_peer(self.source_peer_id, txt)

    # ──────────────────────────────────────────────
    #  Статус апостолов
    # ──────────────────────────────────────────────
    def _format_apo_status(self) -> str:
        from .constants import RACE_NAMES, RACE_EMOJIS

        apostles = [
            t for t in self.tm.all_buffers()
            if t.class_type == "apostle"
        ]
        warlocks = [
            t for t in self.tm.all_buffers()
            if t.class_type == "warlock"
        ]
        paladins = [
            t for t in self.tm.all_buffers()
            if t.class_type in ("crusader", "light_incarnation")
        ]

        lines = []

        if apostles:
            lines.append("👼 Апостолы")
            for t in apostles:
                status = "✅" if t.enabled else "❌"
                races_str = (
                    "/".join(t.races) if t.races else "-"
                )
                temp_races = []
                for tr in t.temp_races:
                    remaining = int(tr["expires"] - time.time())
                    if remaining > 0:
                        if remaining >= 3600:
                            h = remaining // 3600
                            m = (remaining % 3600) // 60
                            time_str = f"{h}ч{m:02d}м"
                        else:
                            m = remaining // 60
                            s = remaining % 60
                            time_str = f"{m}м{s:02d}с"
                        temp_races.append(
                            f"{tr['race']}({time_str})"
                        )
                if temp_races:
                    races_str += "/" + "/".join(temp_races)
                lines.append(
                    f" {status} {t.name}: {races_str} "
                    f"| 🗣️ {t.voices}"
                )
            lines.append("")

        if warlocks:
            lines.append("🧙 Проклинающие")
            for t in warlocks:
                status = "✅" if t.enabled else "❌"
                lines.append(
                    f" {status} {t.name} | 🗣️ {t.voices}"
                )
            lines.append("")

        if paladins:
            lines.append("⚔️ Паладины")
            for t in paladins:
                status = "✅" if t.enabled else "❌"
                lines.append(
                    f" {status} {t.name} (lvl {t.level}) "
                    f"| 🗣️ {t.voices}"
                )
            lines.append("")

        if not lines:
            return "Нет баферов в конфиге."
        return "\n".join(lines)

    # ──────────────────────────────────────────────
    #  Запуск
    # ──────────────────────────────────────────────
    def run(self) -> None:
        self.user_longpoll.start()
        self.user_processor.start()

        logger.info("🚀 Система запущена")
        logger.info(
            f"📝 Чтение команд: чат {self.source_peer_id}"
        )
        logger.info(
            f"📝 Отправка ответов: "
            f"чат {self._get_send_peer_id()}"
        )

        try:
            while self._running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("🛑 Остановка")
            self._running = False
            self.user_longpoll.stop()
