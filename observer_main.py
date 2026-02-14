# -*- coding: utf-8 -*-
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
from .group_handler import GroupProxy
from .observer_longpoll import LongPollWorker
from .observer_processor import MessageProcessor
from .observer_commands import CommandHandler
from .observer_resurrection import ResurrectionHandler
from .observer_triggers import CustomTriggerHandler
from .observer_scheduler import SchedulerCallback

logger = logging.getLogger(__name__)


class ObserverBot:
    """Главный класс Observer - разделён на модули"""
    
    def __init__(self, tm, executor, scheduler, health_monitor):
        self.tm = tm
        self.executor = executor
        self.scheduler = scheduler
        self.health_monitor = health_monitor
        
        # ============= ДВА ТОКЕНА =============
        self._init_tokens()
        
        self.poll_interval = float(self.tm.settings.get("poll_interval", 2.0))
        self.poll_count = int(self.tm.settings.get("poll_count", 20))

        self.state = JobStateStore(storage_path="jobs.json")
        self.state.restore_and_enqueue(self.scheduler)
        
        self.message_queue = queue.Queue()
        self._running = True
        
        self.cmd_handler = CommandHandler(self)
        self.res_handler = ResurrectionHandler(self)
        self.triggers_handler = CustomTriggerHandler(self)
        self.scheduler_callback = SchedulerCallback(self)
        
        self.longpoll = LongPollWorker(self)
        self.processor = MessageProcessor(self)
        
        # Привязываем колбэк к scheduler
        self.scheduler._on_buff_complete = self.scheduler_callback.on_buff_complete
        
        logger.info("🤖 ObserverBot инициализирован")
        logger.info(f"📋 Tokens: {len(self.tm.tokens)}")
        logger.info(f"📌 Source peer ID: {self.source_peer_id}")

    def _init_tokens(self):
        """Инициализация двух токенов (чтение/отправка)"""
        self.reader_token = self.tm.get_token_by_id(self.tm.observer_token_id)
        if not self.reader_token:
            raise RuntimeError(f"❌ Нет токена для чтения: {self.tm.observer_token_id}")
        
        self.sender_token = None
        if hasattr(self.tm, 'group_handler') and self.tm.group_handler:
            source_chat_id = self.tm.settings.get("observer_source_chat_id", 7)
            self.sender_token = GroupProxy(self.tm.group_handler, source_chat_id, self.tm._vk)
            logger.info(f"👥 Отправка через группу: {self.tm.group_handler.name}")
        else:
            self.sender_token = self.reader_token
            logger.warning("⚠️ Нет группового токена, отправка через токен чтения")
        
        self.observer = self.reader_token
        self.is_group = False
        self.source_peer_id = self.reader_token.source_peer_id
        
        logger.info(f"👤 Читаем: {self.reader_token.name}")
        logger.info(f"👥 Отправляем: {self.sender_token.name}")

    def send_to_peer(self, peer_id: int, text: str, forward_msg_id=None, reply_to_cmid=None):
        """Отправка сообщения через групповой токен"""
        target_peer = self.sender_token.source_peer_id
        logger.info(f"📤 Отправка в чат группы {target_peer} (вместо {peer_id})")
        return self.sender_token.send_to_peer(target_peer, text, forward_msg_id, reply_to_cmid)

    def _handle_buff_completion(self, job: Job, buff_info: Dict[str, Any]) -> None:
        """Обработка завершения бафа - оригинальный метод из observer.py"""
        should_finalize, snapshot = self.state.apply_completion(job, buff_info)
        if should_finalize and snapshot:
            txt = build_final_text(job.sender_id, snapshot, self.tm)
            if txt:
                self.send_to_peer(self.source_peer_id, txt)

    def _format_apo_status(self) -> str:
        """Форматирование статуса апостолов"""
        from .constants import RACE_NAMES, RACE_EMOJIS
        
        apostles = [t for t in self.tm.all_buffers() if t.class_type == "apostle"]
        warlocks = [t for t in self.tm.all_buffers() if t.class_type == "warlock"]
        paladins = [t for t in self.tm.all_buffers() if t.class_type in ("crusader", "light_incarnation")]

        lines = []

        if apostles:
            lines.append("👼 Апостолы")
            for t in apostles:
                # Статус токена (включен/выключен)
                status = "✅" if t.enabled else "❌"
                
                # Форматируем расы
                races_str = "/".join(t.races) if t.races else "-"
                
                # Добавляем временные расы
                temp_races = []
                for tr in t.temp_races:
                    remaining = int(tr["expires"] - time.time())
                    if remaining > 0:
                        if remaining >= 3600:
                            hours = remaining // 3600
                            minutes = (remaining % 3600) // 60
                            time_str = f"{hours}ч{minutes:02d}м"
                        else:
                            minutes = remaining // 60
                            seconds = remaining % 60
                            time_str = f"{minutes}м{seconds:02d}с"
                        temp_races.append(f"{tr['race']}({time_str})")
                
                if temp_races:
                    races_str += "/" + "/".join(temp_races)
                
                # Формируем строку без ⚠️
                lines.append(f" {status} {t.name}: {races_str} | 🗣️ {t.voices}")
            lines.append("")

        if warlocks:
            lines.append("🧙 Проклинающие")
            for t in warlocks:
                status = "✅" if t.enabled else "❌"
                lines.append(f" {status} {t.name} | 🗣️ {t.voices}")
            lines.append("")

        if paladins:
            lines.append("⚔️ Паладины")
            for t in paladins:
                status = "✅" if t.enabled else "❌"
                lines.append(f" {status} {t.name} (lvl {t.level}) | 🗣️ {t.voices}")
            lines.append("")

        if not lines:
            return "Нет баферов в конфиге."

        return "\n".join(lines)

    def run(self) -> None:
        """Запуск всех потоков"""
        self.longpoll.start()
        self.processor.start()
        
        logger.info("🚀 Система запущена")
        
        try:
            while self._running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("🛑 Остановка")
            self._running = False
            self.longpoll.stop()
