# -*- coding: utf-8 -*-
import logging
import threading
import queue
import time

logger = logging.getLogger(__name__)


class MessageProcessor:
    """Поток для обработки сообщений из очереди"""
    
    def __init__(self, bot):
        self.bot = bot
        self._thread = None
        self._running = False

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._worker, daemon=True)
        self._thread.start()
        logger.info("📨 Processor запущен")

    def stop(self):
        self._running = False

    def _worker(self):
        while self._running:
            try:
                msg = self.bot.message_queue.get(timeout=1)
                self._process(msg)
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"❌ {e}")

    def _process(self, msg: dict):
        from_id = msg.get("from_id", 0)
        
        if from_id < 0:
            if self.bot.triggers_handler.handle_game_response(msg):
                return
        
        text = msg.get("text", "").strip()
        if text.startswith("/воскрешение"):
            self.bot.res_handler.handle(text, from_id)
            return
        
        if self.bot.triggers_handler.handle_command(text, from_id):
            return
        
        self.bot.cmd_handler.handle(text, from_id, msg)
