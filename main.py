# -*- coding: utf-8 -*-
import os
import sys
import json
import logging
import threading
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(BASE_DIR)
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

from buffguild.logging_setup import setup_logging
from buffguild.vk_client import ResilientVKClient
from buffguild.token_manager import OptimizedTokenManager
from buffguild.executor import AbilityExecutor
from buffguild.observer import ObserverBot

CONFIG_PATH = os.path.join(BASE_DIR, "config.json")


def main() -> None:
    setup_logging()
    logging.info("🚀 Запуск VK Buff Guild Bot...")

    vk = ResilientVKClient()
    tm = OptimizedTokenManager(CONFIG_PATH, vk)
    executor = AbilityExecutor(tm)
    observer_bot = ObserverBot(tm, executor)

    bot_thread = threading.Thread(target=observer_bot.run, daemon=True)
    bot_thread.start()
    logging.info("🤖 VK бот запущен")

    # Телеграм‑админка запускается отдельным сервисом (telegram-bot.service)
    logging.info("📱 Telegram admin bot запускается отдельным сервисом telegram-bot.service")

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        logging.info("🛑 Остановка по Ctrl+C")


if __name__ == "__main__":
    try:
        main()
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"❌ Ошибка config.json: {e}")
        raise
    except Exception:
        logging.critical("💥 Критическая ошибка при запуске!", exc_info=True)
        raise
