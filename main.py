# -*- coding: utf-8 -*-
import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(BASE_DIR)
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

import json
import logging
import threading
import time

from buffguild.logging_setup import setup_logging
from buffguild.vk_client import ResilientVKClient
from buffguild.token_manager import OptimizedTokenManager
from buffguild.executor import AbilityExecutor
from buffguild.observer import ObserverBot
from buffguild.telegram_admin import TelegramAdmin

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

    tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    admins = os.getenv("ADMIN_USER_IDS", "").strip()

    if tg_token and admins:
        admin_ids = [int(x.strip()) for x in admins.split(",") if x.strip().isdigit()]
        telegram_admin = TelegramAdmin(
            telegram_token=tg_token,
            admin_ids=admin_ids,
            config_path=CONFIG_PATH,
            bot_instance=observer_bot,
        )
        logging.info("📱 Telegram admin bot запущен")
        telegram_admin.run()
    else:
        logging.warning("⚠️ Telegram не настроен — работает только VK бот")
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
