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
from buffguild.profile_manager import ProfileManager  # <-- ВАЖНО: этот импорт должен быть!

CONFIG_PATH = os.path.join(BASE_DIR, "config.json")


def main() -> None:
    setup_logging()
    logging.info("🚀 Запуск VK Buff Guild Bot...")

    vk = ResilientVKClient()
    tm = OptimizedTokenManager(CONFIG_PATH, vk)
    executor = AbilityExecutor(tm)
    observer_bot = ObserverBot(tm, executor)

    # ЗАПУСК МЕНЕДЖЕРА С ЧЕРЕДОВАНИЕМ - ЭТО ДОЛЖНО БЫТЬ В ЛОГАХ!
    profile_manager = ProfileManager(tm)
    profile_manager.start()
    logging.info("🔄 ProfileManager запущен (чередование: 30 мин)")

    # Сохраняем ссылку на менеджер в observer для доступа из команд
    observer_bot.profile_manager = profile_manager

    bot_thread = threading.Thread(target=observer_bot.run, daemon=True)
    bot_thread.start()
    logging.info("🤖 VK бот запущен")

    # Запуск автосохранения токенов
    tm.start_auto_save(interval=60)
    
    # Телеграм‑админка запускается отдельным сервисом (telegram-bot.service)
    logging.info("📱 Telegram admin bot запускается отдельным сервисом telegram-bot.service")

    # Таймер для периодического сохранения
    last_save_time = time.time()
    
    try:
        while True:
            # Периодическое сохранение каждые 60 секунд
            current_time = time.time()
            if current_time - last_save_time > 60:
                tm.periodic_save()
                last_save_time = current_time
            
            time.sleep(5)
    except KeyboardInterrupt:
        logging.info("🛑 Остановка по Ctrl+C")
        # Принудительно сохраняем перед выходом
        tm.save(force=True)
        profile_manager.stop()
        tm.stop_auto_save()


if __name__ == "__main__":
    try:
        main()
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"❌ Ошибка config.json: {e}")
        raise
    except Exception:
        logging.critical("💥 Критическая ошибка при запуске!", exc_info=True)
        raise
