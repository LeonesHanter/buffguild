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
from buffguild.scheduler import Scheduler
from buffguild.health import TokenHealthMonitor
from buffguild.observer_main import ObserverBot
from buffguild.profile_manager import ProfileManager

CONFIG_PATH = os.path.join(BASE_DIR, "config.json")


def main() -> None:
    setup_logging()
    logging.info("🚀 Запуск VK Buff Guild Bot...")

    vk = ResilientVKClient()
    tm = OptimizedTokenManager(CONFIG_PATH, vk)
    executor = AbilityExecutor(tm)
    
    # ============= СОЗДАЁМ SCHEDULER =============
    scheduler = Scheduler(tm, executor, on_buff_complete=None)
    
    # ============= СОЗДАЁМ HEALTH MONITOR =============
    health_monitor = TokenHealthMonitor(tm)
    
    # ============= СОЗДАЁМ OBSERVER =============
    observer_bot = ObserverBot(tm, executor, scheduler, health_monitor)

    # Проверяем тип Observer
    if observer_bot.is_group:
        logging.info("👥 Observer работает как группа ВК")
    else:
        logging.info("👤 Observer работает как пользовательский токен")

    # ============= Активируем Voice Prophet для всех токенов =============
    for token in tm.tokens:
        if token.class_type in ["apostle", "warlock", "crusader", "light_incarnation"]:
            if not token.voice_prophet:
                token.enable_voice_prophet("data/voice_prophet")
                logging.info(f"🔮 Voice Prophet активирован для {token.name}")

    # ЗАПУСК МЕНЕДЖЕРА С ПРОФИЛЯМИ
    profile_manager = ProfileManager(tm)
    profile_manager.start()
    logging.info("🔄 ProfileManager запущен с Voice Prophet")
    
    # ============= Активируем турбо-режим =============
    observer_bot.scheduler.turbo_mode_enabled = True
    observer_bot.scheduler.TURBO_DELAY = 0.15
    observer_bot.scheduler.MIN_LETTERS_FOR_TURBO = 2
    logging.info(f"🚀 TURBO MODE активирован: задержка {observer_bot.scheduler.TURBO_DELAY}с, мин.букв {observer_bot.scheduler.MIN_LETTERS_FOR_TURBO}")

    # Сохраняем ссылку на менеджер в observer для доступа из команд
    observer_bot.profile_manager = profile_manager

    bot_thread = threading.Thread(target=observer_bot.run, daemon=True)
    bot_thread.start()
    logging.info("🤖 VK бот запущен")

    # Запуск автосохранения токенов
    tm.start_auto_save(interval=60)
    
    logging.info("📱 Telegram admin bot запускается отдельным сервисом")

    # Таймеры
    last_save_time = time.time()
    last_race_cleanup_time = time.time()
    RACE_CLEANUP_INTERVAL = 300  # 5 минут
    
    try:
        while True:
            current_time = time.time()
            
            if current_time - last_save_time > 60:
                tm.periodic_save()
                last_save_time = current_time
            
            if current_time - last_race_cleanup_time > RACE_CLEANUP_INTERVAL:
                for token in tm.tokens:
                    if token.class_type == "apostle":
                        changed = token._cleanup_expired_temp_races(force=False)
                        if changed:
                            tm.update_race_index(token)
                last_race_cleanup_time = current_time
                logging.debug("🧹 Выполнена плановая очистка временных рас")
            
            time.sleep(5)
            
    except KeyboardInterrupt:
        logging.info("🛑 Остановка по Ctrl+C")
        tm.save(force=True)
        profile_manager.stop()
        tm.stop_auto_save()


if __name__ == "__main__":
    try:
        main()
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"❌ Ошибка config.json: {e}")
        sys.exit(1)
    except Exception as e:
        logging.critical("💥 Критическая ошибка при запуске!", exc_info=True)
        sys.exit(1)
