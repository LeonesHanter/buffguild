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
from buffguild.telegram_admin import TelegramAdmin

CONFIG_PATH = os.path.join(BASE_DIR, "config.json")


def main() -> None:
    setup_logging()
    logging.info("🚀 Запуск VK Buff Guild Bot...")

    # Инициализация VK клиента
    vk = ResilientVKClient()
    
    # Инициализация менеджера токенов
    tm = OptimizedTokenManager(CONFIG_PATH, vk)
    
    # ============= ВАЖНО: Создаём ProfileManager ДО executor =============
    profile_manager = ProfileManager(tm)
    # Устанавливаем ссылку на ProfileManager в TokenManager
    tm.set_profile_manager(profile_manager)
    logging.info("🔄 ProfileManager связан с TokenManager")
    # ======================================================================
    
    # Инициализация исполнителя бафов
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

    # ============= ЗАПУСК МЕНЕДЖЕРА С ПРОФИЛЯМИ =============
    profile_manager.start()
    logging.info("🔄 ProfileManager запущен с Voice Prophet")
    
    # ============= Активируем турбо-режим =============
    observer_bot.scheduler.turbo_mode_enabled = True
    observer_bot.scheduler.TURBO_DELAY = 0.15
    observer_bot.scheduler.MIN_LETTERS_FOR_TURBO = 2
    logging.info(f"🚀 TURBO MODE активирован: задержка {observer_bot.scheduler.TURBO_DELAY}с, мин.букв {observer_bot.scheduler.MIN_LETTERS_FOR_TURBO}")

    # Сохраняем ссылку на менеджер в observer для доступа из команд
    observer_bot.profile_manager = profile_manager

    # ============= ЗАПУСК VK БОТА =============
    bot_thread = threading.Thread(target=observer_bot.run, daemon=True)
    bot_thread.start()
    logging.info("🤖 VK бот запущен")

    # ============= ЗАПУСК TELEGRAM АДМИН-БОТА =============
    telegram_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    admin_ids = os.getenv("ADMIN_USER_IDS", "")

    if telegram_token and admin_ids:
        try:
            admin_ids_list = [int(x.strip()) for x in admin_ids.split(",") if x.strip()]
            
            # Создаем Telegram админ-бота с передачей profile_manager
            telegram_admin = TelegramAdmin(
                telegram_token, 
                admin_ids_list, 
                CONFIG_PATH, 
                bot_instance=observer_bot,
                profile_manager=profile_manager  # ← Важно: передаем profile_manager!
            )
            
            # Запускаем в отдельном потоке
            tg_thread = threading.Thread(target=telegram_admin.run, daemon=True)
            tg_thread.start()
            logging.info(f"📱 Telegram admin бот запущен с поддержкой ProfileManager")
            logging.info(f"   Admin IDs: {admin_ids_list}")
        except Exception as e:
            logging.error(f"❌ Ошибка запуска Telegram бота: {e}")
    else:
        logging.warning("⚠️ TELEGRAM_BOT_TOKEN или ADMIN_USER_IDS не заданы - Telegram бот не запущен")

    # ============= ЗАПУСК АВТОСОХРАНЕНИЯ =============
    tm.start_auto_save(interval=60)
    logging.info("💾 Автосохранение токенов запущено")

    # ============= ТАЙМЕРЫ ДЛЯ ПЕРИОДИЧЕСКИХ ЗАДАЧ =============
    last_save_time = time.time()
    last_race_cleanup_time = time.time()
    RACE_CLEANUP_INTERVAL = 300  # 5 минут
    
    logging.info("✅ Система полностью запущена и готова к работе")
    
    try:
        while True:
            current_time = time.time()
            
            # Периодическое сохранение конфигурации
            if current_time - last_save_time > 60:
                tm.periodic_save()
                last_save_time = current_time
            
            # Очистка временных рас
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
        
        # Корректное завершение всех компонентов
        logging.info("🛑 Останавливаю ProfileManager...")
        profile_manager.stop()
        
        logging.info("🛑 Останавливаю автосохранение...")
        tm.stop_auto_save()
        
        logging.info("💾 Сохраняю финальную конфигурацию...")
        tm.save(force=True)
        
        logging.info("👋 Система остановлена")
        
    except Exception as e:
        logging.critical(f"💥 Критическая ошибка в главном цикле: {e}", exc_info=True)
        # Пытаемся сохранить конфигурацию перед выходом
        try:
            tm.save(force=True)
        except:
            pass
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"❌ Ошибка config.json: {e}")
        sys.exit(1)
    except Exception as e:
        logging.critical("💥 Критическая ошибка при запуске!", exc_info=True)
        sys.exit(1)
