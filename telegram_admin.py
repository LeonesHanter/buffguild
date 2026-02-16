# -*- coding: utf-8 -*-
"""
Telegram админ-бот для управления токенами и сервисами.
"""
import sys
import os
import subprocess
import json
import logging
import time
import asyncio
from typing import Dict, Any, List, Optional, Tuple

# ДОБАВЛЯЕМ ПУТЬ К ПРОЕКТУ
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
    CallbackQueryHandler,
)

from buffguild.constants import RACE_NAMES

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Классы персонажей
CLASS_CHOICES = {
    "apostle": "Апостол",
    "warlock": "Чернокнижник",
    "crusader": "Крестоносец",
    "light_incarnation": "Воплощение света",
}

# Имена сервисов systemd
BUFFGUILD_SERVICE = "buffguild.service"
TELEGRAM_SERVICE = "telegram-bot.service"


class ServiceManager:
    """Класс для управления systemd сервисами"""
    
    @staticmethod
    def run_command(cmd: List[str]) -> Tuple[bool, str, str]:
        """Выполняет команду и возвращает (успех, stdout, stderr)"""
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            stdout, stderr = process.communicate(timeout=30)
            success = process.returncode == 0
            return success, stdout.strip(), stderr.strip()
        except subprocess.TimeoutExpired:
            process.kill()
            return False, "", "Timeout expired"
        except Exception as e:
            return False, "", str(e)
    
    @staticmethod
    def restart_service(service_name: str) -> Tuple[bool, str]:
        """Перезапускает systemd сервис"""
        success, stdout, stderr = ServiceManager.run_command(
            ["sudo", "systemctl", "restart", service_name]
        )
        if success:
            return True, f"✅ Сервис {service_name} успешно перезапущен"
        else:
            return False, f"❌ Ошибка перезапуска {service_name}:\n{stderr}"
    
    @staticmethod
    def stop_service(service_name: str) -> Tuple[bool, str]:
        """Останавливает systemd сервис"""
        success, stdout, stderr = ServiceManager.run_command(
            ["sudo", "systemctl", "stop", service_name]
        )
        if success:
            return True, f"✅ Сервис {service_name} остановлен"
        else:
            return False, f"❌ Ошибка остановки {service_name}:\n{stderr}"
    
    @staticmethod
    def start_service(service_name: str) -> Tuple[bool, str]:
        """Запускает systemd сервис"""
        success, stdout, stderr = ServiceManager.run_command(
            ["sudo", "systemctl", "start", service_name]
        )
        if success:
            return True, f"✅ Сервис {service_name} запущен"
        else:
            return False, f"❌ Ошибка запуска {service_name}:\n{stderr}"
    
    @staticmethod
    def get_service_status(service_name: str) -> Dict[str, Any]:
        """Получает статус сервиса"""
        # Проверяем, активен ли сервис
        success, stdout, stderr = ServiceManager.run_command(
            ["systemctl", "is-active", service_name]
        )
        is_active = success and stdout.strip() == "active"
        
        # Получаем детальный статус
        success, stdout, stderr = ServiceManager.run_command(
            ["systemctl", "status", service_name, "--no-pager"]
        )
        
        # Извлекаем основные метрики
        status_text = stdout if success else stderr
        pid = None
        memory = None
        cpu = None
        
        for line in status_text.split('\n'):
            if 'Main PID:' in line:
                pid_match = line.split('Main PID:')[1].strip().split()[0]
                pid = pid_match
            if 'Memory:' in line:
                memory = line.split('Memory:')[1].strip()
            if 'CPU:' in line:
                cpu = line.split('CPU:')[1].strip()
        
        return {
            'name': service_name,
            'active': is_active,
            'pid': pid,
            'memory': memory,
            'cpu': cpu,
            'status_text': status_text[:500] + "..." if len(status_text) > 500 else status_text
        }
    
    @staticmethod
    def get_logs(service_name: str, lines: int = 50) -> str:
        """Получает последние логи сервиса"""
        success, stdout, stderr = ServiceManager.run_command(
            ["sudo", "journalctl", "-u", service_name, "-n", str(lines), "--no-pager"]
        )
        if success:
            return stdout
        else:
            return f"Ошибка получения логов:\n{stderr}"
    
    @staticmethod
    def check_sudo_permissions() -> bool:
        """Проверяет, есть ли права sudo без пароля"""
        success, stdout, stderr = ServiceManager.run_command(
            ["sudo", "-n", "true"]
        )
        return success


class TelegramAdmin:
    """Telegram бот для управления токенами и сервисами"""

    # Состояния для ConversationHandler
    WAIT_NAME = 1
    WAIT_CLASS = 2
    WAIT_TOKEN = 3
    WAIT_CHAT = 4
    WAIT_VOICES = 5
    WAIT_RACES = 6

    def __init__(
        self, 
        telegram_token: str, 
        admin_ids: List[int], 
        config_path: str, 
        bot_instance=None,
        profile_manager=None  # ← Добавляем profile_manager
    ):
        self.telegram_token = telegram_token
        self.admin_ids = set(admin_ids)
        self.config_path = config_path
        self.bot_instance = bot_instance
        self.profile_manager = profile_manager  # ← Сохраняем profile_manager
        self.tmp: Dict[int, Dict[str, Any]] = {}
        
        # Проверяем права sudo при инициализации
        self.sudo_available = ServiceManager.check_sudo_permissions()
        if not self.sudo_available:
            logging.warning("⚠️ Нет прав sudo без пароля! Команды управления сервисами будут недоступны.")

    def is_admin(self, uid: int) -> bool:
        """Проверка прав администратора"""
        return uid in self.admin_ids

    def _load(self) -> Dict[str, Any]:
        """Загрузка config.json"""
        if not os.path.exists(self.config_path):
            return {"tokens": [], "settings": {"delay": 2}}
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            logging.error("Invalid JSON in config")
            return {"tokens": [], "settings": {"delay": 2}}

    def _save(self, cfg: Dict[str, Any]) -> None:
        """Сохранение config.json"""
        with open(self.config_path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)

    # ---- Основные команды ----

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /start"""
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return

        sudo_status = "✅ Есть" if self.sudo_available else "❌ Нет (команды управления сервисами недоступны)"
        pm_status = "✅ Доступен" if self.profile_manager else "❌ Не инициализирован"
        
        msg = (
            "🤖 **Blessing Bot Admin Panel**\n\n"
            "📋 **Команды управления токенами:**\n"
            "/add_token — добавить токен\n"
            "/list_tokens — список токенов\n"
            "/enable — включить токен\n"
            "/disable — отключить токен\n"
            "/remove — удалить токен\n"
            "/reload — перезагрузить конфиг\n"
            "/token_info — детальная информация о токене\n"
            "/set_voices — установить голоса\n\n"
            "🛠 **Команды управления сервисами:**\n"
            "/restart_bot — перезапустить buffguild.service\n"
            "/restart_tg — перезапустить telegram-bot.service\n"
            "/status — статус сервисов\n"
            "/logs — последние логи buffguild.service\n"
            "/watch — слежение за логами\n\n"
            "📊 **Мониторинг и диагностика:**\n"
            "/stats — общая статистика системы\n"
            "/profile — управление ProfileManager\n"
            "/diagnose — полная диагностика\n\n"
            f"🔐 **Права sudo:** {sudo_status}\n"
            f"📊 **ProfileManager:** {pm_status}"
        )
        await update.message.reply_text(msg, parse_mode='Markdown')

    # ---- Мониторинг и статистика ----

    async def system_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /stats - общая статистика системы"""
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return
        
        # Получаем статус сервисов
        bot_status = ServiceManager.get_service_status(BUFFGUILD_SERVICE)
        tg_status = ServiceManager.get_service_status(TELEGRAM_SERVICE)
        
        # Системная информация
        success, uname, _ = ServiceManager.run_command(["uname", "-a"])
        success, uptime, _ = ServiceManager.run_command(["uptime"])
        success, disk, _ = ServiceManager.run_command(["df", "-h", "/"])
        success, memory, _ = ServiceManager.run_command(["free", "-h"])
        
        # Информация о токенах из конфига
        cfg = self._load()
        tokens = cfg.get("tokens", [])
        enabled_tokens = sum(1 for t in tokens if t.get("enabled", True))
        total_voices = sum(t.get("voices", 0) for t in tokens)
        
        # Статистика по классам
        apostles = sum(1 for t in tokens if t.get("class") == "apostle")
        warlocks = sum(1 for t in tokens if t.get("class") == "warlock")
        paladins = sum(1 for t in tokens if t.get("class") in ["crusader", "light_incarnation"])
        
        stats_msg = (
            "📊 **СИСТЕМНАЯ СТАТИСТИКА**\n\n"
            f"**Сервисы:**\n"
            f"• {BUFFGUILD_SERVICE}: {'✅' if bot_status['active'] else '❌'}\n"
            f"• {TELEGRAM_SERVICE}: {'✅' if tg_status['active'] else '❌'}\n\n"
            f"**Токены VK:**\n"
            f"• Всего: {len(tokens)}\n"
            f"• Активных: {enabled_tokens}\n"
            f"• Апостолы: {apostles}\n"
            f"• Чернокнижники: {warlocks}\n"
            f"• Паладины: {paladins}\n"
            f"• Всего голосов: {total_voices}\n\n"
            f"**Система:**\n"
            f"• Uptime: {uptime[:100]}...\n"
            f"• Диск: {disk.splitlines()[-1] if disk else 'N/A'}\n"
            f"• Память: {memory.splitlines()[1] if memory else 'N/A'}"
        )
        
        await update.message.reply_text(stats_msg, parse_mode='Markdown')

    # ---- Управление токенами ----

    async def token_info(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /token_info - детальная информация о токене"""
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return
        
        if not context.args:
            await update.message.reply_text("Использование: /token_info <имя_токена>")
            return
        
        name = " ".join(context.args)
        cfg = self._load()
        
        for t in cfg.get("tokens", []):
            if t.get("name", "").lower() == name.lower():
                # Форматируем временные расы
                temp_races = []
                for tr in t.get("temp_races", []):
                    expires = tr.get("expires", 0)
                    if expires > time.time():
                        remaining = int(expires - time.time())
                        hours = remaining // 3600
                        minutes = (remaining % 3600) // 60
                        temp_races.append(f"{tr['race']} ({hours}ч {minutes}м)")
                
                # Статистика успешности
                total = t.get("total_attempts", 0)
                success = t.get("successful_buffs", 0)
                success_rate = (success / total * 100) if total > 0 else 0
                
                # Статус капчи
                captcha_until = t.get("captcha_until", 0)
                captcha_status = "нет"
                if captcha_until > time.time():
                    remaining = int(captcha_until - time.time())
                    minutes = remaining // 60
                    captcha_status = f"капча до {time.ctime(captcha_until)} (осталось {minutes} мин)"
                
                info_msg = (
                    f"🔍 **Информация о токене: {t.get('name')}**\n\n"
                    f"**Основное:**\n"
                    f"• ID: `{t.get('id')}`\n"
                    f"• Класс: {CLASS_CHOICES.get(t.get('class'), t.get('class'))}\n"
                    f"• Статус: {'✅ Активен' if t.get('enabled', True) else '❌ Отключен'}\n"
                    f"• Владелец VK: {t.get('owner_vk_id', 0)}\n"
                    f"• Уровень: {t.get('level', 0)}\n\n"
                    f"**Голоса:**\n"
                    f"• Текущие: {t.get('voices', 0)}\n"
                    f"• Нужен ручной ввод: {'⚠️ Да' if t.get('needs_manual_voices', False) else '✅ Нет'}\n"
                    f"• Виртуальных выдач: {t.get('virtual_voice_grants', 0)}\n\n"
                    f"**Расы:**\n"
                    f"• Постоянные: {', '.join(t.get('races', [])) or 'нет'}\n"
                    f"• Временные: {', '.join(temp_races) or 'нет'}\n\n"
                    f"**Статистика:**\n"
                    f"• Успешных бафов: {success}/{total} ({success_rate:.1f}%)\n"
                    f"• Капча: {captcha_status}"
                )
                
                await update.message.reply_text(info_msg, parse_mode='Markdown')
                return
        
        await update.message.reply_text(f"❌ Токен '{name}' не найден")

    async def set_voices(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /set_voices - установка голосов для токена"""
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return
        
        if len(context.args) < 2:
            await update.message.reply_text("Использование: /set_voices <имя_токена> <количество>")
            return
        
        name = context.args[0]
        try:
            voices = int(context.args[1])
            if voices < 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Количество голосов должно быть положительным числом")
            return
        
        cfg = self._load()
        for t in cfg.get("tokens", []):
            if t.get("name", "").lower() == name.lower():
                old_voices = t.get("voices", 0)
                t["voices"] = voices
                t["needs_manual_voices"] = False
                self._save(cfg)
                
                await update.message.reply_text(
                    f"✅ Голоса для '{name}' изменены: {old_voices} → {voices}\n"
                    f"📌 Статус ручного ввода сброшен"
                )
                return
        
        await update.message.reply_text(f"❌ Токен '{name}' не найден")

    # ---- Управление сервисами ----

    async def restart_bot(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Перезапуск buffguild.service"""
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return
        
        if not self.sudo_available:
            await update.message.reply_text(
                "❌ Нет прав sudo без пароля.\n"
                "Настройте sudoers: добавьте 'ALL ALL=(ALL) NOPASSWD: /usr/bin/systemctl'"
            )
            return
        
        await update.message.reply_text(f"🔄 Перезапускаю {BUFFGUILD_SERVICE}...")
        
        success, message = ServiceManager.restart_service(BUFFGUILD_SERVICE)
        await update.message.reply_text(message)
        
        # Если успешно, показываем статус
        if success:
            await asyncio.sleep(2)  # Даем время на запуск
            status = ServiceManager.get_service_status(BUFFGUILD_SERVICE)
            status_msg = (
                f"📊 **Статус после перезапуска:**\n"
                f"Активен: {'✅' if status['active'] else '❌'}\n"
                f"PID: {status['pid'] or 'N/A'}\n"
                f"Память: {status['memory'] or 'N/A'}\n"
                f"CPU: {status['cpu'] or 'N/A'}"
            )
            await update.message.reply_text(status_msg, parse_mode='Markdown')

    async def restart_tg(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Перезапуск telegram-bot.service"""
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return
        
        if not self.sudo_available:
            await update.message.reply_text(
                "❌ Нет прав sudo без пароля.\n"
                "Настройте sudoers: добавьте 'ALL ALL=(ALL) NOPASSWD: /usr/bin/systemctl'"
            )
            return
        
        await update.message.reply_text(f"🔄 Перезапускаю {TELEGRAM_SERVICE}...")
        
        success, message = ServiceManager.restart_service(TELEGRAM_SERVICE)
        await update.message.reply_text(message)
        
        if success:
            await asyncio.sleep(2)
            status = ServiceManager.get_service_status(TELEGRAM_SERVICE)
            status_msg = (
                f"📊 **Статус после перезапуска:**\n"
                f"Активен: {'✅' if status['active'] else '❌'}\n"
                f"PID: {status['pid'] or 'N/A'}\n"
                f"Память: {status['memory'] or 'N/A'}\n"
                f"CPU: {status['cpu'] or 'N/A'}"
            )
            await update.message.reply_text(status_msg, parse_mode='Markdown')

    async def service_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /status - статус сервисов"""
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return
        
        # Получаем статус обоих сервисов
        bot_status = ServiceManager.get_service_status(BUFFGUILD_SERVICE)
        tg_status = ServiceManager.get_service_status(TELEGRAM_SERVICE)
        
        # Формируем клавиатуру для действий
        keyboard = [
            [
                InlineKeyboardButton("🔄 Перезапустить бота", callback_data="restart_bot"),
                InlineKeyboardButton("🔄 Перезапустить TG", callback_data="restart_tg")
            ],
            [
                InlineKeyboardButton("📋 Логи бота", callback_data="logs_bot"),
                InlineKeyboardButton("📋 Логи TG", callback_data="logs_tg")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        status_msg = (
            "📊 **СТАТУС СЕРВИСОВ**\n\n"
            f"**{BUFFGUILD_SERVICE}**\n"
            f"Активен: {'✅' if bot_status['active'] else '❌'}\n"
            f"PID: {bot_status['pid'] or 'N/A'}\n"
            f"Память: {bot_status['memory'] or 'N/A'}\n"
            f"CPU: {bot_status['cpu'] or 'N/A'}\n\n"
            f"**{TELEGRAM_SERVICE}**\n"
            f"Активен: {'✅' if tg_status['active'] else '❌'}\n"
            f"PID: {tg_status['pid'] or 'N/A'}\n"
            f"Память: {tg_status['memory'] or 'N/A'}\n"
            f"CPU: {tg_status['cpu'] or 'N/A'}"
        )
        
        await update.message.reply_text(status_msg, reply_markup=reply_markup, parse_mode='Markdown')

    async def service_logs(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /logs - последние логи buffguild.service"""
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return
        
        if not self.sudo_available:
            await update.message.reply_text(
                "❌ Нет прав sudo без пароля.\n"
                "Настройте sudoers: добавьте 'ALL ALL=(ALL) NOPASSWD: /usr/bin/journalctl'"
            )
            return
        
        # Определяем, сколько строк показать
        lines = 50
        if context.args and context.args[0].isdigit():
            lines = int(context.args[0])
        
        await update.message.reply_text(f"📋 Получаю последние {lines} строк логов {BUFFGUILD_SERVICE}...")
        
        logs = ServiceManager.get_logs(BUFFGUILD_SERVICE, lines)
        
        # Telegram имеет лимит 4096 символов на сообщение
        if len(logs) > 4000:
            # Отправляем частями
            for i in range(0, len(logs), 4000):
                part = logs[i:i+4000]
                await update.message.reply_text(f"```\n{part}\n```", parse_mode='Markdown')
        else:
            await update.message.reply_text(f"```\n{logs}\n```", parse_mode='Markdown')

    # ---- Мониторинг в реальном времени ----

    async def watch_logs(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /watch - слежение за логами в реальном времени"""
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return
        
        if not self.sudo_available:
            await update.message.reply_text("❌ Нет прав sudo без пароля.")
            return
        
        # Сохраняем состояние в context.user_data
        context.user_data['watching'] = True
        context.user_data['last_logs'] = ""
        context.user_data['watch_message_id'] = None
        context.user_data['watch_chat_id'] = update.effective_chat.id
        
        keyboard = [[InlineKeyboardButton("🛑 Остановить", callback_data="stop_watching")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        msg = await update.message.reply_text(
            "📋 **Режим наблюдения за логами активирован**\n"
            "Новые строки будут появляться каждые 10 секунд.\n"
            "Нажмите кнопку ниже для остановки.",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        
        context.user_data['watch_message_id'] = msg.message_id
        
        # Запускаем фоновую задачу
        asyncio.create_task(self._watch_logs_task(context))

    async def _watch_logs_task(self, context: ContextTypes.DEFAULT_TYPE):
        """Фоновая задача для отправки логов"""
        chat_id = context.user_data.get('watch_chat_id')
        message_id = context.user_data.get('watch_message_id')
        
        if not chat_id or not message_id:
            return
        
        while context.user_data.get('watching', False):
            try:
                # Получаем новые логи
                logs = ServiceManager.get_logs(BUFFGUILD_SERVICE, 20)
                
                # Сравниваем с предыдущими
                if logs != context.user_data.get('last_logs', ''):
                    context.user_data['last_logs'] = logs
                    
                    # Обрезаем если слишком длинные
                    display_logs = logs[-3500:] if len(logs) > 3500 else logs
                    
                    # Редактируем существующее сообщение
                    try:
                        await context.bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=message_id,
                            text=f"```\n{display_logs}\n```",
                            parse_mode='Markdown',
                            reply_markup=InlineKeyboardMarkup([[
                                InlineKeyboardButton("🛑 Остановить", callback_data="stop_watching")
                            ]])
                        )
                    except Exception as e:
                        # Если не удалось отредактировать (например, слишком длинное), отправляем новое
                        if "Message is not modified" not in str(e):
                            msg = await context.bot.send_message(
                                chat_id=chat_id,
                                text=f"```\n{display_logs}\n```",
                                parse_mode='Markdown',
                                reply_markup=InlineKeyboardMarkup([[
                                    InlineKeyboardButton("🛑 Остановить", callback_data="stop_watching")
                                ]])
                            )
                            context.user_data['watch_message_id'] = msg.message_id
                
                # Ждем 10 секунд
                for _ in range(10):
                    if not context.user_data.get('watching', False):
                        break
                    await asyncio.sleep(1)
                    
            except Exception as e:
                logging.error(f"Ошибка в watch_logs_task: {e}")
                break

    # ---- Управление ProfileManager ----

    async def profile_manager_control(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /profile - управление ProfileManager"""
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return
        
        if not self.profile_manager:
            await update.message.reply_text(
                "❌ ProfileManager не инициализирован.\n"
                "Убедитесь, что он передан в конструктор TelegramAdmin."
            )
            return
        
        # Получаем статус ProfileManager
        is_running = hasattr(self.profile_manager, '_running') and self.profile_manager._running
        
        keyboard = [
            [
                InlineKeyboardButton("▶️ Запустить", callback_data="pm_start"),
                InlineKeyboardButton("⏸️ Остановить", callback_data="pm_stop")
            ],
            [
                InlineKeyboardButton("🔄 Перезапустить", callback_data="pm_restart"),
                InlineKeyboardButton("📊 Статус", callback_data="pm_status")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            f"**Управление ProfileManager**\n"
            f"Текущий статус: {'✅ Запущен' if is_running else '⏸️ Остановлен'}\n\n"
            f"Выберите действие:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )

    # ---- Диагностика ----

    async def full_diagnose(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /diagnose - полная диагностика системы"""
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return
        
        await update.message.reply_text("🔍 Запускаю диагностику...")
        
        # Проверка сервисов
        bot_status = ServiceManager.get_service_status(BUFFGUILD_SERVICE)
        tg_status = ServiceManager.get_service_status(TELEGRAM_SERVICE)
        
        # Проверка API VK
        vk_check = "✅ OK"
        vk_error = ""
        if self.bot_instance and hasattr(self.bot_instance, 'tm') and self.bot_instance.tm:
            try:
                # Пробуем получить observer
                observer = self.bot_instance.tm.get_observer()
                if observer:
                    vk_check = "✅ OK (есть observer)"
                else:
                    vk_check = "⚠️ Observer не найден"
            except Exception as e:
                vk_check = "❌ Ошибка"
                vk_error = str(e)
        else:
            vk_check = "❌ Нет доступа к VK боту"
        
        # Проверка ProfileManager
        pm_check = "✅ Доступен" if self.profile_manager else "❌ Не инициализирован"
        pm_status = ""
        if self.profile_manager:
            is_running = hasattr(self.profile_manager, '_running') and self.profile_manager._running
            pm_status = f" ({'запущен' if is_running else 'остановлен'})"
        
        # Проверка файлов
        files_check = []
        for f in ["config.json", "jobs.json", "profile_manager_state.json"]:
            if os.path.exists(f):
                size = os.path.getsize(f) / 1024
                mtime = os.path.getmtime(f)
                age_hours = (time.time() - mtime) / 3600
                files_check.append(f"✅ {f} ({size:.1f} KB, изменён {age_hours:.1f} ч назад)")
            else:
                files_check.append(f"⚠️ {f} (не найден)")
        
        # Проверка директорий
        dirs_check = []
        for d in ["data/voice_prophet", "logs"]:
            if os.path.exists(d):
                files = os.listdir(d) if os.path.isdir(d) else []
                dirs_check.append(f"✅ {d}/ ({len(files)} файлов)")
            else:
                dirs_check.append(f"⚠️ {d}/ (не найдена)")
        
        # Проверка токенов
        cfg = self._load()
        tokens = cfg.get("tokens", [])
        tokens_with_issues = []
        total_success = 0
        total_attempts = 0
        
        for t in tokens:
            issues = []
            if not t.get("access_token"):
                issues.append("нет токена")
            if t.get("needs_manual_voices"):
                issues.append("ручной ввод")
            if t.get("captcha_until", 0) > time.time():
                issues.append("капча")
            if not t.get("enabled", True):
                issues.append("отключен")
            
            total_success += t.get("successful_buffs", 0)
            total_attempts += t.get("total_attempts", 0)
            
            if issues:
                tokens_with_issues.append(f"  • {t.get('name')}: {', '.join(issues)}")
        
        success_rate = (total_success / total_attempts * 100) if total_attempts > 0 else 0
        
        # Проверка прав sudo
        sudo_check = "✅ Есть" if self.sudo_available else "❌ Нет"
        
        diag_msg = (
            "📋 **РЕЗУЛЬТАТЫ ДИАГНОСТИКИ**\n\n"
            f"**Сервисы:**\n"
            f"• {BUFFGUILD_SERVICE}: {'✅' if bot_status['active'] else '❌'}\n"
            f"• {TELEGRAM_SERVICE}: {'✅' if tg_status['active'] else '❌'}\n"
            f"• VK API: {vk_check}\n"
            f"{'  ' + vk_error if vk_error else ''}\n"
            f"• ProfileManager: {pm_check}{pm_status}\n\n"
            f"**Файлы:**\n" + "\n".join(files_check) + "\n\n"
            f"**Директории:**\n" + "\n".join(dirs_check) + "\n\n"
            f"**Токены:**\n"
            f"• Всего: {len(tokens)}\n"
            f"• Общая успешность: {success_rate:.1f}% ({total_success}/{total_attempts})\n"
        )
        
        if tokens_with_issues:
            diag_msg += "• Проблемные:\n" + "\n".join(tokens_with_issues) + "\n"
        else:
            diag_msg += "• Все токены в порядке ✅\n"
        
        diag_msg += f"\n**Права sudo:** {sudo_check}"
        
        await update.message.reply_text(diag_msg, parse_mode='Markdown')

    # ---- Обработка кнопок ----

    async def button_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка нажатий на инлайн-кнопки"""
        query = update.callback_query
        await query.answer()
        
        uid = query.from_user.id
        if not self.is_admin(uid):
            await query.edit_message_text("❌ Нет прав.")
            return
        
        # Управление сервисами
        if query.data == "restart_bot":
            await query.edit_message_text(f"🔄 Перезапускаю {BUFFGUILD_SERVICE}...")
            success, message = ServiceManager.restart_service(BUFFGUILD_SERVICE)
            await query.edit_message_text(message)
        
        elif query.data == "restart_tg":
            await query.edit_message_text(f"🔄 Перезапускаю {TELEGRAM_SERVICE}...")
            success, message = ServiceManager.restart_service(TELEGRAM_SERVICE)
            await query.edit_message_text(message)
        
        elif query.data == "logs_bot":
            logs = ServiceManager.get_logs(BUFFGUILD_SERVICE, 30)
            if len(logs) > 4000:
                logs = logs[:4000] + "..."
            await query.edit_message_text(f"```\n{logs}\n```", parse_mode='Markdown')
        
        elif query.data == "logs_tg":
            logs = ServiceManager.get_logs(TELEGRAM_SERVICE, 30)
            if len(logs) > 4000:
                logs = logs[:4000] + "..."
            await query.edit_message_text(f"```\n{logs}\n```", parse_mode='Markdown')
        
        # Остановка слежения
        elif query.data == "stop_watching":
            context.user_data['watching'] = False
            await query.edit_message_text("🛑 Наблюдение остановлено")
        
        # Управление ProfileManager
        elif query.data == "pm_start":
            if not self.profile_manager:
                await query.edit_message_text("❌ ProfileManager не инициализирован")
                return
            
            if hasattr(self.profile_manager, 'start'):
                self.profile_manager.start()
                await query.edit_message_text("✅ ProfileManager запущен")
            else:
                await query.edit_message_text("❌ Метод start не найден")
        
        elif query.data == "pm_stop":
            if not self.profile_manager:
                await query.edit_message_text("❌ ProfileManager не инициализирован")
                return
            
            if hasattr(self.profile_manager, 'stop'):
                self.profile_manager.stop()
                await query.edit_message_text("⏸️ ProfileManager остановлен")
            else:
                await query.edit_message_text("❌ Метод stop не найден")
        
        elif query.data == "pm_restart":
            if not self.profile_manager:
                await query.edit_message_text("❌ ProfileManager не инициализирован")
                return
            
            if hasattr(self.profile_manager, 'stop'):
                self.profile_manager.stop()
            await asyncio.sleep(2)
            if hasattr(self.profile_manager, 'start'):
                self.profile_manager.start()
            await query.edit_message_text("🔄 ProfileManager перезапущен")
        
        elif query.data == "pm_status":
            if not self.profile_manager:
                await query.edit_message_text("❌ ProfileManager не инициализирован")
                return
            
            is_running = hasattr(self.profile_manager, '_running') and self.profile_manager._running
            status_msg = f"📊 ProfileManager: {'✅ Запущен' if is_running else '⏸️ Остановлен'}"
            
            # Добавляем статистику если есть
            if hasattr(self.profile_manager, '_state'):
                pending = len(self.profile_manager._state.get("pending_triggers", {}))
                status_msg += f"\nАктивных триггеров: {pending}"
            
            await query.edit_message_text(status_msg)

    # ---- Существующие методы управления токенами (без изменений) ----

    async def add_token(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Начало добавления токена"""
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return ConversationHandler.END

        self.tmp[uid] = {}
        await update.message.reply_text(
            "➕ Добавление токена\n\n"
            "📝 Шаг 1/6: Введите имя токена\n"
            "Например: Main, Backup1, Reserve"
        )
        return self.WAIT_NAME

    async def recv_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Получение имени"""
        uid = update.effective_user.id
        name = update.message.text.strip()
        if len(name) < 2:
            await update.message.reply_text("Слишком короткое имя. Ещё раз:")
            return self.WAIT_NAME

        self.tmp[uid]["name"] = name
        classes = "\n".join(
            [f"{k} — {v}" for k, v in CLASS_CHOICES.items()]
        )
        await update.message.reply_text(
            f"✅ Имя: {name}\n\n"
            f"🎭 Шаг 2/6: Выберите класс\n\n"
            f"{classes}\n\n"
            f"Отправьте код класса (например: apostle)"
        )
        return self.WAIT_CLASS

    async def recv_class(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Получение класса"""
        uid = update.effective_user.id
        cls = update.message.text.strip().lower()
        if cls not in CLASS_CHOICES:
            await update.message.reply_text(
                f"❌ Неизвестный класс: {cls}\n\n"
                f"Доступные: {', '.join(CLASS_CHOICES.keys())}"
            )
            return self.WAIT_CLASS

        self.tmp[uid]["class"] = cls
        class_name = CLASS_CHOICES[cls]
        await update.message.reply_text(
            f"✅ Класс: {class_name}\n\n"
            f"🔑 Шаг 3/6: Отправьте VK access token\n"
            f"Токен должен начинаться с vk1.a."
        )
        return self.WAIT_TOKEN

    async def recv_token(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Получение токена"""
        uid = update.effective_user.id
        token = update.message.text.strip()
        if not token.startswith("vk1.a."):
            await update.message.reply_text(
                "❌ Неверный формат токена. Должен начинаться с vk1.a."
            )
            return self.WAIT_TOKEN

        self.tmp[uid]["access_token"] = token
        await update.message.reply_text(
            "✅ Токен сохранён\n\n"
            "📁 Шаг 4/6: ID чата (source_chat_id)\n"
            "Например: 48"
        )
        return self.WAIT_CHAT

    async def recv_chat(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Получение chat_id"""
        uid = update.effective_user.id
        try:
            chat_id = int(update.message.text.strip())
        except ValueError:
            await update.message.reply_text("❌ Нужно число.")
            return self.WAIT_CHAT

        self.tmp[uid]["source_chat_id"] = chat_id

        await update.message.reply_text(
            "🔊 Шаг 5/6: Введите стартовое количество голосов для токена\n"
            "Например: 27"
        )
        return self.WAIT_VOICES

    async def recv_voices(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Получение стартовых голосов"""
        uid = update.effective_user.id
        text = update.message.text.strip()
        try:
            voices = int(text)
            if voices < 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Нужно неотрицательное число голосов. Попробуйте ещё раз:")
            return self.WAIT_VOICES

        self.tmp[uid]["voices"] = voices

        cls = self.tmp[uid].get("class")
        if cls == "apostle":
            await update.message.reply_text(
                "🎭 Шаг 6/6: Укажите основные расы для апостола\n"
                "Формат: буквы через запятую, например: ч,г\n"
                "Доступные коды рас смотрите в описании бота."
            )
            return self.WAIT_RACES

        # если не апостол — завершаем создание
        await self._finalize_token_creation(uid, update)
        return ConversationHandler.END

    async def recv_races(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Получение основных рас для апостола"""
        uid = update.effective_user.id
        text = update.message.text.strip().replace(" ", "")
        text = text.replace(";", ",")
        race_keys_raw = [r for r in text.split(",") if r]

        if not race_keys_raw:
            await update.message.reply_text("❌ Не указаны расы. Введите, например: ч,г")
            return self.WAIT_RACES

        seen = set()
        race_keys: List[str] = []
        for rk in race_keys_raw:
            if rk in seen:
                await update.message.reply_text(
                    f"❌ Нельзя указывать одну и ту же расу несколько раз ('{rk}')."
                )
                return self.WAIT_RACES
            seen.add(rk)
            race_keys.append(rk)

        for rk in race_keys:
            if rk not in RACE_NAMES:
                await update.message.reply_text(
                    f"❌ Неизвестная раса '{rk}'. Введите заново."
                )
                return self.WAIT_RACES

        self.tmp[uid]["races"] = race_keys
        await self._finalize_token_creation(uid, update)
        return ConversationHandler.END

    async def _finalize_token_creation(self, uid: int, update: Update):
        """Финальное создание токена и запись в config.json"""
        data = self.tmp.get(uid, {})
        target_peer = -183040898
        token_id = f"token_{int(time.time())}"

        voices = int(data.get("voices", 0))
        races = data.get("races", []) if data.get("class") == "apostle" else []

        new_token = {
            "id": token_id,
            "name": data["name"],
            "class": data["class"],
            "access_token": data["access_token"],
            "owner_vk_id": 0,
            "source_chat_id": data["source_chat_id"],
            "target_peer_id": target_peer,
            "voices": voices,
            "enabled": True,
            "races": races,
            "temp_races": [],
            "captcha_until": 0,
            "level": 0,
            "needs_manual_voices": False,
            "virtual_voice_grants": 0,
            "next_virtual_grant_ts": 0,
        }

        cfg = self._load()
        cfg.setdefault("tokens", []).append(new_token)
        cfg.setdefault("settings", {}).setdefault("delay", 2)
        self._save(cfg)

        if self.bot_instance and hasattr(self.bot_instance, "tm"):
            self.bot_instance.tm.reload()
            logging.info("🔄 TokenManager.reload() после добавления токена")

        self.tmp.pop(uid, None)

        class_name = CLASS_CHOICES[new_token["class"]]
        races_str = ", ".join(races) if races else "-"

        message = (
            "✅ Токен добавлен!\n\n"
            f"📛 Имя: {new_token['name']}\n"
            f"🎭 Класс: {class_name}\n"
            f"🆔 ID: {token_id}\n"
            f"📁 Chat: {new_token['source_chat_id']}\n"
            f"🎯 Target: {target_peer}\n"
            f"🔊 Голосов: {voices}\n"
            f"🧬 Основные расы: {races_str}\n"
            f"✅ Статус: Активен"
        )
        await update.message.reply_text(message)

    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отмена диалога"""
        uid = update.effective_user.id
        self.tmp.pop(uid, None)
        await update.message.reply_text("❌ Отменено.")
        return ConversationHandler.END

    async def list_tokens(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Список всех токенов"""
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return

        cfg = self._load()
        tokens = cfg.get("tokens", [])
        if not tokens:
            await update.message.reply_text("📭 Нет токенов.")
            return

        lines = ["📋 **Список токенов:**\n"]
        for i, t in enumerate(tokens, 1):
            cls = t.get("class", "apostle")
            cls_name = CLASS_CHOICES.get(cls, cls)
            status = "✅" if t.get("enabled", True) else "🚫"
            voices = t.get("voices", "?")
            voices_emoji = "🔊" if isinstance(voices, int) and voices > 0 else "🔇"
            manual = "⚠️" if t.get("needs_manual_voices", False) else ""

            lines.append(
                f"{i}. **{t.get('name', t['id'])}**\n"
                f"  🎭 {cls_name}\n"
                f"  {status} {voices_emoji} Голоса: {voices} {manual}\n"
                f"  🆔 `{t['id']}`"
            )

        await update.message.reply_text("\n\n".join(lines), parse_mode='Markdown')

    def _toggle(self, name: str, enabled: bool) -> bool:
        """Включить/отключить токен по имени"""
        cfg = self._load()
        changed = False
        for t in cfg.get("tokens", []):
            if t.get("name") == name:
                t["enabled"] = enabled
                changed = True

        if changed:
            self._save(cfg)
        return changed

    async def enable(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Включить токен"""
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return

        if not context.args:
            await update.message.reply_text("Использование: /enable <name>")
            return

        name = " ".join(context.args)
        ok = self._toggle(name, True)
        await update.message.reply_text(
            f"✅ Токен '{name}' включён" if ok else f"❌ Не найдено токена с именем: '{name}'"
        )

    async def disable(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отключить токен"""
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return

        if not context.args:
            await update.message.reply_text("Использование: /disable <name>")
            return

        name = " ".join(context.args)
        ok = self._toggle(name, False)
        await update.message.reply_text(
            f"🚫 Токен '{name}' отключён" if ok else f"❌ Не найдено токена с именем: '{name}'"
        )

    async def remove(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Удалить токен по имени"""
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return

        if not context.args:
            await update.message.reply_text("Использование: /remove <name>")
            return

        name = " ".join(context.args)
        cfg = self._load()
        before = len(cfg.get("tokens", []))
        cfg["tokens"] = [
            t for t in cfg.get("tokens", []) if t.get("name") != name
        ]
        after = len(cfg["tokens"])

        if after < before:
            self._save(cfg)
            await update.message.reply_text(f"🗑️ Токен '{name}' удалён")
        else:
            await update.message.reply_text(f"❌ Не найдено токена с именем: '{name}'")

    async def reload_config(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Перезагрузить конфигурацию"""
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return

        # Перезагружаем конфиг
        _ = self._load()
        
        # Если есть ссылка на VK бота, перезагружаем и его
        if self.bot_instance and hasattr(self.bot_instance, "tm"):
            self.bot_instance.tm.reload()
            await update.message.reply_text("🔄 Конфигурация перечитана с диска и VK бот перезагружен")
        else:
            await update.message.reply_text("🔄 Конфигурация перечитана с диска (локально)")

    # ---- Запуск ----

    def run(self):
        """Запуск Telegram бота"""
        app = Application.builder().token(self.telegram_token).build()

        # Диалог добавления токена
        conv = ConversationHandler(
            entry_points=[CommandHandler("add_token", self.add_token)],
            states={
                self.WAIT_NAME: [
                    MessageHandler(
                        filters.TEXT & ~filters.COMMAND, self.recv_name
                    )
                ],
                self.WAIT_CLASS: [
                    MessageHandler(
                        filters.TEXT & ~filters.COMMAND, self.recv_class
                    )
                ],
                self.WAIT_TOKEN: [
                    MessageHandler(
                        filters.TEXT & ~filters.COMMAND, self.recv_token
                    )
                ],
                self.WAIT_CHAT: [
                    MessageHandler(
                        filters.TEXT & ~filters.COMMAND, self.recv_chat
                    )
                ],
                self.WAIT_VOICES: [
                    MessageHandler(
                        filters.TEXT & ~filters.COMMAND, self.recv_voices
                    )
                ],
                self.WAIT_RACES: [
                    MessageHandler(
                        filters.TEXT & ~filters.COMMAND, self.recv_races
                    )
                ],
            },
            fallbacks=[CommandHandler("cancel", self.cancel)],
        )

        # Основные команды управления токенами
        app.add_handler(CommandHandler("start", self.start))
        app.add_handler(conv)
        app.add_handler(CommandHandler("list_tokens", self.list_tokens))
        app.add_handler(CommandHandler("enable", self.enable))
        app.add_handler(CommandHandler("disable", self.disable))
        app.add_handler(CommandHandler("remove", self.remove))
        app.add_handler(CommandHandler("reload", self.reload_config))
        
        # Новые команды для информации о токенах
        app.add_handler(CommandHandler("token_info", self.token_info))
        app.add_handler(CommandHandler("set_voices", self.set_voices))
        
        # Команды для управления сервисами
        app.add_handler(CommandHandler("restart_bot", self.restart_bot))
        app.add_handler(CommandHandler("restart_tg", self.restart_tg))
        app.add_handler(CommandHandler("status", self.service_status))
        app.add_handler(CommandHandler("logs", self.service_logs))
        
        # Мониторинг и диагностика
        app.add_handler(CommandHandler("stats", self.system_stats))
        app.add_handler(CommandHandler("watch", self.watch_logs))
        app.add_handler(CommandHandler("profile", self.profile_manager_control))
        app.add_handler(CommandHandler("diagnose", self.full_diagnose))
        
        # Обработчик инлайн-кнопок
        app.add_handler(CallbackQueryHandler(self.button_callback))

        logging.info("🤖 Telegram Admin Bot started with enhanced features")
        app.run_polling()


def main():
    """Точка входа"""
    tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    admins = os.getenv("ADMIN_USER_IDS", "")

    if not tg_token:
        raise SystemExit("❌ Set TELEGRAM_BOT_TOKEN environment variable")

    if not admins:
        raise SystemExit(
            "❌ Set ADMIN_USER_IDS environment variable (comma-separated)"
        )

    admin_ids = [int(x.strip()) for x in admins.split(",") if x.strip()]
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
    
    # Проверка наличия sudo прав
    if not ServiceManager.check_sudo_permissions():
        logging.warning(
            "⚠️ Нет прав sudo без пароля! Команды управления сервисами будут недоступны.\n"
            "Добавьте в sudoers: username ALL=(ALL) NOPASSWD: /usr/bin/systemctl, /usr/bin/journalctl"
        )
    
    # Здесь profile_manager не передается, потому что это отдельный запуск
    # Для работы с profile_manager нужно использовать основной main.py
    TelegramAdmin(tg_token, admin_ids, config_path).run()


if __name__ == "__main__":
    main()
