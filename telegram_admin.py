# -*- coding: utf-8 -*-
"""
Telegram админ-бот для управления токенами и сервисами.
"""
import sys
import os
import json
import logging
import time
import asyncio
from collections import defaultdict
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

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
from telegram.constants import ParseMode

from buffguild.constants import RACE_NAMES

# Настройка логирования
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Константы - отдельные службы
BUFFGUILD_SERVICE = "buffguild.service"  # Основной бот VK
TELEGRAM_SERVICE = "telegram-bot.service"  # Этот Telegram админ-бот
ALLOWED_SERVICES = {BUFFGUILD_SERVICE, TELEGRAM_SERVICE}

# Классы персонажей
CLASS_CHOICES = {
    "apostle": "Апостол",
    "warlock": "Чернокнижник",
    "crusader": "Крестоносец",
    "light_incarnation": "Воплощение света",
}


class ConversationState(Enum):
    """Состояния для диалогов"""
    WAIT_NAME = 1
    WAIT_CLASS = 2
    WAIT_TOKEN = 3
    WAIT_CHAT = 4
    WAIT_VOICES = 5
    WAIT_RACES = 6


@dataclass
class CommandRateLimit:
    """Rate limiting для команд"""
    max_calls: int
    period: int
    calls: Dict[int, List[float]] = field(default_factory=lambda: defaultdict(list))
    
    def is_allowed(self, user_id: int) -> Tuple[bool, Optional[int]]:
        now = time.time()
        # Очищаем старые вызовы
        self.calls[user_id] = [t for t in self.calls[user_id] if now - t < self.period]
        
        if len(self.calls[user_id]) >= self.max_calls:
            oldest = min(self.calls[user_id]) if self.calls[user_id] else now
            wait_until = oldest + self.period
            wait_seconds = int(wait_until - now)
            return False, max(1, wait_seconds)
        
        self.calls[user_id].append(now)
        return True, None


class ServiceManager:
    """Класс для безопасного управления systemd сервисами"""
    
    _restart_locks: Dict[str, asyncio.Lock] = {}
    _last_restart: Dict[str, float] = {}
    
    _rate_limits = {
        'restart': CommandRateLimit(max_calls=2, period=60),
        'status': CommandRateLimit(max_calls=10, period=60),
        'logs': CommandRateLimit(max_calls=5, period=60),
    }
    
    @classmethod
    def _get_lock(cls, service_name: str) -> asyncio.Lock:
        if service_name not in cls._restart_locks:
            cls._restart_locks[service_name] = asyncio.Lock()
        return cls._restart_locks[service_name]
    
    @classmethod
    async def _run_command(
        cls, 
        cmd: List[str], 
        timeout: int = 30,
        check_service: bool = True
    ) -> Tuple[bool, str, str]:
        """Безопасно выполняет команду с таймаутом"""
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), 
                timeout=timeout
            )
            
            success = process.returncode == 0
            return success, stdout.decode('utf-8', errors='ignore'), stderr.decode('utf-8', errors='ignore')
            
        except asyncio.TimeoutError:
            try:
                process.kill()
            except:
                pass
            return False, "", f"Timeout after {timeout} seconds"
        except Exception as e:
            return False, "", str(e)
    
    @classmethod
    async def restart_service(cls, service_name: str, user_id: int) -> Tuple[bool, str]:
        allowed, wait = cls._rate_limits['restart'].is_allowed(user_id)
        if not allowed:
            return False, f"❌ Слишком частые перезапуски. Подождите {wait} секунд."
        
        if service_name not in ALLOWED_SERVICES:
            return False, f"❌ Сервис {service_name} не разрешен"
        
        async with cls._get_lock(service_name):
            now = time.time()
            if service_name in cls._last_restart:
                if now - cls._last_restart[service_name] < 10:
                    return False, f"❌ Сервис {service_name} уже перезапускался менее 10 секунд назад"
            
            success, stdout, stderr = await cls._run_command(
                ["sudo", "systemctl", "restart", service_name],
                timeout=30
            )
            
            if success:
                cls._last_restart[service_name] = now
                return True, f"✅ Сервис {service_name} успешно перезапущен"
            else:
                return False, f"❌ Ошибка перезапуска {service_name}:\n{stderr[:200]}"
    
    @classmethod
    async def get_service_status(cls, service_name: str, user_id: int) -> Dict[str, Any]:
        if service_name not in ALLOWED_SERVICES:
            return {'error': f'Service {service_name} not allowed', 'active': False}
        
        allowed, wait = cls._rate_limits['status'].is_allowed(user_id)
        if not allowed:
            return {'error': f'Rate limited. Wait {wait}s', 'active': False}
        
        # Проверяем, активен ли сервис
        success, stdout, stderr = await cls._run_command(
            ["systemctl", "is-active", service_name],
            timeout=10
        )
        is_active = success and stdout.strip() == "active"
        
        # Получаем детальный статус
        success, stdout, stderr = await cls._run_command(
            ["systemctl", "status", service_name],
            timeout=10
        )
        
        status_text = stdout if success else stderr
        pid = None
        memory = None
        cpu = None
        
        for line in status_text.split('\n'):
            if 'Main PID:' in line:
                parts = line.split('Main PID:')[1].strip().split()
                pid = parts[0] if parts else None
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
        }
    
    @classmethod
    async def get_logs(cls, service_name: str, lines: int = 50, user_id: int = 0) -> str:
        if service_name not in ALLOWED_SERVICES:
            return f"❌ Сервис {service_name} не разрешен"
        
        allowed, wait = cls._rate_limits['logs'].is_allowed(user_id)
        if not allowed:
            return f"❌ Слишком частые запросы логов. Подождите {wait} секунд."
        
        lines = max(10, min(lines, 500))
        
        success, stdout, stderr = await cls._run_command(
            ["sudo", "journalctl", "-u", service_name, "-n", str(lines)],
            timeout=15
        )
        
        if success:
            return stdout
        else:
            return f"❌ Ошибка получения логов:\n{stderr[:500]}"
    
    @classmethod
    async def check_sudo_permissions(cls) -> Tuple[bool, str]:
        success, stdout, stderr = await cls._run_command(
            ["sudo", "-n", "true"],
            timeout=5,
            check_service=False
        )
        if success:
            return True, "✅ Права sudo настроены"
        else:
            return False, "❌ Нет прав sudo без пароля"


class ConfigManager:
    """Менеджер для работы с конфигурационным файлом"""
    
    def __init__(self, config_path: str, cache_ttl: int = 5):
        self.config_path = config_path
        self.cache_ttl = cache_ttl
        self._cache: Optional[Dict[str, Any]] = None
        self._cache_time: float = 0
        self._lock = asyncio.Lock()
    
    async def load(self, force: bool = False) -> Tuple[bool, Optional[Dict[str, Any]], str]:
        async with self._lock:
            now = time.time()
            
            if not force and self._cache and (now - self._cache_time) < self.cache_ttl:
                return True, self._cache.copy(), "OK (cached)"
            
            if not os.path.exists(self.config_path):
                return True, {"tokens": [], "settings": {"delay": 2}}, "Config not found, created default"
            
            try:
                with open(self.config_path, "r", encoding="utf-8") as f:
                    self._cache = json.load(f)
                    self._cache_time = now
                logger.info(f"✅ Config loaded: {len(self._cache.get('tokens', []))} tokens")
                return True, self._cache.copy(), "OK"
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in config: {e}")
                return False, None, f"Invalid JSON: {e}"
            except Exception as e:
                logger.error(f"Error loading config: {e}")
                return False, None, str(e)
    
    async def save(self, cfg: Dict[str, Any]) -> Tuple[bool, str]:
        async with self._lock:
            temp_path = self.config_path + ".tmp"
            
            try:
                with open(temp_path, "w", encoding="utf-8") as f:
                    json.dump(cfg, f, ensure_ascii=False, indent=2)
                os.replace(temp_path, self.config_path)
                self._cache = cfg.copy()
                self._cache_time = time.time()
                logger.info(f"✅ Config saved: {len(cfg.get('tokens', []))} tokens")
                return True, "OK"
            except Exception as e:
                logger.error(f"Error saving config: {e}")
                return False, str(e)
            finally:
                if os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                    except:
                        pass


class TokenFormatter:
    """Форматирование информации о токенах"""
    
    @staticmethod
    def format_short(token: Dict, index: int = None) -> str:
        prefix = f"{index}. " if index else ""
        cls = token.get("class", "apostle")
        cls_name = CLASS_CHOICES.get(cls, cls)
        status = "✅" if token.get("enabled", True) else "🚫"
        voices = token.get("voices", "?")
        manual = "⚠️" if token.get("needs_manual_voices", False) else ""
        
        return (
            f"{prefix}**{token.get('name', token['id'])}**\n"
            f"  🎭 {cls_name} {status} 🔊 {voices} {manual}"
        )
    
    @staticmethod
    def format_detailed(token: Dict) -> str:
        temp_races = []
        for tr in token.get("temp_races", []):
            expires = tr.get("expires", 0)
            if expires > time.time():
                remaining = int(expires - time.time())
                hours = remaining // 3600
                minutes = (remaining % 3600) // 60
                temp_races.append(f"{tr['race']} ({hours}ч {minutes}м)")
        
        total = token.get("total_attempts", 0)
        success = token.get("successful_buffs", 0)
        success_rate = (success / total * 100) if total > 0 else 0
        
        captcha_until = token.get("captcha_until", 0)
        captcha_status = "нет"
        if captcha_until > time.time():
            remaining = int(captcha_until - time.time())
            minutes = remaining // 60
            captcha_status = f"⚠️ капча до {time.ctime(captcha_until)} (осталось {minutes} мин)"
        
        return (
            f"🔍 **Информация о токене: {token.get('name')}**\n\n"
            f"**Основное:**\n"
            f"• ID: `{token.get('id')}`\n"
            f"• Класс: {CLASS_CHOICES.get(token.get('class'), token.get('class'))}\n"
            f"• Статус: {'✅ Активен' if token.get('enabled', True) else '❌ Отключен'}\n"
            f"• Владелец VK: {token.get('owner_vk_id', 0)}\n"
            f"• Уровень: {token.get('level', 0)}\n\n"
            f"**Голоса:**\n"
            f"• Текущие: {token.get('voices', 0)}\n"
            f"• Нужен ручной ввод: {'⚠️ Да' if token.get('needs_manual_voices', False) else '✅ Нет'}\n\n"
            f"**Расы:**\n"
            f"• Постоянные: {', '.join(token.get('races', [])) or 'нет'}\n"
            f"• Временные: {', '.join(temp_races) or 'нет'}\n\n"
            f"**Статистика:**\n"
            f"• Успешных бафов: {success}/{total} ({success_rate:.1f}%)\n"
            f"• Капча: {captcha_status}"
        )


class TelegramAdmin:
    """Telegram бот для управления токенами и сервисами"""

    def __init__(
        self, 
        telegram_token: str, 
        admin_ids: List[int], 
        config_path: str, 
        bot_instance=None,
        profile_manager=None
    ):
        self.telegram_token = telegram_token
        self.admin_ids = set(admin_ids)
        self.bot_instance = bot_instance
        self.profile_manager = profile_manager
        self.game_chat_id = -183040898
        
        self.tmp: Dict[int, Dict[str, Any]] = {}
        self.config_manager = ConfigManager(config_path)
        self.token_formatter = TokenFormatter()
        
        self.rate_limiters = {
            'service': CommandRateLimit(max_calls=5, period=60),
            'token': CommandRateLimit(max_calls=20, period=60),
            'info': CommandRateLimit(max_calls=30, period=60),
        }
        
        self._sudo_cache: Optional[Tuple[bool, str, float]] = None
        self._sudo_cache_ttl = 300

    def is_admin(self, uid: int) -> bool:
        return uid in self.admin_ids

    def _normalize_token_name(self, name: str) -> str:
        return name.strip().lower()
    
    def _find_token_by_name(self, tokens: List[Dict], name: str) -> Optional[Dict]:
        normalized = self._normalize_token_name(name)
        for token in tokens:
            if self._normalize_token_name(token.get("name", "")) == normalized:
                return token
        return None
    
    def _find_and_modify_token(self, tokens: List[Dict], name: str, modifier) -> Tuple[bool, int, Optional[Dict]]:
        normalized = self._normalize_token_name(name)
        changed_count = 0
        modified_token = None
        
        for token in tokens:
            if self._normalize_token_name(token.get("name", "")) == normalized:
                old_values = token.copy()
                modifier(token)
                changed_count += 1
                modified_token = token.copy()
                modified_token['old_values'] = old_values
        
        return changed_count > 0, changed_count, modified_token

    async def _check_rate_limit(self, update: Update, command: str) -> bool:
        uid = update.effective_user.id
        limiter = self.rate_limiters['token']
        if command in ['restart_bot', 'restart_tg', 'status', 'logs', 'watch']:
            limiter = self.rate_limiters['service']
        elif command in ['token_info', 'set_voices', 'stats', 'diagnose']:
            limiter = self.rate_limiters['info']
        
        allowed, wait = limiter.is_allowed(uid)
        if not allowed:
            await update.message.reply_text(f"⏳ Слишком много запросов. Подождите {wait} секунд.")
            return False
        return True

    # ============= СТАРТ =============
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return

        sudo_status, sudo_message = await self._get_sudo_status()
        pm_status = "✅ Доступен" if self.profile_manager else "❌ Не инициализирован"
        
        msg = (
            "🤖 **Blessing Bot Admin Panel**\n\n"
            "📋 **Команды управления токенами:**\n"
            "/addtoken — добавить токен\n"
            "/listtokens — список токенов\n"
            "/enable — включить токен\n"
            "/disable — отключить токен\n"
            "/remove — удалить токен\n"
            "/reload — перезагрузить конфиг\n"
            "/tokeninfo — детальная информация о токене\n"
            "/setvoices — установить голоса\n\n"
            "🛠 **Команды управления сервисами:**\n"
            f"/restart_bot — перезапустить {BUFFGUILD_SERVICE}\n"
            f"/restart_tg — перезапустить {TELEGRAM_SERVICE}\n"
            "/status — статус сервисов\n"
            f"/logs — последние логи {BUFFGUILD_SERVICE}\n"
            "/watch — слежение за логами\n\n"
            "📊 **Мониторинг и диагностика:**\n"
            "/stats — общая статистика системы\n"
            "/profile — управление ProfileManager\n"
            "/diagnose — полная диагностика системы\n\n"
            f"🔐 **Права sudo:** {sudo_message}\n"
            f"📊 **ProfileManager:** {pm_status}"
        )
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    async def _get_sudo_status(self) -> Tuple[bool, str]:
        now = time.time()
        if self._sudo_cache and (now - self._sudo_cache[2]) < self._sudo_cache_ttl:
            return self._sudo_cache[0], self._sudo_cache[1]
        
        success, message = await ServiceManager.check_sudo_permissions()
        self._sudo_cache = (success, message, now)
        return success, message

    # ============= ADD TOKEN =============
    async def add_token(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return ConversationHandler.END

        logger.info(f"📝 Starting add_token for user {uid}")
        self.tmp[uid] = {}
        await update.message.reply_text(
            "➕ **Добавление токена**\n\n"
            "📝 Шаг 1/6: Введите имя токена\n"
            "Например: `Main`, `Backup1`, `Reserve`",
            parse_mode=ParseMode.MARKDOWN
        )
        return ConversationState.WAIT_NAME.value

    async def recv_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        name = update.message.text.strip()
        
        if len(name) < 2:
            await update.message.reply_text("❌ Слишком короткое имя. Минимум 2 символа.")
            return ConversationState.WAIT_NAME.value
        if len(name) > 50:
            await update.message.reply_text("❌ Имя слишком длинное. Максимум 50 символов.")
            return ConversationState.WAIT_NAME.value

        self.tmp[uid]["name"] = name
        classes = "\n".join([f"`{k}` — {v}" for k, v in CLASS_CHOICES.items()])
        await update.message.reply_text(
            f"✅ Имя: **{name}**\n\n"
            f"🎭 Шаг 2/6: Выберите класс\n\n"
            f"{classes}\n\n"
            f"Отправьте код класса (например: `apostle`)",
            parse_mode=ParseMode.MARKDOWN
        )
        return ConversationState.WAIT_CLASS.value

    async def recv_class(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        cls = update.message.text.strip().lower()
        
        if cls not in CLASS_CHOICES:
            await update.message.reply_text(
                f"❌ Неизвестный класс: `{cls}`\n\n"
                f"Доступные: {', '.join(f'`{k}`' for k in CLASS_CHOICES.keys())}",
                parse_mode=ParseMode.MARKDOWN
            )
            return ConversationState.WAIT_CLASS.value

        self.tmp[uid]["class"] = cls
        class_name = CLASS_CHOICES[cls]
        await update.message.reply_text(
            f"✅ Класс: **{class_name}**\n\n"
            f"🔑 Шаг 3/6: Отправьте VK access token\n"
            f"Токен должен начинаться с `vk1.a.`",
            parse_mode=ParseMode.MARKDOWN
        )
        return ConversationState.WAIT_TOKEN.value

    async def recv_token(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        token = update.message.text.strip()
        
        if not token.startswith(("vk1.a.", "vk1.")):
            await update.message.reply_text(
                "❌ Неверный формат токена. Должен начинаться с `vk1.a.` или `vk1.`",
                parse_mode=ParseMode.MARKDOWN
            )
            return ConversationState.WAIT_TOKEN.value
        
        if len(token) < 50:
            await update.message.reply_text("❌ Токен слишком короткий. Убедитесь, что скопировали полностью.")
            return ConversationState.WAIT_TOKEN.value

        self.tmp[uid]["access_token"] = token
        await update.message.reply_text(
            "✅ Токен сохранён\n\n"
            "📁 Шаг 4/6: **ID чата** (source_chat_id)\n"
            "Например: `48` или `120`",
            parse_mode=ParseMode.MARKDOWN
        )
        return ConversationState.WAIT_CHAT.value

    async def recv_chat(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        try:
            chat_id = int(update.message.text.strip())
            if chat_id <= 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Нужно положительное число.")
            return ConversationState.WAIT_CHAT.value

        self.tmp[uid]["source_chat_id"] = chat_id
        await update.message.reply_text(
            f"✅ ID чата: `{chat_id}`\n\n"
            f"🔊 Шаг 5/6: Введите стартовое количество голосов для токена\n"
            f"Например: `27`",
            parse_mode=ParseMode.MARKDOWN
        )
        return ConversationState.WAIT_VOICES.value

    async def recv_voices(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        text = update.message.text.strip()
        try:
            voices = int(text)
            if voices < 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Нужно неотрицательное число голосов.")
            return ConversationState.WAIT_VOICES.value

        self.tmp[uid]["voices"] = voices

        cls = self.tmp[uid].get("class")
        if cls == "apostle":
            await update.message.reply_text(
                f"✅ Голоса: **{voices}**\n\n"
                f"🎭 Шаг 6/6: Укажите основные расы для апостола\n"
                f"Формат: буквы через запятую, например: `ч,г`\n"
                f"Доступные расы: `ч,г,н,э,м,д,о`",
                parse_mode=ParseMode.MARKDOWN
            )
            return ConversationState.WAIT_RACES.value

        return await self._finalize_token_creation(uid, update)

    async def recv_races(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        text = update.message.text.strip().replace(" ", "")
        text = text.replace(";", ",")
        race_keys_raw = [r for r in text.split(",") if r]

        if not race_keys_raw:
            await update.message.reply_text("❌ Не указаны расы. Введите, например: `ч,г`", parse_mode=ParseMode.MARKDOWN)
            return ConversationState.WAIT_RACES.value

        seen = set()
        race_keys: List[str] = []
        for rk in race_keys_raw:
            if rk in seen:
                await update.message.reply_text(f"❌ Нельзя указывать одну и ту же расу несколько раз (`{rk}`).", parse_mode=ParseMode.MARKDOWN)
                return ConversationState.WAIT_RACES.value
            seen.add(rk)
            race_keys.append(rk)

        for rk in race_keys:
            if rk not in RACE_NAMES:
                await update.message.reply_text(
                    f"❌ Неизвестная раса `{rk}`. Допустимые: `{', '.join(RACE_NAMES.keys())}`",
                    parse_mode=ParseMode.MARKDOWN
                )
                return ConversationState.WAIT_RACES.value

        self.tmp[uid]["races"] = race_keys
        return await self._finalize_token_creation(uid, update)

    async def _finalize_token_creation(self, uid: int, update: Update):
        data = self.tmp.get(uid, {})
        logger.info(f"📝 Finalizing token creation for {uid}: {data.get('name')}")
        
        success, cfg, error = await self.config_manager.load()
        if not success:
            await update.message.reply_text(f"❌ Ошибка загрузки конфига: {error}")
            return ConversationHandler.END
        
        if not cfg or "tokens" not in cfg:
            cfg = {"tokens": [], "settings": {"delay": 2}}
        
        existing = self._find_token_by_name(cfg.get("tokens", []), data["name"])
        if existing:
            await update.message.reply_text(
                f"❌ Токен с именем **{data['name']}** уже существует!\n"
                f"Используйте другое имя.",
                parse_mode=ParseMode.MARKDOWN
            )
            self.tmp.pop(uid, None)
            return ConversationHandler.END

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
            "target_peer_id": self.game_chat_id,
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

        cfg.setdefault("tokens", []).append(new_token)
        cfg.setdefault("settings", {}).setdefault("delay", 2)
        
        save_success, save_error = await self.config_manager.save(cfg)
        if not save_success:
            await update.message.reply_text(f"❌ Ошибка сохранения конфига: {save_error}")
            return ConversationHandler.END

        if self.bot_instance and hasattr(self.bot_instance, "tm"):
            self.bot_instance.tm.reload()
            logger.info("🔄 TokenManager.reload() после добавления токена")

        self.tmp.pop(uid, None)

        class_name = CLASS_CHOICES[new_token["class"]]
        races_str = ", ".join(races) if races else "-"

        message = (
            "✅ **Токен добавлен!**\n\n"
            f"📛 Имя: **{new_token['name']}**\n"
            f"🎭 Класс: **{class_name}**\n"
            f"🆔 ID: `{token_id}`\n"
            f"📁 Chat: `{new_token['source_chat_id']}`\n"
            f"🎯 Target: `{self.game_chat_id}`\n"
            f"🔊 Голосов: **{voices}**\n"
            f"🧬 Основные расы: **{races_str}**\n"
            f"✅ Статус: **Активен**"
        )
        await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
        return ConversationHandler.END

    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        self.tmp.pop(uid, None)
        await update.message.reply_text("❌ Отменено.")
        return ConversationHandler.END

    # ============= LIST TOKENS =============
    async def list_tokens(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return

        logger.info(f"📋 Listing tokens for user {uid}")
        
        success, cfg, error = await self.config_manager.load()
        if not success:
            await update.message.reply_text(f"❌ Ошибка загрузки конфига: {error}")
            return

        if not cfg or "tokens" not in cfg:
            await update.message.reply_text("❌ Конфиг поврежден: нет секции tokens")
            return

        tokens = cfg.get("tokens", [])
        if not tokens:
            await update.message.reply_text("📭 Нет токенов.")
            return

        page = int(context.args[0]) if context.args and context.args[0].isdigit() else 1
        page_size = 5
        start = (page - 1) * page_size
        end = start + page_size
        current_page = tokens[start:end]

        if not current_page:
            await update.message.reply_text(f"❌ Страница {page} пуста")
            return

        total_pages = (len(tokens) - 1) // page_size + 1
        lines = [f"📋 **Список токенов (страница {page}/{total_pages}):**\n"]
        for i, t in enumerate(current_page, start=start+1):
            lines.append(self.token_formatter.format_short(t, i))

        keyboard = []
        nav_buttons = []
        if start > 0:
            nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"list_page_{page-1}"))
        if end < len(tokens):
            nav_buttons.append(InlineKeyboardButton("Вперед ▶️", callback_data=f"list_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
        
        await update.message.reply_text(
            "\n\n".join(lines), 
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )

    async def list_tokens_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        uid = query.from_user.id
        if not self.is_admin(uid):
            await query.edit_message_text("❌ Нет прав.")
            return
        
        page = int(query.data.split('_')[-1])
        
        success, cfg, error = await self.config_manager.load()
        if not success:
            await query.edit_message_text(f"❌ Ошибка загрузки конфига: {error}")
            return
        
        tokens = cfg.get("tokens", [])
        page_size = 5
        start = (page - 1) * page_size
        end = start + page_size
        current_page = tokens[start:end]
        total_pages = (len(tokens) - 1) // page_size + 1

        lines = [f"📋 **Список токенов (страница {page}/{total_pages}):**\n"]
        for i, t in enumerate(current_page, start=start+1):
            lines.append(self.token_formatter.format_short(t, i))

        keyboard = []
        nav_buttons = []
        if start > 0:
            nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"list_page_{page-1}"))
        if end < len(tokens):
            nav_buttons.append(InlineKeyboardButton("Вперед ▶️", callback_data=f"list_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
        
        await query.edit_message_text(
            "\n\n".join(lines),
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )

    # ============= TOKEN INFO =============
    async def token_info(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return
        
        if not context.args:
            await update.message.reply_text(
                "❌ Использование: `/tokeninfo имя_токена`\n"
                "Например: `/tokeninfo Main`",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        name = " ".join(context.args)
        logger.info(f"🔍 Token info for '{name}' from user {uid}")
        
        success, cfg, error = await self.config_manager.load()
        if not success:
            await update.message.reply_text(f"❌ Ошибка загрузки конфига: {error}")
            return
        
        token = self._find_token_by_name(cfg.get("tokens", []), name)
        
        if token:
            info_msg = self.token_formatter.format_detailed(token)
            await update.message.reply_text(info_msg, parse_mode=ParseMode.MARKDOWN)
        else:
            tokens = cfg.get("tokens", [])
            similar = [t.get("name") for t in tokens if name.lower() in t.get("name", "").lower()]
            similar_msg = f"\n\nПохожие: {', '.join(similar[:3])}" if similar else ""
            await update.message.reply_text(f"❌ Токен '{name}' не найден.{similar_msg}")

    # ============= SET VOICES =============
    async def set_voices(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return
        
        if len(context.args) < 2:
            await update.message.reply_text(
                "❌ Использование: `/setvoices имя_токена количество`\n"
                "Например: `/setvoices Main 25`",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        name = context.args[0]
        try:
            voices = int(context.args[1])
            if voices < 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Количество голосов должно быть положительным числом")
            return
        
        logger.info(f"🎤 Set voices for '{name}' to {voices} by user {uid}")
        
        success, cfg, error = await self.config_manager.load()
        if not success:
            await update.message.reply_text(f"❌ Ошибка загрузки конфига: {error}")
            return
        
        found, changed, token = self._find_and_modify_token(
            cfg.get("tokens", []),
            name,
            lambda t: t.update({"voices": voices, "needs_manual_voices": False})
        )
        
        if found:
            old_voices = token.get('old_values', {}).get('voices', '?')
            save_success, save_error = await self.config_manager.save(cfg)
            if save_success:
                await update.message.reply_text(
                    f"✅ Голоса для **{token['name']}** изменены: {old_voices} → {voices}\n"
                    f"📌 Статус ручного ввода сброшен",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await update.message.reply_text(f"❌ Ошибка сохранения: {save_error}")
        else:
            tokens = cfg.get("tokens", [])
            similar = [t.get("name") for t in tokens if name.lower() in t.get("name", "").lower()]
            similar_msg = f"\n\nПохожие: {', '.join(similar[:3])}" if similar else ""
            await update.message.reply_text(f"❌ Токен '{name}' не найден.{similar_msg}")

    # ============= ENABLE/DISABLE/REMOVE =============
    async def enable(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return

        if not context.args:
            await update.message.reply_text(
                "❌ Использование: `/enable имя_токена`\n"
                "Например: `/enable Main`",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        name = " ".join(context.args)
        logger.info(f"✅ Enabling token '{name}' by user {uid}")
        
        success, cfg, error = await self.config_manager.load()
        if not success:
            await update.message.reply_text(f"❌ Ошибка загрузки конфига: {error}")
            return
        
        found, changed, token = self._find_and_modify_token(
            cfg.get("tokens", []),
            name,
            lambda t: t.update({"enabled": True})
        )
        
        if found:
            save_success, save_error = await self.config_manager.save(cfg)
            if save_success:
                await update.message.reply_text(f"✅ Токен **{token['name']}** включён", parse_mode=ParseMode.MARKDOWN)
            else:
                await update.message.reply_text(f"❌ Ошибка сохранения: {save_error}")
        else:
            tokens = cfg.get("tokens", [])
            similar = [t.get("name") for t in tokens if name.lower() in t.get("name", "").lower()]
            similar_msg = f"\n\nПохожие: {', '.join(similar[:3])}" if similar else ""
            await update.message.reply_text(f"❌ Токен '{name}' не найден.{similar_msg}")

    async def disable(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return

        if not context.args:
            await update.message.reply_text(
                "❌ Использование: `/disable имя_токена`\n"
                "Например: `/disable Main`",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        name = " ".join(context.args)
        logger.info(f"🚫 Disabling token '{name}' by user {uid}")
        
        success, cfg, error = await self.config_manager.load()
        if not success:
            await update.message.reply_text(f"❌ Ошибка загрузки конфига: {error}")
            return
        
        found, changed, token = self._find_and_modify_token(
            cfg.get("tokens", []),
            name,
            lambda t: t.update({"enabled": False})
        )
        
        if found:
            save_success, save_error = await self.config_manager.save(cfg)
            if save_success:
                await update.message.reply_text(f"🚫 Токен **{token['name']}** отключён", parse_mode=ParseMode.MARKDOWN)
            else:
                await update.message.reply_text(f"❌ Ошибка сохранения: {save_error}")
        else:
            tokens = cfg.get("tokens", [])
            similar = [t.get("name") for t in tokens if name.lower() in t.get("name", "").lower()]
            similar_msg = f"\n\nПохожие: {', '.join(similar[:3])}" if similar else ""
            await update.message.reply_text(f"❌ Токен '{name}' не найден.{similar_msg}")

    async def remove(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return

        if not context.args:
            await update.message.reply_text(
                "❌ Использование: `/remove имя_токена`\n"
                "Например: `/remove Main`",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        name = " ".join(context.args)
        logger.info(f"🗑️ Removing token '{name}' by user {uid}")
        
        success, cfg, error = await self.config_manager.load()
        if not success:
            await update.message.reply_text(f"❌ Ошибка загрузки конфига: {error}")
            return
        
        normalized = self._normalize_token_name(name)
        before = len(cfg.get("tokens", []))
        
        removed_token = None
        for t in cfg.get("tokens", []):
            if self._normalize_token_name(t.get("name", "")) == normalized:
                removed_token = t.copy()
                break
        
        cfg["tokens"] = [t for t in cfg.get("tokens", []) 
                        if self._normalize_token_name(t.get("name", "")) != normalized]
        after = len(cfg["tokens"])

        if after < before:
            save_success, save_error = await self.config_manager.save(cfg)
            if save_success:
                await update.message.reply_text(f"🗑️ Токен **{removed_token['name'] if removed_token else name}** удалён", parse_mode=ParseMode.MARKDOWN)
            else:
                await update.message.reply_text(f"❌ Ошибка сохранения: {save_error}")
        else:
            tokens = cfg.get("tokens", [])
            similar = [t.get("name") for t in tokens if name.lower() in t.get("name", "").lower()]
            similar_msg = f"\n\nПохожие: {', '.join(similar[:3])}" if similar else ""
            await update.message.reply_text(f"❌ Токен '{name}' не найден.{similar_msg}")

    # ============= RELOAD =============
    async def reload_config(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return

        logger.info(f"🔄 Reloading config by user {uid}")
        
        success, cfg, error = await self.config_manager.load(force=True)
        
        if not success:
            await update.message.reply_text(f"❌ Ошибка перезагрузки конфига: {error}")
            return
        
        token_count = len(cfg.get("tokens", [])) if cfg else 0
        
        if self.bot_instance and hasattr(self.bot_instance, "tm"):
            self.bot_instance.tm.reload()
            await update.message.reply_text(
                f"🔄 Конфигурация перечитана с диска (**{token_count}** токенов) и VK бот перезагружен",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await update.message.reply_text(
                f"🔄 Конфигурация перечитана с диска (**{token_count}** токенов)",
                parse_mode=ParseMode.MARKDOWN
            )

    # ============= SERVICE COMMANDS =============
    async def restart_bot(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return
        
        keyboard = [[InlineKeyboardButton("✅ Да, перезапустить", callback_data="confirm_restart_bot"),
                     InlineKeyboardButton("❌ Отмена", callback_data="cancel_restart")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            f"⚠️ **Подтвердите действие**\n\n"
            f"Вы действительно хотите перезапустить {BUFFGUILD_SERVICE}?",
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )

    async def restart_tg(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return
        
        keyboard = [[InlineKeyboardButton("✅ Да, перезапустить", callback_data="confirm_restart_tg"),
                     InlineKeyboardButton("❌ Отмена", callback_data="cancel_restart")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            f"⚠️ **Подтвердите действие**\n\n"
            f"Вы действительно хотите перезапустить {TELEGRAM_SERVICE}?",
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )

    async def service_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return
        
        status_msg = await update.message.reply_text("🔄 Получаю статус сервисов...")
        
        bot_status_task = ServiceManager.get_service_status(BUFFGUILD_SERVICE, uid)
        tg_status_task = ServiceManager.get_service_status(TELEGRAM_SERVICE, uid)
        
        bot_status, tg_status = await asyncio.gather(bot_status_task, tg_status_task)
        
        if 'error' in bot_status:
            await status_msg.edit_text(f"❌ {bot_status['error']}")
            return
        
        keyboard = [
            [InlineKeyboardButton("🔄 Перезапустить VK бота", callback_data="restart_bot"),
             InlineKeyboardButton("🔄 Перезапустить TG бота", callback_data="restart_tg")],
            [InlineKeyboardButton("📋 Логи VK бота", callback_data="logs_bot"),
             InlineKeyboardButton("📋 Логи TG бота", callback_data="logs_tg")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        status_text = (
            "📊 **СТАТУС СЕРВИСОВ**\n\n"
            f"**{BUFFGUILD_SERVICE} (VK бот)**\n"
            f"Активен: {'✅' if bot_status['active'] else '❌'}\n"
            f"PID: {bot_status['pid'] or 'N/A'}\n"
            f"Память: {bot_status['memory'] or 'N/A'}\n"
            f"CPU: {bot_status['cpu'] or 'N/A'}\n\n"
            f"**{TELEGRAM_SERVICE} (Telegram админ)**\n"
            f"Активен: {'✅' if tg_status['active'] else '❌'}\n"
            f"PID: {tg_status['pid'] or 'N/A'}\n"
            f"Память: {tg_status['memory'] or 'N/A'}\n"
            f"CPU: {tg_status['cpu'] or 'N/A'}"
        )
        
        await status_msg.edit_text(status_text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)

    async def service_logs(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return
        
        lines = 50
        if context.args and context.args[0].isdigit():
            lines = min(int(context.args[0]), 500)
        
        status_msg = await update.message.reply_text(f"📋 Получаю последние {lines} строк логов VK бота...")
        
        logs = await ServiceManager.get_logs(BUFFGUILD_SERVICE, lines, uid)
        
        if logs.startswith("❌"):
            await status_msg.edit_text(logs)
            return
        
        if len(logs) > 4000:
            await status_msg.delete()
            for i in range(0, len(logs), 4000):
                part = logs[i:i+4000]
                await update.message.reply_text(f"```\n{part}\n```", parse_mode=ParseMode.MARKDOWN)
        else:
            await status_msg.edit_text(f"```\n{logs}\n```", parse_mode=ParseMode.MARKDOWN)

    async def watch_logs(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return
        
        context.user_data['watching'] = True
        context.user_data['last_logs'] = ""
        context.user_data['watch_message_id'] = None
        context.user_data['watch_chat_id'] = update.effective_chat.id
        
        keyboard = [[InlineKeyboardButton("🛑 Остановить", callback_data="stop_watching")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        msg = await update.message.reply_text(
            f"📋 **Режим наблюдения за логами {BUFFGUILD_SERVICE} активирован**\n"
            "Новые строки будут появляться каждые 10 секунд.",
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
        
        context.user_data['watch_message_id'] = msg.message_id
        asyncio.create_task(self._watch_logs_task(context, uid))

    async def _watch_logs_task(self, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        chat_id = context.user_data.get('watch_chat_id')
        message_id = context.user_data.get('watch_message_id')
        
        if not chat_id or not message_id:
            return
        
        consecutive_errors = 0
        
        while context.user_data.get('watching', False):
            try:
                logs = await ServiceManager.get_logs(BUFFGUILD_SERVICE, 20, user_id)
                
                if logs.startswith("❌ Слишком частые запросы"):
                    if consecutive_errors > 3:
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text="🛑 Автоматическая остановка из-за превышения лимитов запросов."
                        )
                        break
                    consecutive_errors += 1
                    await asyncio.sleep(30)
                    continue
                
                consecutive_errors = 0
                
                if logs != context.user_data.get('last_logs', ''):
                    context.user_data['last_logs'] = logs
                    display_logs = logs[-3500:] if len(logs) > 3500 else logs
                    
                    try:
                        await context.bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=message_id,
                            text=f"```\n{display_logs}\n```",
                            parse_mode=ParseMode.MARKDOWN,
                            reply_markup=InlineKeyboardMarkup([[
                                InlineKeyboardButton("🛑 Остановить", callback_data="stop_watching")
                            ]])
                        )
                    except Exception as e:
                        if "Message is not modified" not in str(e):
                            msg = await context.bot.send_message(
                                chat_id=chat_id,
                                text=f"```\n{display_logs}\n```",
                                parse_mode=ParseMode.MARKDOWN,
                                reply_markup=InlineKeyboardMarkup([[
                                    InlineKeyboardButton("🛑 Остановить", callback_data="stop_watching")
                                ]])
                            )
                            context.user_data['watch_message_id'] = msg.message_id
                            message_id = msg.message_id
                
                for _ in range(10):
                    if not context.user_data.get('watching', False):
                        break
                    await asyncio.sleep(1)
                    
            except Exception as e:
                logger.error(f"Ошибка в watch_logs_task: {e}")
                await asyncio.sleep(5)

    # ============= PROFILE MANAGER =============
    async def profile_manager_control(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        
        is_running = hasattr(self.profile_manager, '_running') and self.profile_manager._running
        
        keyboard = [
            [InlineKeyboardButton("▶️ Запустить", callback_data="pm_start"),
             InlineKeyboardButton("⏸️ Остановить", callback_data="pm_stop")],
            [InlineKeyboardButton("🔄 Перезапустить", callback_data="pm_restart"),
             InlineKeyboardButton("📊 Статус", callback_data="pm_status")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            f"**Управление ProfileManager**\n"
            f"Текущий статус: {'✅ Запущен' if is_running else '⏸️ Остановлен'}\n\n"
            f"Выберите действие:",
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )

    # ============= DIAGNOSE =============
    async def full_diagnose(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return
        
        status_msg = await update.message.reply_text("🔍 Запускаю диагностику...")
        
        bot_status_task = ServiceManager.get_service_status(BUFFGUILD_SERVICE, uid)
        tg_status_task = ServiceManager.get_service_status(TELEGRAM_SERVICE, uid)
        sudo_status_task = self._get_sudo_status()
        
        bot_status, tg_status, (sudo_ok, sudo_msg) = await asyncio.gather(
            bot_status_task, tg_status_task, sudo_status_task
        )
        
        vk_check = "❌ Нет доступа к VK боту"
        vk_error = ""
        if self.bot_instance and hasattr(self.bot_instance, 'tm') and self.bot_instance.tm:
            try:
                observer = self.bot_instance.tm.get_observer()
                if observer:
                    vk_check = "✅ OK (есть observer)"
                else:
                    vk_check = "⚠️ Observer не найден"
            except Exception as e:
                vk_check = "❌ Ошибка"
                vk_error = str(e)
        
        pm_check = "✅ Доступен" if self.profile_manager else "❌ Не инициализирован"
        pm_status = ""
        if self.profile_manager:
            is_running = hasattr(self.profile_manager, '_running') and self.profile_manager._running
            pm_status = f" ({'запущен' if is_running else 'остановлен'})"
        
        files_check = []
        for f in ["config.json", "jobs.json", "profile_manager_state.json"]:
            if os.path.exists(f):
                size = os.path.getsize(f) / 1024
                mtime = os.path.getmtime(f)
                age_hours = (time.time() - mtime) / 3600
                files_check.append(f"✅ {f} ({size:.1f} KB, изменён {age_hours:.1f} ч назад)")
            else:
                files_check.append(f"⚠️ {f} (не найден)")
        
        success, cfg, error = await self.config_manager.load()
        tokens = cfg.get("tokens", []) if success and cfg else []
        
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
        
        diag_msg = (
            "📋 **РЕЗУЛЬТАТЫ ДИАГНОСТИКИ**\n\n"
            f"**Сервисы:**\n"
            f"• {BUFFGUILD_SERVICE} (VK бот): {'✅' if bot_status.get('active') else '❌'}\n"
            f"• {TELEGRAM_SERVICE} (TG админ): {'✅' if tg_status.get('active') else '❌'}\n"
            f"• VK API: {vk_check}\n"
            f"{'  ' + vk_error if vk_error else ''}\n"
            f"• ProfileManager: {pm_check}{pm_status}\n\n"
            f"**Файлы:**\n" + "\n".join(files_check) + "\n\n"
            f"**Токены:**\n"
            f"• Всего: {len(tokens)}\n"
            f"• Общая успешность: {success_rate:.1f}% ({total_success}/{total_attempts})\n"
        )
        
        if tokens_with_issues:
            diag_msg += "• Проблемные:\n" + "\n".join(tokens_with_issues) + "\n"
        else:
            diag_msg += "• Все токены в порядке ✅\n"
        
        diag_msg += f"\n**Права sudo:** {sudo_msg}"
        
        await status_msg.edit_text(diag_msg, parse_mode=ParseMode.MARKDOWN)

    # ============= STATS =============
    async def system_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return
        
        status_msg = await update.message.reply_text("📊 Собираю статистику...")
        
        bot_status_task = ServiceManager.get_service_status(BUFFGUILD_SERVICE, uid)
        tg_status_task = ServiceManager.get_service_status(TELEGRAM_SERVICE, uid)
        
        bot_status, tg_status = await asyncio.gather(bot_status_task, tg_status_task)
        
        success, uname, _ = await ServiceManager._run_command(["uname", "-a"], check_service=False)
        success, uptime, _ = await ServiceManager._run_command(["uptime"], check_service=False)
        success, disk, _ = await ServiceManager._run_command(["df", "-h", "/"], check_service=False)
        success, memory, _ = await ServiceManager._run_command(["free", "-h"], check_service=False)
        
        success, cfg, error = await self.config_manager.load()
        tokens = cfg.get("tokens", []) if success and cfg else []
        enabled_tokens = sum(1 for t in tokens if t.get("enabled", True))
        total_voices = sum(t.get("voices", 0) for t in tokens)
        
        apostles = sum(1 for t in tokens if t.get("class") == "apostle")
        warlocks = sum(1 for t in tokens if t.get("class") == "warlock")
        paladins = sum(1 for t in tokens if t.get("class") in ["crusader", "light_incarnation"])
        
        stats_msg = (
            "📊 **СИСТЕМНАЯ СТАТИСТИКА**\n\n"
            f"**Сервисы:**\n"
            f"• {BUFFGUILD_SERVICE} (VK бот): {'✅' if bot_status.get('active') else '❌'}\n"
            f"• {TELEGRAM_SERVICE} (TG админ): {'✅' if tg_status.get('active') else '❌'}\n\n"
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
        
        await status_msg.edit_text(stats_msg, parse_mode=ParseMode.MARKDOWN)

    # ============= BUTTON CALLBACKS =============
    async def button_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        uid = query.from_user.id
        if not self.is_admin(uid):
            await query.edit_message_text("❌ Нет прав.")
            return
        
        if query.data == "confirm_restart_bot":
            await query.edit_message_text(f"🔄 Перезапускаю {BUFFGUILD_SERVICE}...")
            success, message = await ServiceManager.restart_service(BUFFGUILD_SERVICE, uid)
            await query.edit_message_text(message)
        
        elif query.data == "confirm_restart_tg":
            await query.edit_message_text(f"🔄 Перезапускаю {TELEGRAM_SERVICE}...")
            success, message = await ServiceManager.restart_service(TELEGRAM_SERVICE, uid)
            await query.edit_message_text(message)
        
        elif query.data == "cancel_restart":
            await query.edit_message_text("❌ Перезапуск отменён")
        
        elif query.data == "restart_bot":
            keyboard = [[InlineKeyboardButton("✅ Да", callback_data="confirm_restart_bot"),
                         InlineKeyboardButton("❌ Нет", callback_data="cancel_restart")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(f"⚠️ Перезапустить {BUFFGUILD_SERVICE}?", reply_markup=reply_markup)
        
        elif query.data == "restart_tg":
            keyboard = [[InlineKeyboardButton("✅ Да", callback_data="confirm_restart_tg"),
                         InlineKeyboardButton("❌ Нет", callback_data="cancel_restart")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(f"⚠️ Перезапустить {TELEGRAM_SERVICE}?", reply_markup=reply_markup)
        
        elif query.data == "logs_bot":
            logs = await ServiceManager.get_logs(BUFFGUILD_SERVICE, 30, uid)
            if len(logs) > 4000:
                logs = logs[:4000] + "..."
            await query.edit_message_text(f"```\n{logs}\n```", parse_mode=ParseMode.MARKDOWN)
        
        elif query.data == "logs_tg":
            logs = await ServiceManager.get_logs(TELEGRAM_SERVICE, 30, uid)
            if len(logs) > 4000:
                logs = logs[:4000] + "..."
            await query.edit_message_text(f"```\n{logs}\n```", parse_mode=ParseMode.MARKDOWN)
        
        elif query.data == "stop_watching":
            context.user_data['watching'] = False
            await query.edit_message_text("🛑 Наблюдение остановлено")
        
        elif query.data == "pm_start":
            if not self.profile_manager:
                await query.edit_message_text("❌ ProfileManager не инициализирован")
                return
            if hasattr(self.profile_manager, 'start'):
                self.profile_manager.start()
                await query.edit_message_text("✅ ProfileManager запущен")
        
        elif query.data == "pm_stop":
            if not self.profile_manager:
                await query.edit_message_text("❌ ProfileManager не инициализирован")
                return
            if hasattr(self.profile_manager, 'stop'):
                self.profile_manager.stop()
                await query.edit_message_text("⏸️ ProfileManager остановлен")
        
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
            if hasattr(self.profile_manager, '_state'):
                pending = len(self.profile_manager._state.get("pending_triggers", {}))
                status_msg += f"\nАктивных триггеров: {pending}"
            await query.edit_message_text(status_msg)

    # ============= RUN =============
    def run(self):
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        app = Application.builder().token(self.telegram_token).build()

        # Диалог добавления токена
        conv = ConversationHandler(
            entry_points=[CommandHandler("addtoken", self.add_token)],
            states={
                ConversationState.WAIT_NAME.value: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.recv_name)],
                ConversationState.WAIT_CLASS.value: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.recv_class)],
                ConversationState.WAIT_TOKEN.value: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.recv_token)],
                ConversationState.WAIT_CHAT.value: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.recv_chat)],
                ConversationState.WAIT_VOICES.value: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.recv_voices)],
                ConversationState.WAIT_RACES.value: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.recv_races)],
            },
            fallbacks=[CommandHandler("cancel", self.cancel)],
        )

        # Основные команды
        app.add_handler(CommandHandler("start", self.start))
        app.add_handler(conv)
        app.add_handler(CommandHandler(["listtokens", "list_tokens"], self.list_tokens))
        app.add_handler(CommandHandler(["tokeninfo", "token_info"], self.token_info))
        app.add_handler(CommandHandler(["setvoices", "set_voices"], self.set_voices))
        app.add_handler(CommandHandler("enable", self.enable))
        app.add_handler(CommandHandler("disable", self.disable))
        app.add_handler(CommandHandler("remove", self.remove))
        app.add_handler(CommandHandler("reload", self.reload_config))
        
        # Сервисные команды
        app.add_handler(CommandHandler("restart_bot", self.restart_bot))
        app.add_handler(CommandHandler("restart_tg", self.restart_tg))
        app.add_handler(CommandHandler("status", self.service_status))
        app.add_handler(CommandHandler("logs", self.service_logs))
        
        # Мониторинг
        app.add_handler(CommandHandler("stats", self.system_stats))
        app.add_handler(CommandHandler("watch", self.watch_logs))
        app.add_handler(CommandHandler("profile", self.profile_manager_control))
        app.add_handler(CommandHandler("diagnose", self.full_diagnose))
        
        # Callback handlers
        app.add_handler(CallbackQueryHandler(self.button_callback))
        app.add_handler(CallbackQueryHandler(self.list_tokens_callback, pattern=r"^list_page_\d+$"))

        logger.info("🤖 Telegram Admin Bot started")
        logger.info(f"📡 Services: {BUFFGUILD_SERVICE} and {TELEGRAM_SERVICE}")
        
        app.run_polling()


def main():
    tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    admins = os.getenv("ADMIN_USER_IDS", "")

    if not tg_token:
        raise SystemExit("❌ Set TELEGRAM_BOT_TOKEN environment variable")

    if not admins:
        raise SystemExit("❌ Set ADMIN_USER_IDS environment variable")

    admin_ids = [int(x.strip()) for x in admins.split(",") if x.strip()]
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
    
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    async def check_sudo():
        success, message = await ServiceManager.check_sudo_permissions()
        if not success:
            logger.warning("⚠️ Нет прав sudo без пароля! Команды управления сервисами будут недоступны.")
        else:
            logger.info("✅ Права sudo настроены корректно")
    
    loop.run_until_complete(check_sudo())
    
    # Запускаем бота
    bot = TelegramAdmin(tg_token, admin_ids, config_path)
    bot.run()


if __name__ == "__main__":
    main()
