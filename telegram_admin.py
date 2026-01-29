# -*- coding: utf-8 -*-
"""
Telegram админ-бот для управления токенами.

Команды:
- /start - Список команд
- /add_token - Добавить новый токен (диалог)
- /list_tokens - Список всех токенов
- /enable <id|name> - Включить токен
- /disable <id|name> - Отключить токен
- /remove <id|name> - Удалить токен
- /weights - Статистика весов
- /timing - Статистика таймингов
- /reset_weights - Сбросить веса
"""

import json
import logging
import os
import time
from typing import Dict, Any, List

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# Классы персонажей
CLASS_CHOICES = {
    "apostle": "Апостол",
    "warlock": "Чернокнижник",
    "crusader": "Крестоносец",
    "light_incarnation": "Воплощение света",
}


class TelegramAdmin:
    """Telegram бот для управления токенами"""
    
    WAIT_NAME = 1
    WAIT_CLASS = 2
    WAIT_TOKEN = 3
    WAIT_CHAT = 4
    WAIT_TARGET = 5

    def __init__(
        self,
        telegram_token: str,
        admin_ids: List[int],
        config_path: str,
        bot_instance=None
    ):
        self.telegram_token = telegram_token
        self.admin_ids = set(admin_ids)
        self.config_path = config_path
        self.bot_instance = bot_instance  # Ссылка на MultiTokenBot (опционально)
        self.tmp: Dict[int, Dict[str, Any]] = {}

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

    # ---- Команды ----
    
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /start"""
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return

        msg = (
            "🤖 *Blessing Bot Admin Panel*\n\n"
            "📋 *Команды:*\n"
            "/add\\_token — добавить токен\n"
            "/list\\_tokens — список токенов\n"
            "/weights — статистика весов\n"
            "/timing — статистика таймингов\n"
            "/enable <id\\|name> — включить токен\n"
            "/disable <id\\|name> — отключить токен\n"
            "/remove <id\\|name> — удалить токен\n"
            "/reset\\_weights — сбросить веса\n"
            "/reload — перезагрузить конфиг"
        )
        await update.message.reply_text(msg, parse_mode="MarkdownV2")

    # ---- Добавление токена (диалог) ----
    
    async def add_token(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Начало добавления токена"""
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return ConversationHandler.END

        self.tmp[uid] = {}
        await update.message.reply_text(
            "➕ *Добавление токена*\n\n"
            "📝 Шаг 1/5: Введите имя токена\n"
            "Например: `Main`, `Backup1`, `Reserve`",
            parse_mode="MarkdownV2"
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

        classes = "\n".join([
            f"`{k}` — {v}" 
            for k, v in CLASS_CHOICES.items()
        ])
        
        await update.message.reply_text(
            f"✅ Имя: *{name}*\n\n"
            f"🎭 Шаг 2/5: Выберите класс\n\n"
            f"{classes}\n\n"
            f"Отправьте код класса \\(например: `apostle`\\)",
            parse_mode="MarkdownV2"
        )
        return self.WAIT_CLASS

    async def recv_class(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Получение класса"""
        uid = update.effective_user.id
        cls = update.message.text.strip().lower()
        
        if cls not in CLASS_CHOICES:
            await update.message.reply_text(
                f"❌ Неизвестный класс: `{cls}`\n\n"
                f"Доступные: {', '.join(CLASS_CHOICES.keys())}",
                parse_mode="MarkdownV2"
            )
            return self.WAIT_CLASS
        
        self.tmp[uid]["class"] = cls
        class_name = CLASS_CHOICES[cls]
        
        await update.message.reply_text(
            f"✅ Класс: *{class_name}*\n\n"
            f"🔑 Шаг 3/5: Отправьте VK access token\n"
            f"Токен должен начинаться с `vk1\\.a\\.`",
            parse_mode="MarkdownV2"
        )
        return self.WAIT_TOKEN

    async def recv_token(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Получение токена"""
        uid = update.effective_user.id
        token = update.message.text.strip()
        
        if not token.startswith("vk1.a."):
            await update.message.reply_text(
                "❌ Неверный формат токена. Должен начинаться с `vk1.a.`"
            )
            return self.WAIT_TOKEN
        
        self.tmp[uid]["access_token"] = token
        
        await update.message.reply_text(
            "✅ Токен сохранён\n\n"
            "📁 Шаг 4/5: ID чата \\(source\\_chat\\_id\\)\n"
            "Например: `48`",
            parse_mode="MarkdownV2"
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
            f"✅ Chat ID: `{chat_id}`\n\n"
            f"🎯 Шаг 5/5: Target peer\\_id\n"
            f"Например: `\\-183040898`",
            parse_mode="MarkdownV2"
        )
        return self.WAIT_TARGET

    async def recv_target(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Получение target_peer_id и завершение"""
        uid = update.effective_user.id
        
        try:
            target_peer = int(update.message.text.strip())
        except ValueError:
            await update.message.reply_text("❌ Нужно число.")
            return self.WAIT_TARGET

        data = self.tmp.get(uid, {})
        token_id = f"token_{int(time.time())}"
        
        new_token = {
            "id": token_id,
            "name": data["name"],
            "class": data["class"],
            "access_token": data["access_token"],
            "user_id": 0,
            "source_chat_id": data["source_chat_id"],
            "target_peer_id": target_peer,
            "voices": 5,
            "enabled": True,
            "last_check": 0,
        }

        cfg = self._load()
        cfg.setdefault("tokens", []).append(new_token)
        cfg.setdefault("settings", {}).setdefault("delay", 2)
        self._save(cfg)

        # Перезагружаем конфиг в боте (если подключён)
        if self.bot_instance:
            self.bot_instance.tm.reload()

        self.tmp.pop(uid, None)

        class_name = CLASS_CHOICES[new_token["class"]]
        
        await update.message.reply_text(
            f"✅ *Токен добавлен\\!*\n\n"
            f"📛 Имя: *{new_token['name']}*\n"
            f"🎭 Класс: *{class_name}*\n"
            f"🆔 ID: `{token_id}`\n"
            f"📁 Chat: `{new_token['source_chat_id']}`\n"
            f"🎯 Target: `{target_peer}`\n"
            f"🔊 Голосов: *5*\n"
            f"✅ Статус: *Активен*",
            parse_mode="MarkdownV2"
        )
        return ConversationHandler.END

    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отмена диалога"""
        uid = update.effective_user.id
        self.tmp.pop(uid, None)
        await update.message.reply_text("❌ Отменено.")
        return ConversationHandler.END

    # ---- Список токенов ----
    
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

        lines = ["📋 *Список токенов:*\n"]
        
        for i, t in enumerate(tokens, 1):
            cls = t.get("class", "apostle")
            cls_name = CLASS_CHOICES.get(cls, cls)
            status = "✅" if t.get("enabled", True) else "🚫"
            voices = t.get("voices", "?")
            voices_emoji = "🔊" if voices > 0 else "🔇"
            
            lines.append(
                f"{i}\\. *{t.get('name', t['id'])}*\n"
                f"   🎭 {cls_name}\n"
                f"   {status} {voices_emoji} Голосов: `{voices}`\n"
                f"   🆔 `{t['id']}`\n"
            )
        
        await update.message.reply_text("\n".join(lines), parse_mode="MarkdownV2")

    # ---- Включение/отключение токенов ----
    
    def _toggle(self, ident: str, enabled: bool) -> bool:
        """Включить/отключить токен по ID или имени"""
        cfg = self._load()
        changed = False
        
        for t in cfg.get("tokens", []):
            if t.get("id") == ident or t.get("name") == ident:
                t["enabled"] = enabled
                changed = True
        
        if changed:
            self._save(cfg)
            if self.bot_instance:
                self.bot_instance.tm.reload()
        
        return changed

    async def enable(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Включить токен"""
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return
        
        if not context.args:
            await update.message.reply_text("Использование: `/enable <id|name>`", parse_mode="MarkdownV2")
            return
        
        ident = " ".join(context.args)
        ok = self._toggle(ident, True)
        
        await update.message.reply_text(
            f"✅ Токен `{ident}` включён" if ok else f"❌ Не найдено: `{ident}`",
            parse_mode="MarkdownV2"
        )

    async def disable(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отключить токен"""
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return
        
        if not context.args:
            await update.message.reply_text("Использование: `/disable <id|name>`", parse_mode="MarkdownV2")
            return
        
        ident = " ".join(context.args)
        ok = self._toggle(ident, False)
        
        await update.message.reply_text(
            f"🚫 Токен `{ident}` отключён" if ok else f"❌ Не найдено: `{ident}`",
            parse_mode="MarkdownV2"
        )

    # ---- Удаление токена ----
    
    async def remove(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Удалить токен"""
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return
        
        if not context.args:
            await update.message.reply_text("Использование: `/remove <id|name>`", parse_mode="MarkdownV2")
            return
        
        ident = " ".join(context.args)

        cfg = self._load()
        before = len(cfg.get("tokens", []))
        cfg["tokens"] = [
            t for t in cfg.get("tokens", [])
            if t.get("id") != ident and t.get("name") != ident
        ]
        after = len(cfg["tokens"])
        
        if after < before:
            self._save(cfg)
            if self.bot_instance:
                self.bot_instance.tm.reload()
            await update.message.reply_text(f"🗑️ Токен `{ident}` удалён", parse_mode="MarkdownV2")
        else:
            await update.message.reply_text(f"❌ Не найдено: `{ident}`", parse_mode="MarkdownV2")

    # ---- Статистика ----
    
    async def weights(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Статистика весов токенов"""
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return

        if not self.bot_instance:
            await update.message.reply_text("⚠️ Бот не подключён")
            return

        stats = self.bot_instance.tm.get_weight_stats()
        
        if not stats:
            await update.message.reply_text("📭 Нет данных о весах.")
            return

        lines = ["⚖️ *Веса токенов:*\n"]
        
        for s in stats:
            token_id = s["token_id"]
            weight = s["weight"]
            failures = s["consecutive_failures"]
            
            # Находим имя токена
            token_name = token_id
            for t in self.bot_instance.tm.get_all_tokens_info():
                if t["id"] == token_id:
                    token_name = t["name"]
                    break
            
            # Визуальная шкала веса
            bar_full = int(weight * 10)
            bar = "█" * bar_full + "░" * (10 - bar_full)
            
            lines.append(
                f"*{token_name}*\n"
                f"   {bar} `{weight:.1f}`\n"
                f"   Провалов подряд: `{failures}`\n"
            )
        
        await update.message.reply_text("\n".join(lines), parse_mode="MarkdownV2")

    async def timing_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Статистика таймингов"""
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return

        if not self.bot_instance:
            await update.message.reply_text("⚠️ Бот не подключён")
            return

        stats = self.bot_instance.timing.get_stats()
        
        if not stats:
            await update.message.reply_text("📭 Недостаточно данных.")
            return

        msg = (
            f"⏱️ *Статистика таймингов*\n\n"
            f"⏰ Текущее ожидание: `{stats['current_wait']:.2f}` сек\n"
            f"📊 Среднее: `{stats['avg_response']:.2f}` сек\n"
            f"⚡ Минимум: `{stats['min_response']:.2f}` сек\n"
            f"🐌 Максимум: `{stats['max_response']:.2f}` сек\n"
            f"📈 Образцов: `{int(stats['samples'])}`\n\n"
            f"💡 Система автоматически подстраивает время ожидания"
        )
        
        await update.message.reply_text(msg, parse_mode="MarkdownV2")

    async def reset_weights(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Сбросить веса всех токенов"""
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return

        if not self.bot_instance:
            await update.message.reply_text("⚠️ Бот не подключён")
            return

        self.bot_instance.tm.weight.reset_all()
        await update.message.reply_text("♻️ Все веса сброшены до 1\\.0", parse_mode="MarkdownV2")

    async def reload_config(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Перезагрузить конфигурацию"""
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("❌ Нет прав.")
            return

        if self.bot_instance:
            self.bot_instance.tm.reload()
            await update.message.reply_text("🔄 Конфигурация перезагружена")
        else:
            await update.message.reply_text("⚠️ Бот не подключён")

    # ---- Запуск ----
    
    def run(self):
        """Запуск Telegram бота"""
        app = Application.builder().token(self.telegram_token).build()

        # Диалог добавления токена
        conv = ConversationHandler(
            entry_points=[CommandHandler("add_token", self.add_token)],
            states={
                self.WAIT_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.recv_name)],
                self.WAIT_CLASS: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.recv_class)],
                self.WAIT_TOKEN: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.recv_token)],
                self.WAIT_CHAT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.recv_chat)],
                self.WAIT_TARGET: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.recv_target)],
            },
            fallbacks=[CommandHandler("cancel", self.cancel)],
        )

        # Регистрация команд
        app.add_handler(CommandHandler("start", self.start))
        app.add_handler(conv)
        app.add_handler(CommandHandler("list_tokens", self.list_tokens))
        app.add_handler(CommandHandler("enable", self.enable))
        app.add_handler(CommandHandler("disable", self.disable))
        app.add_handler(CommandHandler("remove", self.remove))
        app.add_handler(CommandHandler("weights", self.weights))
        app.add_handler(CommandHandler("timing", self.timing_stats))
        app.add_handler(CommandHandler("reset_weights", self.reset_weights))
        app.add_handler(CommandHandler("reload", self.reload_config))

        logging.info("🤖 Telegram Admin Bot started")
        app.run_polling()


def main():
    """Точка входа"""
    tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    admins = os.getenv("ADMIN_USER_IDS", "")
    
    if not tg_token:
        raise SystemExit("❌ Set TELEGRAM_BOT_TOKEN environment variable")
    
    if not admins:
        raise SystemExit("❌ Set ADMIN_USER_IDS environment variable (comma-separated)")

    admin_ids = [int(x.strip()) for x in admins.split(",") if x.strip()]
    
    TelegramAdmin(tg_token, admin_ids, "config.json").run()


if __name__ == "__main__":
    main()
