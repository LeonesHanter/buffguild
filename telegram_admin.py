# -*- coding: utf-8 -*-
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# Дублируем минимально список классов (чтобы админка не импортировала main.py)
CLASS_CHOICES = {
    "apostle": "Апостол",
    "warlock": "Чернокнижник",
    "crusader": "Крестоносец",
    "light_incarnation": "Воплощение света",
}


class TelegramAdmin:
    WAIT_NAME = 1
    WAIT_CLASS = 2
    WAIT_TOKEN = 3
    WAIT_CHAT = 4
    WAIT_TARGET = 5

    def __init__(self, telegram_token: str, admin_ids: List[int], config_path: str):
        self.telegram_token = telegram_token
        self.admin_ids = set(admin_ids)
        self.config_path = config_path
        self.tmp: Dict[int, Dict[str, Any]] = {}

    def is_admin(self, uid: int) -> bool:
        return uid in self.admin_ids

    def _load(self) -> Dict[str, Any]:
        if not os.path.exists(self.config_path):
            return {"tokens": [], "settings": {"delay": 2}}
        with open(self.config_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save(self, cfg: Dict[str, Any]) -> None:
        with open(self.config_path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("Нет прав.")
            return

        await update.message.reply_text(
            "Команды:\n"
            "/add_token — добавить токен\n"
            "/list_tokens — список\n"
            "/enable <id|name>\n"
            "/disable <id|name>\n"
            "/remove <id|name>\n"
        )

    async def add_token(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("Нет прав.")
            return ConversationHandler.END

        self.tmp[uid] = {}
        await update.message.reply_text("Шаг 1/5: имя токена (например: Main1)")
        return self.WAIT_NAME

    async def recv_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        name = update.message.text.strip()
        if len(name) < 2:
            await update.message.reply_text("Слишком коротко. Ещё раз.")
            return self.WAIT_NAME
        self.tmp[uid]["name"] = name

        classes = "\n".join([f"- {k} = {v}" for k, v in CLASS_CHOICES.items()])
        await update.message.reply_text(f"Шаг 2/5: класс:\n{classes}")
        return self.WAIT_CLASS

    async def recv_class(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        cls = update.message.text.strip().lower()
        if cls not in CLASS_CHOICES:
            await update.message.reply_text("Неизвестный класс. Введите ещё раз.")
            return self.WAIT_CLASS
        self.tmp[uid]["class"] = cls
        await update.message.reply_text("Шаг 3/5: VK access_token (vk1.a....)")
        return self.WAIT_TOKEN

    async def recv_token(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        token = update.message.text.strip()
        if not token.startswith("vk1.a."):
            await update.message.reply_text("Похоже не vk1.a. токен. Ещё раз.")
            return self.WAIT_TOKEN
        self.tmp[uid]["access_token"] = token
        await update.message.reply_text("Шаг 4/5: source_chat_id (например 48)")
        return self.WAIT_CHAT

    async def recv_chat(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        try:
            chat_id = int(update.message.text.strip())
        except ValueError:
            await update.message.reply_text("Нужно число.")
            return self.WAIT_CHAT
        self.tmp[uid]["source_chat_id"] = chat_id
        await update.message.reply_text("Шаг 5/5: target_peer_id (например -183040898)")
        return self.WAIT_TARGET

    async def recv_target(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        try:
            target_peer = int(update.message.text.strip())
        except ValueError:
            await update.message.reply_text("Нужно число.")
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

        self.tmp.pop(uid, None)

        await update.message.reply_text(
            f"Добавлено:\n"
            f"id={token_id}\n"
            f"name={new_token['name']}\n"
            f"class={new_token['class']} ({CLASS_CHOICES[new_token['class']]})\n"
            f"chat={new_token['source_chat_id']}\n"
            f"target={new_token['target_peer_id']}"
        )
        return ConversationHandler.END

    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        self.tmp.pop(uid, None)
        await update.message.reply_text("Отменено.")
        return ConversationHandler.END

    async def list_tokens(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("Нет прав.")
            return

        cfg = self._load()
        tokens = cfg.get("tokens", [])
        if not tokens:
            await update.message.reply_text("Пусто.")
            return

        lines = []
        for t in tokens:
            cls = t.get("class", "apostle")
            cls_name = CLASS_CHOICES.get(cls, cls)
            lines.append(
                f"- {t.get('name', t['id'])} | {t['id']} | {cls_name} | enabled={t.get('enabled', True)} | voices={t.get('voices', '?')}"
            )
        await update.message.reply_text("\n".join(lines))

    def _toggle(self, ident: str, enabled: bool) -> bool:
        cfg = self._load()
        changed = False
        for t in cfg.get("tokens", []):
            if t.get("id") == ident or t.get("name") == ident:
                t["enabled"] = enabled
                changed = True
        if changed:
            self._save(cfg)
        return changed

    async def enable(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("Нет прав.")
            return
        if not context.args:
            await update.message.reply_text("Использование: /enable <id|name>")
            return
        ident = " ".join(context.args)
        ok = self._toggle(ident, True)
        await update.message.reply_text("OK" if ok else "Не найдено")

    async def disable(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("Нет прав.")
            return
        if not context.args:
            await update.message.reply_text("Использование: /disable <id|name>")
            return
        ident = " ".join(context.args)
        ok = self._toggle(ident, False)
        await update.message.reply_text("OK" if ok else "Не найдено")

    async def remove(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not self.is_admin(uid):
            await update.message.reply_text("Нет прав.")
            return
        if not context.args:
            await update.message.reply_text("Использование: /remove <id|name>")
            return
        ident = " ".join(context.args)

        cfg = self._load()
        before = len(cfg.get("tokens", []))
        cfg["tokens"] = [t for t in cfg.get("tokens", []) if t.get("id") != ident and t.get("name") != ident]
        after = len(cfg["tokens"])
        self._save(cfg)
        await update.message.reply_text("Удалено" if after < before else "Не найдено")

    def run(self):
        app = Application.builder().token(self.telegram_token).build()

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

        app.add_handler(CommandHandler("start", self.start))
        app.add_handler(conv)
        app.add_handler(CommandHandler("list_tokens", self.list_tokens))
        app.add_handler(CommandHandler("enable", self.enable))
        app.add_handler(CommandHandler("disable", self.disable))
        app.add_handler(CommandHandler("remove", self.remove))

        logging.info("TelegramAdmin started")
        app.run_polling()


def main():
    tg = os.getenv("TELEGRAM_BOT_TOKEN", "")
    admins = os.getenv("ADMIN_USER_IDS", "")
    if not tg:
        raise SystemExit("Set TELEGRAM_BOT_TOKEN")
    if not admins:
        raise SystemExit("Set ADMIN_USER_IDS=1,2,3")

    admin_ids = [int(x.strip()) for x in admins.split(",") if x.strip()]
    TelegramAdmin(tg, admin_ids, "config.json").run()


if __name__ == "__main__":
    main()
