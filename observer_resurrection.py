# -*- coding: utf-8 -*-
import logging
import time

from .constants import RESURRECTION_CONFIG
from .commands import parse_resurrection_cmd

logger = logging.getLogger(__name__)


class ResurrectionHandler:
    def __init__(self, bot):
        self.bot = bot

    def handle(self, text: str, from_id: int):
        lvl = parse_resurrection_cmd(text)
        if not lvl:
            self.bot.send_to_peer(self.bot.source_peer_id, "❌ Неверный формат. Пример: /воскрешение 25")
            return

        logger.info(f"♻️ Воскрешение: цель уровень {lvl}")
        candidates = []

        for t in self.bot.tm.tokens:
            if t.class_type not in ["crusader", "light_incarnation"]:
                continue
            if not t.enabled or t.is_captcha_paused():
                continue
            if t.level < lvl or t.voices < 5:
                continue
            if not t.can_use_social()[0] or not t.can_use_ability("воскрешение")[0]:
                continue
            candidates.append(t)

        if not candidates:
            self.bot.send_to_peer(self.bot.source_peer_id, f"❌ Нет паладинов для уровня {lvl}")
            return

        best = sorted(candidates, key=lambda x: (-x.level, -x.voices))[0]
        logger.info(f"✅ Выбран {best.name} (lvl {best.level})")

        ok, _ = best.send_to_peer(best.target_peer_id, RESURRECTION_CONFIG["command_text"], None)
        if not ok:
            self.bot.send_to_peer(self.bot.source_peer_id, "❌ Ошибка отправки")
            return

        best.set_social_cooldown(62)
        best.set_ability_cooldown("воскрешение", 6*3600+1)

        time.sleep(3)
        found = False
        for msg in best.get_history_cached(best.target_peer_id, 20)[:10]:
            if str(from_id) in msg.get("text", ""):
                found = True
                break

        if not found:
            self.bot.send_to_peer(self.bot.source_peer_id, "❌ Нет подтверждения")
            return

        old = best.voices
        for _ in range(5):
            best.spend_voice()
        logger.info(f"🗣️ {best.name}: списано 5 голосов ({old}→{best.voices})")

        owner = best.owner_vk_id or best.fetch_owner_id_lazy()
        notif = (
            f"🎉 Воскрешение успешно!\n"
            f"[https://vk.ru/id{owner}|♻️]Воскрешение\n"
            f"[https://vk.ru/id{from_id}|💰]Списано 500 баллов"
        )
        self.bot.send_to_peer(self.bot.source_peer_id, notif)
