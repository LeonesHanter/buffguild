# -*- coding: utf-8 -*-
import logging
import re
import threading
import time

from .custom_triggers import custom_parser, custom_storage, CustomBuff

logger = logging.getLogger(__name__)


class CustomTriggerHandler:
    def __init__(self, bot):
        self.bot = bot
        self.ARA_ID = 294529251
        self.KIR_ID = 8244449

    def handle_command(self, text: str, from_id: int) -> bool:
        """Обработка команды от пользователя (Ара/Кир)"""
        trig, q = custom_parser.parse_command(text)
        if not trig or not q:
            return False

        keys = custom_parser.parse_buff_query(trig, q)
        if not keys:
            return False

        ex_id = self.ARA_ID if trig == 'ара' else self.KIR_ID
        logger.info(f"🎯 {trig} для @id{from_id}: {keys}")
        
        custom_storage.register_trigger(from_id, trig, ex_id, keys)

        threading.Thread(target=self._wait, args=(from_id, len(keys)), daemon=True).start()
        return True

    def handle_game_response(self, msg: dict) -> bool:
        """Обработка ответа от игры"""
        text = msg.get("text", "")
        msg_id = msg.get("id", 0)
        cmid = msg.get("conversation_message_id", 0)
        
        m = re.search(r'\[id(\d+)\|', text)
        if not m:
            return False

        uid = int(m.group(1))
        
        tdata = custom_storage.get_trigger_data(uid)
        if not tdata:
            return False

        low = text.lower()
        bkey = None
        if "атака" in low:
            bkey = 'а'
        elif "защита" in low:
            bkey = 'з'
        elif "удача" in low:
            bkey = 'у'
        elif "человек" in low or "людей" in low:
            bkey = 'ч'
        elif "эльф" in low:
            bkey = 'э'
        else:
            return False

        if bkey not in tdata['buff_keys']:
            return False

        crit, val, _ = custom_parser.parse_game_response(text)
        voices = custom_parser.extract_voices_from_response(text)

        buff = CustomBuff(
            trigger=tdata['trigger'],
            buff_key=bkey,
            buff_name=custom_parser.buff_names[bkey],
            is_critical=crit,
            buff_value=val,
            full_response=text,
            user_id=uid,
            executor_id=tdata['executor_id'],
            timestamp=time.time()
        )

        all_col, notif = custom_storage.add_response(uid, buff)
        custom_storage.mark_msg_processed(msg_id, cmid)

        current = len(tdata['responses'])
        total = len(tdata['buff_keys'])
        logger.info(f"✅ Добавлен {bkey} для {uid} ({current}/{total})")

        if notif:
            self._send_notif(uid)
        return True

    def _wait(self, uid: int, need: int):
        """Ожидание всех ответов (в отдельном потоке)"""
        max_wait = 240  # ← УВЕЛИЧЕНО ДО 4 МИНУТ
        waited = 0
        interval = 0.5
        
        while waited < max_wait:
            time.sleep(interval)
            waited += interval
            
            td = custom_storage.get_trigger_data(uid)
            if not td:
                return
            if len(td['responses']) >= need:
                logger.info(f"✅ Все {need} ответов получены для {uid}")
                return
        
        logger.warning(f"⏰ Таймаут для {uid}")
        td = custom_storage.get_trigger_data(uid)
        if td and td['responses']:
            self._send_notif(uid)

    def _send_notif(self, uid: int):
        """Отправка нотификации"""
        td = custom_storage.get_trigger_data(uid)
        rs = custom_storage.get_responses(uid)
        
        if not td or not rs:
            return
            
        notif = custom_parser.format_notification(
            td['trigger'], 
            uid, 
            td['executor_id'], 
            rs
        )
        self.bot.send_to_peer(self.bot.source_peer_id, notif)
        custom_storage.complete_trigger(uid)
