# -*- coding: utf-8 -*-
import logging

from .notifications import build_final_text

logger = logging.getLogger(__name__)


class SchedulerCallback:
    def __init__(self, bot):
        self.bot = bot

    def on_buff_complete(self, job, info: dict):
        fin, snap = self.bot.state.apply_completion(job, info)
        if fin and snap:
            txt = build_final_text(job.sender_id, snap, self.bot.tm)
            if txt:
                self.bot.send_to_peer(self.bot.source_peer_id, txt)
