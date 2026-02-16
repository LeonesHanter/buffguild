# -*- coding: utf-8 -*-
import logging

from .notifications import build_final_text
from .group_handler import GroupProxy

logger = logging.getLogger(__name__)


class SchedulerCallback:
    def __init__(self, bot):
        self.bot = bot

    def _get_edit_peer_id(self) -> int:
        """peer_id чата где сообщение было отправлено"""
        if isinstance(self.bot.sender_token, GroupProxy):
            return self.bot.sender_token.source_peer_id
        return self.bot.source_peer_id

    def on_buff_complete(self, job, info: dict):
        fin, snap = self.bot.state.apply_completion(job, info)
        if fin and snap:
            txt = build_final_text(
                job.sender_id, snap, self.bot.tm
            )
            if not txt:
                return

            # ── Ищем registration_msg_id ──
            registration_msg_id = None

            for i, item in enumerate(snap):
                if item.get("registration_msg_id"):
                    registration_msg_id = item[
                        "registration_msg_id"
                    ]
                    logger.info(
                        f"🔍 registration_msg_id="
                        f"{registration_msg_id} "
                        f"в snap[{i}]"
                    )
                    break

            if (
                not registration_msg_id
                and info.get("registration_msg_id")
            ):
                registration_msg_id = info[
                    "registration_msg_id"
                ]
                logger.info(
                    f"🔍 registration_msg_id="
                    f"{registration_msg_id} из info"
                )

            if registration_msg_id:
                edit_peer = self._get_edit_peer_id()

                logger.info(
                    f"✏️ Редактируем {registration_msg_id} "
                    f"в чате {edit_peer} "
                    f"для user_id={job.sender_id}"
                )

                success, status = self.bot.edit_message(
                    peer_id=edit_peer,
                    message_id=registration_msg_id,
                    text=txt
                )

                if success:
                    logger.info(
                        f"✅ Отредактировано "
                        f"{registration_msg_id} "
                        f"для user_id={job.sender_id}"
                    )
                else:
                    logger.error(
                        f"❌ Ошибка edit "
                        f"{registration_msg_id}: {status}"
                    )
                    logger.info(
                        "📤 Фолбэк: новое сообщение"
                    )
                    self.bot.send_to_peer(
                        self.bot.source_peer_id, txt
                    )
            else:
                logger.warning(
                    f"📤 Нет registration_msg_id, "
                    f"новое сообщение "
                    f"для user_id={job.sender_id}"
                )
                self.bot.send_to_peer(
                    self.bot.source_peer_id, txt
                )
