# -*- coding: utf-8 -*-
import logging

from .notifications import build_final_text
from .group_handler import GroupProxy

logger = logging.getLogger(__name__)

# Флаг: редактировать ли сообщение регистрации
ENABLE_EDIT = False


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
        if not (fin and snap):
            return

        txt = build_final_text(job.sender_id, snap, self.bot.tm)
        if not txt:
            return

        # ── Ищем registration_msg_id ──
        registration_msg_id = None

        for i, item in enumerate(snap):
            if item.get("registration_msg_id"):
                registration_msg_id = item["registration_msg_id"]
                logger.info(
                    f"🔍 registration_msg_id={registration_msg_id} "
                    f"в snap[{i}] для user_id={job.sender_id}"
                )
                break

        if not registration_msg_id and info.get("registration_msg_id"):
            registration_msg_id = info["registration_msg_id"]
            logger.info(
                f"🔍 registration_msg_id={registration_msg_id} "
                f"из info для user_id={job.sender_id}"
            )

        # ── Редактирование (если включено) ──
        if ENABLE_EDIT and registration_msg_id:
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
                    f"✅ Сообщение {registration_msg_id} "
                    f"отредактировано для user_id={job.sender_id}"
                )
            else:
                logger.error(
                    f"❌ Ошибка редактирования сообщения "
                    f"{registration_msg_id}: {status} "
                    f"для user_id={job.sender_id}"
                )
                logger.info(
                    f"📤 Отправляем новое сообщение "
                    f"для user_id={job.sender_id} (фолбэк)"
                )
                self.bot.send_to_peer(self.bot.source_peer_id, txt)

        else:
            # ── Новое сообщение (парсер видит) ──
            if not ENABLE_EDIT:
                logger.info(
                    f"📤 ENABLE_EDIT=False, "
                    f"отправляем новое сообщение "
                    f"для user_id={job.sender_id}"
                )
            else:
                logger.warning(
                    f"📤 Нет registration_msg_id, новое сообщение "
                    f"для user_id={job.sender_id}"
                )

            # Отправляем итоговый результат в чат группы (через send_to_peer)
            self.bot.send_to_peer(self.bot.source_peer_id, txt)

            # ── Дополнительно: УДАЛЯЕМ сообщение регистрации ──
            if registration_msg_id and isinstance(self.bot.sender_token, GroupProxy):
                try:
                    peer = self.bot.sender_token.source_peer_id  # чат группы (7)
                    cmid = registration_msg_id  # у нас registration_msg_id == cmid
                    ok = self.bot.sender_token.delete_message(
                        peer_id=peer,
                        message_id=0,
                        cmid=cmid
                    )
                    if ok:
                        logger.info(
                            f"🗑️ Сообщение регистрации cmid={cmid} "
                            f"удалено из чата {peer}"
                        )
                    else:
                        logger.warning(
                            f"⚠️ Не удалось удалить сообщение регистрации "
                            f"cmid={cmid} из чата {peer}"
                        )
                except Exception as e:
                    logger.error(
                        f"❌ Ошибка при удалении сообщения регистрации: {e}",
                        exc_info=True
                    )
