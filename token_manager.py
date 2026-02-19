# -*- coding: utf-8 -*-
import json
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional

from .constants import RACE_NAMES
from .token_handler import TokenHandler

logger = logging.getLogger(__name__)


class AutoSaveThread(threading.Thread):
    def __init__(self, token_manager, interval=30):
        super().__init__(daemon=True)
        self.token_manager = token_manager
        self.interval = interval
        self.running = True

    def run(self):
        logger.info(f"💾 Автосохранение запущено (интервал: {self.interval}с)")
        while self.running:
            try:
                self.token_manager.periodic_save()
            except Exception as e:
                logger.error(f"❌ Ошибка автосохранения: {e}")

            for _ in range(self.interval):
                if not self.running:
                    break
                time.sleep(1)

    def stop(self):
        self.running = False


class OptimizedTokenManager:
    def __init__(self, config_path: str, vk):
        self.config_path = config_path
        self._lock = threading.Lock()
        self._vk = vk

        self._pending_save = False
        self._last_save_time = 0.0
        self._save_interval = 30
        self._auto_save_thread = None

        # индексы
        self._by_id_index: Dict[str, TokenHandler] = {}
        self._by_name_index: Dict[str, TokenHandler] = {}
        self._by_owner_index: Dict[int, List[TokenHandler]] = {}
        self._by_class_index: Dict[str, List[TokenHandler]] = {}
        self._apostles_by_race_index: Dict[str, List[TokenHandler]] = {}

        self.config: Dict[str, Any] = {}
        self.tokens: List[TokenHandler] = []
        self.observer_token_id: str = ""
        self.settings: Dict[str, Any] = {}

        # ============= ДОБАВЛЯЕМ ССЫЛКУ НА PROFILE MANAGER =============
        self.profile_manager = None
        # ==============================================================

        # Обработчик сообщества
        self.group_handler: Optional[Any] = None

        self.load()
        self._init_group_handler()
        self._build_indexes()

    # ============= НОВЫЙ МЕТОД =============
    def set_profile_manager(self, profile_manager):
        """Устанавливает ссылку на ProfileManager"""
        self.profile_manager = profile_manager
        logger.info("🔗 ProfileManager связан с TokenManager")
    # ========================================

    def _init_group_handler(self):
        """Инициализация обработчика сообщества"""
        try:
            from .group_handler import GroupHandler

            group_settings = self.config.get("group_settings", {})
            if group_settings:
                self.group_handler = GroupHandler(group_settings, self._vk)

                logger.info(f"🔍 Инициализация GroupHandler:")
                logger.info(f"  • config group_id: {self.group_handler.group_id}")
                logger.info(f"  • API group_id (abs): {abs(self.group_handler.group_id)}")
                logger.info(f"  • token length: {len(self.group_handler.access_token)}")
                logger.info(f"  • group_name: {self.group_handler.name}")

                is_valid = self.group_handler.is_valid()
                logger.info(f"  • is_valid(): {is_valid}")

                if is_valid:
                    logger.info(f"✅ GroupHandler инициализирован: {self.group_handler.name}")
                    if self.group_handler.get_long_poll_server():
                        logger.info(f"✅ GroupHandler LongPoll настроен")
                    else:
                        logger.warning(f"⚠️ GroupHandler: не удалось настроить LongPoll")
                else:
                    logger.warning("⚠️ GroupHandler: конфигурация невалидна")

        except ImportError as e:
            logger.warning(f"⚠️ Не удалось импортировать GroupHandler: {e}")
            self.group_handler = None
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации GroupHandler: {e}", exc_info=True)
            self.group_handler = None

    def load(self) -> None:
        with open(self.config_path, "r", encoding="utf-8") as f:
            self.config = json.load(f)

        self.observer_token_id = str(self.config.get("observer_token_id", "")).strip()
        self.settings = dict(self.config.get("settings", {}))

        self.tokens = []
        for t_cfg in self.config.get("tokens", []):
            self.tokens.append(TokenHandler(t_cfg, self._vk, self))

        logging.info(f"📋 Tokens: {len(self.tokens)}")

    def _build_indexes(self) -> None:
        self._by_id_index = {t.id: t for t in self.tokens}
        self._by_name_index = {t.name.strip().lower(): t for t in self.tokens}
        self._by_owner_index = {}
        self._by_class_index = {}
        self._apostles_by_race_index = {race: [] for race in RACE_NAMES.keys()}

        obs = self.get_observer_token_object()

        for t in self.tokens:
            self._by_owner_index.setdefault(t.owner_vk_id, []).append(t)
            self._by_class_index.setdefault(t.class_type, []).append(t)
            
            # ============= ВАЖНО: Проверяем и синхронизируем голоса при загрузке =============
            # Если есть реальные голоса, но есть и виртуальные - очищаем виртуальные
            if t.voices > 0 and t.virtual_voices > 0:
                logger.info(f"🔄 {t.name}: обнаружены реальные ({t.voices}) и виртуальные ({t.virtual_voices}) голоса, очищаем виртуальные")
                t.virtual_voices = 0
                t.mark_for_save()
            
            # Если есть реальные голоса и флаг ручного ввода - сбрасываем флаг
            if t.voices > 0 and t.needs_manual_voices:
                logger.info(f"🔄 {t.name}: сброс флага ручного ввода при загрузке (есть {t.voices} голосов)")
                t.needs_manual_voices = False
                t.mark_for_save()
            # =================================================================================

            if t.class_type == "apostle" and (not obs or t.id != obs.id):
                if t.temp_races:
                    logger.debug(
                        f"🔍 {t.name}: проверка временных рас "
                        f"при загрузке: {t.temp_races}"
                    )
                    changed = t.cleanup_only_expired()

                    for tr in t.temp_races:
                        race = tr.get("race", "unknown")
                        expires = tr.get("expires", 0)
                        current_time = time.time()
                        if expires > current_time:
                            hours_left = (expires - current_time) / 3600
                            logger.info(
                                f"🕒 {t.name}: активная временная раса "
                                f"'{race}' (осталось {hours_left:.1f} часов)"
                            )
                        else:
                            logger.warning(
                                f"⚠️ {t.name}: временная раса "
                                f"'{race}' просрочена"
                            )

                for race in t.races:
                    if race in self._apostles_by_race_index:
                        self._apostles_by_race_index[race].append(t)

                for tr in t.temp_races:
                    race = tr["race"]
                    if race in self._apostles_by_race_index:
                        self._apostles_by_race_index[race].append(t)

    def get_observer_token_object(self) -> Optional[TokenHandler]:
        """Получает объект токена Observer"""
        if not self.observer_token_id:
            return None
        return self._by_id_index.get(self.observer_token_id)

    # ═══════════════════════════════════════════════
    #  ИСПРАВЛЕНО: используем GroupProxy из group_handler.py
    #  Старый внутренний класс GroupProxy УДАЛЁН
    # ═══════════════════════════════════════════════
    def _create_group_proxy(self):
        """Создает прокси-объект для совместимости с TokenHandler"""
        if not self.group_handler:
            raise RuntimeError("GroupHandler не инициализирован")

        from .group_handler import GroupProxy
        source_chat_id = self.settings.get("observer_source_chat_id", 7)
        return GroupProxy(self.group_handler, source_chat_id, self._vk)

    def get_observer(self):
        """Получение Observer (токен или группа)"""
        if self.group_handler:
            try:
                if (
                    hasattr(self.group_handler, 'is_valid')
                    and self.group_handler.is_valid()
                ):
                    logger.info("👥 Используется GroupHandler для Observer")
                    return self._create_group_proxy()
                else:
                    logger.warning(
                        "⚠️ GroupHandler невалиден, "
                        "пробуем пользовательский токен"
                    )
            except Exception as e:
                logger.error(f"❌ Ошибка проверки GroupHandler: {e}")

        if not self.observer_token_id:
            raise RuntimeError(
                "observer_token_id is not set in config.json"
            )

        t = self.get_token_by_id(self.observer_token_id)
        if not t:
            raise RuntimeError(
                f"observer_token_id='{self.observer_token_id}' "
                f"not found in tokens[]"
            )

        logger.info("👤 Используется пользовательский токен для Observer")
        return t

    def reload(self) -> None:
        with self._lock:
            self.load()
            self._init_group_handler()
            self._build_indexes()
            logging.info(
                "🔄 TokenManager: конфигурация перезагружена"
            )

    def mark_for_save(self) -> None:
        self._pending_save = True

    def save_all_tokens(self):
        self.save(force=True)

    def periodic_save(self):
        current_time = time.time()
        if (
            self._pending_save
            and current_time - self._last_save_time >= self._save_interval
        ):
            self.save(force=True)
            logger.debug("💾 Периодическое сохранение конфигурации")

    def save(self, force: bool = False) -> None:
        current_time = time.time()

        if (
            not force
            and self._pending_save
            and current_time - self._last_save_time < 3
        ):
            logging.debug(
                f"⏳ Пропускаем сохранение, еще рано. "
                f"Последнее: {self._last_save_time:.1f}, "
                f"сейчас: {current_time:.1f}"
            )
            return

        with self._lock:
            temp_path = self.config_path + ".tmp"

            payload_tokens: List[Dict[str, Any]] = []
            for t in self.tokens:
                payload_tokens.append(
                    {
                        "id": t.id,
                        "name": t.name,
                        "class": t.class_type,
                        "access_token": t.access_token,
                        "owner_vk_id": t.owner_vk_id,
                        "source_chat_id": t.source_chat_id,
                        "target_peer_id": t.target_peer_id,
                        "voices": t.voices,
                        "enabled": t.enabled,
                        "races": t.races,
                        "successful_buffs": t.successful_buffs,
                        "total_attempts": t.total_attempts,
                        "temp_races": t.temp_races,
                        "captcha_until": t.captcha_until,
                        "level": t.level,
                        "needs_manual_voices": t.needs_manual_voices,
                        "virtual_voice_grants": t.virtual_voice_grants,
                        "next_virtual_grant_ts": t.next_virtual_grant_ts,
                        "virtual_voices": t.virtual_voices,  # Добавляем виртуальные голоса
                    }
                )

            for token_data in payload_tokens:
                if token_data.get("temp_races"):
                    logger.debug(
                        f"💾 Сохранение временных рас для "
                        f"{token_data['name']}: {token_data['temp_races']}"
                    )

            self.config["observer_token_id"] = self.observer_token_id
            self.config["settings"] = self.settings
            self.config["tokens"] = payload_tokens

            try:
                with open(temp_path, "w", encoding="utf-8") as f:
                    json.dump(
                        self.config, f, ensure_ascii=False, indent=2
                    )

                os.replace(temp_path, self.config_path)

                self._last_save_time = time.time()
                self._pending_save = False

                logging.info(
                    f"💾 Конфигурация сохранена: {self.config_path} "
                    f"(время: {time.strftime('%H:%M:%S')})"
                )

                for token_data in payload_tokens:
                    if token_data.get("temp_races"):
                        logger.info(
                            f"✅ Временные расы сохранены для "
                            f"{token_data['name']}"
                        )

            except Exception as e:
                logging.error(f"❌ Ошибка сохранения конфигурации: {e}")
                try:
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                except Exception:
                    pass
                raise

    def start_auto_save(self, interval=30):
        if self._auto_save_thread is None:
            self._auto_save_thread = AutoSaveThread(self, interval)
            self._auto_save_thread.start()
            logger.info(
                f"💾 Автосохранение запущено (интервал: {interval}с)"
            )

    def stop_auto_save(self):
        if self._auto_save_thread:
            self._auto_save_thread.stop()
            self._auto_save_thread.join(timeout=5)
            self._auto_save_thread = None
            logger.info("💾 Автосохранение остановлено")

    def get_token_by_id(
        self, token_id: str
    ) -> Optional[TokenHandler]:
        return self._by_id_index.get(token_id)

    def get_token_by_name(
        self, name: str
    ) -> Optional[TokenHandler]:
        return self._by_name_index.get(
            (name or "").strip().lower()
        )

    def _update_owner_index(
        self, token: TokenHandler, old_owner: int, new_owner: int
    ) -> None:
        with self._lock:
            if old_owner in self._by_owner_index:
                self._by_owner_index[old_owner] = [
                    t
                    for t in self._by_owner_index[old_owner]
                    if t.id != token.id
                ]
                if not self._by_owner_index[old_owner]:
                    del self._by_owner_index[old_owner]

            self._by_owner_index.setdefault(new_owner, []).append(
                token
            )

    def get_token_by_sender_id(
        self, sender_id: int
    ) -> Optional[TokenHandler]:
        if sender_id in self._by_owner_index:
            for t in self._by_owner_index[sender_id]:
                if t.owner_vk_id == sender_id:
                    return t

        unknown_owner_tokens = self._by_owner_index.get(0, [])
        for t in unknown_owner_tokens[:5]:
            old_owner = t.owner_vk_id
            uid = t.fetch_owner_id_lazy()
            if uid and uid != old_owner:
                self._update_owner_index(t, old_owner, uid)
            if uid == sender_id:
                return t

        if sender_id in self._by_owner_index:
            for t in self._by_owner_index[sender_id]:
                if t.owner_vk_id == sender_id:
                    return t

        return None

    def all_buffers(self) -> List[TokenHandler]:
        obs_token = self.get_observer_token_object()
        return [
            t for t in self.tokens
            if not obs_token or t.id != obs_token.id
        ]

    def get_apostles_with_race(
        self, race_key: str
    ) -> List[TokenHandler]:
        obs_token = self.get_observer_token_object()
        result: List[TokenHandler] = []

        for t in self._apostles_by_race_index.get(race_key, []):
            if not obs_token or t.id != obs_token.id:
                result.append(t)

        return result

    def update_race_index(self, token: TokenHandler) -> None:
        if token.class_type != "apostle":
            return

        for race in RACE_NAMES.keys():
            if token in self._apostles_by_race_index.get(race, []):
                self._apostles_by_race_index[race] = [
                    t
                    for t in self._apostles_by_race_index[race]
                    if t.id != token.id
                ]

        token.cleanup_only_expired()

        for race in token.races:
            if race in self._apostles_by_race_index:
                self._apostles_by_race_index[race].append(token)

        for tr in token.temp_races:
            race = tr["race"]
            if race in self._apostles_by_race_index:
                self._apostles_by_race_index[race].append(token)
