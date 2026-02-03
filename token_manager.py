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


class OptimizedTokenManager:
    def __init__(self, config_path: str, vk):
        self.config_path = config_path
        self._lock = threading.Lock()
        self._vk = vk
        self._pending_save = False  # ✅ Флаг отложенного сохранения
        self._last_save_time = 0    # ✅ Время последнего сохранения

        self._by_id_index: Dict[str, TokenHandler] = {}
        self._by_name_index: Dict[str, TokenHandler] = {}
        self._by_owner_index: Dict[int, List[TokenHandler]] = {}
        self._by_class_index: Dict[str, List[TokenHandler]] = {}
        self._apostles_by_race_index: Dict[str, List[TokenHandler]] = {}

        self.config: Dict[str, Any] = {}
        self.tokens: List[TokenHandler] = []
        self.observer_token_id: str = ""
        self.settings: Dict[str, Any] = {}

        self.load()
        self._build_indexes()

    def load(self) -> None:
        with open(self.config_path, "r", encoding="utf-8") as f:
            self.config = json.load(f)

        self.observer_token_id = str(self.config.get("observer_token_id", "")).strip()
        self.settings = dict(self.config.get("settings", {}))

        self.tokens = []
        for t_cfg in self.config.get("tokens", []):
            self.tokens.append(TokenHandler(t_cfg, self._vk, self))

        logging.info(f"📋 Tokens: {len(self.tokens)}")

    def _build_indexes(self):
        self._by_id_index = {t.id: t for t in self.tokens}
        self._by_name_index = {t.name.strip().lower(): t for t in self.tokens}

        self._by_owner_index = {}
        self._by_class_index = {}
        self._apostles_by_race_index = {race: [] for race in RACE_NAMES.keys()}

        obs = self.get_observer()  # ← Получаем Observer

        for t in self.tokens:
            self._by_owner_index.setdefault(t.owner_vk_id, []).append(t)
            self._by_class_index.setdefault(t.class_type, []).append(t)

            if t.class_type == "apostle" and t.id != obs.id:  # ← Исключаем Observer из индекса рас
                # force cleanup чтобы индекс не тащил мусор
                changed = t._cleanup_expired_temp_races(force=True)
                # постоянные
                for race in t.races:
                    if race in self._apostles_by_race_index:
                        self._apostles_by_race_index[race].append(t)
                # временные
                for tr in t.temp_races:
                    race = tr["race"]
                    if race in self._apostles_by_race_index:
                        self._apostles_by_race_index[race].append(t)
                if changed:
                    # если очистили — индекс уже соответствует
                    pass

    def reload(self) -> None:
        with self._lock:
            self.load()
            self._build_indexes()
            logging.info("🔄 TokenManager: конфигурация перезагружена и индексы обновлены")

    def mark_for_save(self):
        """Пометить что нужна запись конфигурации"""
        self._pending_save = True

    def save(self, force: bool = False) -> None:
        """Сохранить конфигурацию (с отложенной записью)"""
        current_time = time.time()

        # ✅ Отложенное сохранение: не чаще чем раз в 3 секунды
        if not force and self._pending_save and current_time - self._last_save_time < 3:
            logging.debug(f"⏳ Пропускаем сохранение, еще рано. Последнее: {self._last_save_time:.1f}, сейчас: {current_time:.1f}")
            return  # Пропускаем, еще рано сохранять

        with self._lock:
            temp_path = self.config_path + ".tmp"

            payload_tokens = []
            for t in self.tokens:
                payload_tokens.append({
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
                })

            self.config["observer_token_id"] = self.observer_token_id
            self.config["settings"] = self.settings
            self.config["tokens"] = payload_tokens

            try:
                with open(temp_path, "w", encoding="utf-8") as f:
                    json.dump(self.config, f, ensure_ascii=False, indent=2)
                os.replace(temp_path, self.config_path)
                self._last_save_time = time.time()
                self._pending_save = False
                logging.info(f"💾 Конфигурация сохранена: {self.config_path} (время: {time.strftime('%H:%M:%S')})")
            except Exception as e:
                logging.error(f"❌ Ошибка сохранения конфигурации: {e}")
                try:
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                except:
                    pass
                raise

    def get_token_by_id(self, token_id: str) -> Optional[TokenHandler]:
        return self._by_id_index.get(token_id)

    def get_token_by_name(self, name: str) -> Optional[TokenHandler]:
        return self._by_name_index.get((name or "").strip().lower())

    def _update_owner_index(self, token: TokenHandler, old_owner: int, new_owner: int):
        with self._lock:
            if old_owner in self._by_owner_index:
                self._by_owner_index[old_owner] = [t for t in self._by_owner_index[old_owner] if t.id != token.id]
                if not self._by_owner_index[old_owner]:
                    del self._by_owner_index[old_owner]
            self._by_owner_index.setdefault(new_owner, []).append(token)

    def get_token_by_sender_id(self, sender_id: int) -> Optional[TokenHandler]:
        # 1) быстрый путь по индексу владельца
        if sender_id in self._by_owner_index:
            for t in self._by_owner_index[sender_id]:
                if t.owner_vk_id == sender_id:
                    return t

        # 2) fallback: попробуем "лениво" определить владельца у токенов с owner_vk_id==0
        unknown_owner_tokens = self._by_owner_index.get(0, [])
        # ограничение чтобы не делать много users.get за один вызов
        for t in unknown_owner_tokens[:5]:
            old_owner = t.owner_vk_id
            uid = t.fetch_owner_id_lazy()
            if uid and uid != old_owner:
                self._update_owner_index(t, old_owner, uid)
                if uid == sender_id:
                    return t

        # 3) повторим быстрый поиск после возможного обновления индекса
        if sender_id in self._by_owner_index:
            for t in self._by_owner_index[sender_id]:
                if t.owner_vk_id == sender_id:
                    return t

        return None

    def get_observer(self) -> TokenHandler:
        if not self.observer_token_id:
            raise RuntimeError("observer_token_id is not set in config.json")
        t = self.get_token_by_id(self.observer_token_id)
        if not t:
            raise RuntimeError(f"observer_token_id='{self.observer_token_id}' not found in tokens[]")
        return t

    def all_buffers(self) -> List[TokenHandler]:
        obs = self.get_observer()
        return [t for t in self.tokens if t.id != obs.id]

    def get_apostles_with_race(self, race_key: str) -> List[TokenHandler]:
        """Получить апостолов с определенной расой, исключая Observer"""
        obs = self.get_observer()
        result = []
        for t in self._apostles_by_race_index.get(race_key, []):
            if t.id != obs.id:  # ← Исключаем Observer
                result.append(t)
        return result

    def update_race_index(self, token: TokenHandler):
        if token.class_type != "apostle":
            return

        for race in RACE_NAMES.keys():
            if token in self._apostles_by_race_index.get(race, []):
                self._apostles_by_race_index[race] = [t for t in self._apostles_by_race_index[race] if t.id != token.id]

        token._cleanup_expired_temp_races(force=True)

        for race in token.races:
            if race in self._apostles_by_race_index:
                self._apostles_by_race_index[race].append(token)

        for tr in token.temp_races:
            race = tr["race"]
            if race in self._apostles_by_race_index:
                self._apostles_by_race_index[race].append(token)
