# -*- coding: utf-8 -*-
import logging
import threading
import time
from typing import Any, Dict, Optional

from .constants import VK_API_VERSION

class TokenHealthMonitor:
    def __init__(self, token_manager):
        self.tm = token_manager
        self.health_data: Dict[str, Dict] = {}
        self._monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self._last_report_time = 0

        self.thresholds = {
            "min_voices": 3,
            "max_error_rate": 0.3,
            "max_captcha_time": 3600,
            "api_timeout": 10,
        }

        self._monitor_thread.start()
        logging.info("🏥 Health Monitor запущен")

    def _check_single_token(self, token) -> Dict[str, Any]:
        health = {
            "token_id": token.id,
            "token_name": token.name,
            "class": token.class_type,
            "enabled": token.enabled,
            "timestamp": time.time(),
            "status": "unknown",
            "issues": [],
            "metrics": {},
            "details": {},
        }

        if not token.enabled:
            health["status"] = "disabled"
            health["issues"].append("Токен отключен вручную")
            return health

        if token.is_captcha_paused():
            remaining = token.captcha_until - time.time()
            health["status"] = "captcha"
            health["issues"].append(f"CAPTCHA блокировка ({int(remaining)}с осталось)")
            health["details"]["captcha_remaining"] = remaining
            if remaining > self.thresholds["max_captcha_time"]:
                health["issues"].append("Долгая CAPTCHA блокировка (>1 час)")

        try:
            test_data = {
                "access_token": token.access_token,
                "v": VK_API_VERSION,
                "user_ids": "1",
                "fields": "online",
            }

            start_time = time.time()
            response = token._vk.call(token._vk.post("users.get", test_data))
            api_time = time.time() - start_time

            health["metrics"]["api_response_time"] = api_time

            if "error" in response:
                error = response["error"]
                error_code = error.get("error_code")
                error_msg = error.get("error_msg", "")

                health["status"] = "api_error"
                health["issues"].append(f"API ошибка {error_code}: {error_msg}")

                if error_code in [5, 17]:
                    health["issues"].append("Невалидный или устаревший токен")
                elif error_code == 6:
                    health["issues"].append("Превышен лимит запросов")
                elif error_code == 9:
                    health["issues"].append("Флуд-контроль")

            else:
                if health["status"] == "unknown":
                    health["status"] = "healthy"
                health["details"]["api_available"] = True
                if api_time > self.thresholds["api_timeout"]:
                    health["issues"].append(f"Медленный ответ API ({api_time:.1f}с)")

        except Exception as e:
            health["status"] = "connection_error"
            health["issues"].append(f"Ошибка соединения: {str(e)}")
            health["details"]["api_available"] = False

        health["metrics"]["voices"] = token.voices
        health["metrics"]["level"] = token.level
        health["metrics"]["temp_races_count"] = len(token.temp_races)
        health["metrics"]["successful_buffs"] = token.successful_buffs
        health["metrics"]["total_attempts"] = token.total_attempts

        if token.total_attempts > 0:
            success_rate = token.successful_buffs / token.total_attempts
            health["metrics"]["success_rate"] = success_rate
            if success_rate < (1 - self.thresholds["max_error_rate"]):
                health["issues"].append(f"Низкая успешность ({success_rate*100:.0f}%)")

        if token.voices < self.thresholds["min_voices"]:
            if token.voices == 0:
                health["status"] = "no_voices"
                health["issues"].append("Нет голосов")
            else:
                health["issues"].append(f"Мало голосов ({token.voices})")

        # cleanup temp races
        token._cleanup_expired_temp_races(force=True)

        return health

    def _take_auto_actions(self, token, health_info: Dict):
        status = health_info.get("status", "")
        issues = health_info.get("issues", [])

        if status in ["api_error", "connection_error"]:
            if "Невалидный или устаревший токен" in str(issues):
                logging.warning(f"🚨 Отключаю токен {token.name} (невалидный токен)")
                token.enabled = False
                self.tm.save()

        # ✅ FIX: если после cleanup что-то изменилось — обновляем индекс
        if token.class_type == "apostle":
            # принудительно чистим и если были изменения — sync index
            changed = token._cleanup_expired_temp_races(force=True)
            if changed:
                self.tm.update_race_index(token)

    def _generate_health_report(self):
        if not self.health_data:
            return

        total_tokens = len(self.tm.tokens)
        healthy_tokens = 0
        warning_tokens = 0
        error_tokens = 0

        issues_summary = {}

        for health in self.health_data.values():
            status = health.get("status", "unknown")
            if status == "healthy":
                healthy_tokens += 1
            elif status in ["disabled", "captcha", "no_voices"]:
                warning_tokens += 1
            elif status in ["api_error", "connection_error"]:
                error_tokens += 1

            for issue in health.get("issues", []):
                key = issue.split(":")[0] if ":" in issue else issue
                issues_summary[key] = issues_summary.get(key, 0) + 1

        logging.info("=" * 50)
        logging.info("📊 ОТЧЕТ СОСТОЯНИЯ СИСТЕМЫ")
        logging.info(f"🏥 СТАТУС ТОКЕНОВ:")
        logging.info(f"  ✅ Здоровые: {healthy_tokens}/{total_tokens}")
        logging.info(f"  ⚠️  Предупреждения: {warning_tokens}/{total_tokens}")
        logging.info(f"  ❌ Ошибки: {error_tokens}/{total_tokens}")

        if issues_summary:
            logging.info("📋 ОСНОВНЫЕ ПРОБЛЕМЫ:")
            for issue, count in sorted(issues_summary.items(), key=lambda x: x[1], reverse=True)[:3]:
                logging.info(f"  • {issue}: {count} токенов")

        total_buffs = sum(h.get("metrics", {}).get("successful_buffs", 0) for h in self.health_data.values())
        total_attempts = sum(h.get("metrics", {}).get("total_attempts", 0) for h in self.health_data.values())
        if total_attempts > 0:
            rate = total_buffs / total_attempts * 100
            logging.info(f"📈 УСПЕШНОСТЬ: {rate:.1f}% ({total_buffs}/{total_attempts})")

        logging.info("=" * 50)

    def _cleanup_old_data(self):
        now = time.time()
        max_age = 3600
        to_delete = [tid for tid, h in self.health_data.items() if now - h.get("timestamp", 0) > max_age]
        for tid in to_delete:
            del self.health_data[tid]

    def _monitoring_loop(self):
        while True:
            try:
                for token in self.tm.tokens:
                    try:
                        health_info = self._check_single_token(token)
                        self.health_data[token.id] = health_info
                        self._take_auto_actions(token, health_info)
                    except Exception as e:
                        logging.error(f"❌ Ошибка проверки токена {token.name}: {e}")

                if time.time() - self._last_report_time > 300:
                    self._generate_health_report()
                    self._last_report_time = time.time()

                self._cleanup_old_data()
                time.sleep(60)
            except Exception as e:
                logging.error(f"❌ Критическая ошибка в мониторинге: {e}")
                time.sleep(30)

    def get_detailed_report(self, token_name: Optional[str] = None) -> str:
        if token_name:
            token = self.tm.get_token_by_name(token_name)
            if not token:
                return f"❌ Токен '{token_name}' не найден"
            health = self.health_data.get(token.id)
            if not health:
                return f"ℹ️ Нет данных о токене '{token_name}'"
            return self._format_token_details(health)
        return self._generate_health_report_text()

    def _format_token_details(self, health: Dict) -> str:
        lines = [
            f"🔍 ДЕТАЛЬНАЯ ИНФОРМАЦИЯ: {health.get('token_name')}",
            f"📊 Статус: {health.get('status', 'unknown')}",
            f"🎭 Класс: {health.get('class')}",
            f"⚙️ Включен: {'✅' if health.get('enabled') else '❌'}",
            "",
        ]
        metrics = health.get("metrics", {})
        if metrics:
            lines.append("📈 МЕТРИКИ:")
            if "voices" in metrics:
                lines.append(f"  🗣️ Голоса: {metrics['voices']}")
            if "level" in metrics:
                lines.append(f"  💀 Уровень: {metrics['level']}")
            if "temp_races_count" in metrics:
                lines.append(f"  🎯 Временные расы: {metrics['temp_races_count']}")
            if "success_rate" in metrics:
                lines.append(f"  📊 Успешность: {metrics['success_rate']*100:.1f}%")
            lines.append("")

        issues = health.get("issues", [])
        if issues:
            lines.append("⚠️ ПРОБЛЕМЫ:")
            for issue in issues[:5]:
                lines.append(f"  • {issue}")
            lines.append("")
        return "\n".join(lines)

    def _generate_health_report_text(self) -> str:
        if not self.health_data:
            return "Нет данных мониторинга"
        report_lines = ["🏥 ОТЧЕТ ЗДОРОВЬЯ СИСТЕМЫ"]
        status_counts = {}
        for h in self.health_data.values():
            status = h.get("status", "unknown")
            status_counts[status] = status_counts.get(status, 0) + 1
        report_lines.append("\n📊 СТАТУС ТОКЕНОВ:")
        for status, count in sorted(status_counts.items()):
            report_lines.append(f"• {status}: {count}")
        return "\n".join(report_lines)
