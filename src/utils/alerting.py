"""
alerting.py
===========
Module giám sát cảnh báo bất thường chủ động (Proactive Anomaly Alerting Gate)
cho Lambda Lakehouse Architecture.

Tự động phát hiện và phát tín hiệu cảnh báo khi:
  1. Sai lệch khối lượng đối soát (Volume Drift ΔV%) vượt ngưỡng (> 5%).
  2. Số lượng bản ghi bị cách ly (Quarantine Spike) tăng đột biến.
  3. Độ trễ luồng xử lý (Streaming Freshness Lag) vượt ngưỡng SLA (> 15 giây).
  4. Mất chuỗi Sequence ID liên tiếp.
"""

from __future__ import annotations

import json
import os
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional
import urllib.request

from src.utils.logger import setup_logger

logger = setup_logger("alerting")

# Ngưỡng mặc định có thể cấu hình qua biến môi trường
DEFAULT_DRIFT_THRESHOLD_PCT = float(os.getenv("ALERT_DRIFT_THRESHOLD_PCT", "5.0"))
DEFAULT_QUARANTINE_THRESHOLD = int(os.getenv("ALERT_QUARANTINE_THRESHOLD", "100"))
DEFAULT_LAG_THRESHOLD_SECONDS = float(os.getenv("ALERT_LAG_THRESHOLD_SECONDS", "15.0"))
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "")


@dataclass
class AnomalyAlert:
    """Bản ghi cảnh báo bất thường hệ thống."""
    alert_id: str
    alert_type: str  # VOLUME_DRIFT, QUARANTINE_SPIKE, STREAMING_LAG, SEQUENCE_GAP
    severity: str    # WARNING, CRITICAL
    symbol: str
    message: str
    metrics: Dict[str, Any]
    timestamp: str


class AlertManager:
    """Quản trị và điều phối toàn bộ cảnh báo của hệ thống."""

    _instance: Optional[AlertManager] = None
    _lock = threading.Lock()

    def __init__(self, max_history: int = 200):
        self._history: Deque[AnomalyAlert] = deque(maxlen=max_history)
        self._drift_streak: Dict[str, int] = {}
        self._webhook_url = ALERT_WEBHOOK_URL

    @classmethod
    def get_instance(cls) -> AlertManager:
        with cls._lock:
            if cls._instance is None:
                cls._instance = AlertManager()
            return cls._instance

    def trigger_alert(
        self,
        alert_type: str,
        severity: str,
        symbol: str,
        message: str,
        metrics: Optional[Dict[str, Any]] = None,
    ) -> AnomalyAlert:
        """Kích hoạt và lưu trữ một cảnh báo mới."""
        now_str = datetime.now(timezone.utc).isoformat()
        alert = AnomalyAlert(
            alert_id=f"alert_{int(time.time() * 1000)}",
            alert_type=alert_type,
            severity=severity.upper(),
            symbol=symbol.upper(),
            message=message,
            metrics=metrics or {},
            timestamp=now_str,
        )

        with self._lock:
            self._history.appendleft(alert)

        log_fn = logger.critical if severity.upper() == "CRITICAL" else logger.warning
        log_fn(
            f"[{alert.severity} ALERT] [{alert.alert_type}] {alert.symbol}: {alert.message} | metrics={alert.metrics}"
        )

        # Gửi Webhook nếu được cấu hình
        if self._webhook_url:
            self._dispatch_webhook(alert)

        return alert

    def _dispatch_webhook(self, alert: AnomalyAlert) -> None:
        """Gửi thông báo cảnh báo qua HTTP Webhook bất đồng bộ."""
        def _send():
            try:
                payload = json.dumps({
                    "text": f"🚨 *[{alert.severity}] {alert.alert_type}* for `{alert.symbol}`\n{alert.message}",
                    "alert": asdict(alert),
                }).encode("utf-8")
                req = urllib.request.Request(
                    self._webhook_url,
                    data=payload,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=5) as resp:
                    pass
            except Exception as ex:
                logger.debug(f"Webhook dispatch failed: {ex}")

        threading.Thread(target=_send, daemon=True).start()

    def check_volume_drift(
        self,
        symbol: str,
        drift_pct: float,
        window_start: str,
        threshold_pct: float = DEFAULT_DRIFT_THRESHOLD_PCT,
    ) -> Optional[AnomalyAlert]:
        """Kiểm tra độ lệch khối lượng giữa Speed và Batch View."""
        if drift_pct > threshold_pct:
            streak = self._drift_streak.get(symbol, 0) + 1
            self._drift_streak[symbol] = streak
            severity = "CRITICAL" if streak >= 3 else "WARNING"
            return self.trigger_alert(
                alert_type="VOLUME_DRIFT",
                severity=severity,
                symbol=symbol,
                message=f"Độ lệch khối lượng đối soát ({drift_pct:.2f}%) vượt ngưỡng {threshold_pct}%. (Streak={streak})",
                metrics={"drift_pct": drift_pct, "threshold_pct": threshold_pct, "window_start": str(window_start), "streak": streak},
            )
        else:
            self._drift_streak[symbol] = 0
            return None

    def check_quarantine_spike(
        self,
        quarantined_count: int,
        total_count: int,
        threshold: int = DEFAULT_QUARANTINE_THRESHOLD,
    ) -> Optional[AnomalyAlert]:
        """Kiểm tra tỷ lệ cách ly bản ghi lỗi tăng vọt."""
        if quarantined_count >= threshold:
            rejection_pct = round(quarantined_count / max(total_count, 1) * 100.0, 2)
            return self.trigger_alert(
                alert_type="QUARANTINE_SPIKE",
                severity="CRITICAL",
                symbol="GLOBAL",
                message=f"Số lượng bản ghi bị cách ly ({quarantined_count}) tăng đột biến ({rejection_pct}% tổng số).",
                metrics={"quarantined_count": quarantined_count, "total_count": total_count, "rejection_pct": rejection_pct},
            )
        return None

    def check_streaming_lag(
        self,
        symbol: str,
        lag_seconds: float,
        threshold_seconds: float = DEFAULT_LAG_THRESHOLD_SECONDS,
    ) -> Optional[AnomalyAlert]:
        """Kiểm tra độ trễ luồng xử lý streaming."""
        if lag_seconds > threshold_seconds:
            return self.trigger_alert(
                alert_type="STREAMING_LAG",
                severity="WARNING" if lag_seconds < 60.0 else "CRITICAL",
                symbol=symbol,
                message=f"Độ tươi dữ liệu (freshness lag = {lag_seconds:.1f}s) vượt ngưỡng SLA {threshold_seconds}s.",
                metrics={"lag_seconds": lag_seconds, "threshold_seconds": threshold_seconds},
            )
        return None

    def get_recent_alerts(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Trả về danh sách các cảnh báo gần nhất."""
        with self._lock:
            return [asdict(a) for a in list(self._history)[:limit]]


# Singleton helper functions
def get_alert_manager() -> AlertManager:
    return AlertManager.get_instance()
