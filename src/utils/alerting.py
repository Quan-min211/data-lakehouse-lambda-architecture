"""
alerting.py
===========
Module giám sát cảnh báo bất thường chủ động (Proactive Anomaly Alerting Gate)
cho Lambda Lakehouse Architecture.

Tự động phát hiện và phát tín hiệu cảnh báo khi:
  1. Sai lệch khối lượng đối soát (Volume Drift ΔV%) vượt ngưỡng (> 5%).
  2. Số lượng bản ghi bị cách ly (Quarantine Spike) tăng đột biến.
  3. Độ trễ Watermark (Freshness Lag = T_end - W) vượt chu kỳ Batch:
     WARNING khi > 900 giây (15 phút), CRITICAL khi >= 1800 giây (30 phút).
  4. Mất chuỗi Sequence ID liên tiếp (sequence_gap_count > 0 trong reconciliation_audit_metrics).
  5. Biến động giá bất thường trong nến OHLCV (Price Spike: is_spike == 1).
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
# Do tre Watermark: Batch nang W moi 15 phut, nen lag duoi 900 s la binh thuong.
# Ten bien cu ALERT_LAG_THRESHOLD_SECONDS van duoc chap nhan cho nguong WARNING.
DEFAULT_LAG_WARNING_SECONDS = float(
    os.getenv("ALERT_LAG_WARNING_SECONDS", os.getenv("ALERT_LAG_THRESHOLD_SECONDS", "900.0"))
)
DEFAULT_LAG_CRITICAL_SECONDS = float(os.getenv("ALERT_LAG_CRITICAL_SECONDS", "1800.0"))
# Ty le hut chuoi (%) tu do canh bao SEQUENCE_GAP chuyen tu WARNING sang CRITICAL
DEFAULT_GAP_CRITICAL_PCT = float(os.getenv("ALERT_GAP_CRITICAL_PCT", "1.0"))
# Giu ten cu de tuong thich nguoc voi ma goi
DEFAULT_LAG_THRESHOLD_SECONDS = DEFAULT_LAG_WARNING_SECONDS
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "")
# Số nến tối đa được ghi nhớ để khử trùng cảnh báo PRICE_SPIKE
MAX_SPIKE_ALERT_KEYS = 5000


@dataclass
class AnomalyAlert:
    """Bản ghi cảnh báo bất thường hệ thống."""
    alert_id: str
    alert_type: str  # VOLUME_DRIFT, QUARANTINE_SPIKE, STREAMING_LAG, SEQUENCE_GAP, PRICE_SPIKE
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
        # Các nến (symbol, window_start) đã phát PRICE_SPIKE, giữ theo thứ tự chèn
        self._spike_alerted: Dict[tuple, None] = {}
        # Cac batch_run_id da phat QUARANTINE_SPIKE (moi lo chi canh bao mot lan)
        self._quarantine_alerted: Dict[str, None] = {}
        # Cac (run_id, symbol) da phat SEQUENCE_GAP
        self._gap_alerted: Dict[tuple, None] = {}
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
        batch_run_id: Optional[str] = None,
    ) -> Optional[AnomalyAlert]:
        """Kiểm tra số bản ghi bị cách ly của một lô Batch tăng vọt.

        Args:
            quarantined_count: Số bản ghi bị cách ly trong lô.
            total_count: Tổng số bản ghi của lô (cách ly + hợp lệ).
            threshold: Ngưỡng số bản ghi cách ly kích hoạt cảnh báo.
            batch_run_id: Mã lô; nếu có, mỗi lô chỉ phát cảnh báo một lần
                (Dashboard gọi lặp lại mỗi chu kỳ làm mới).

        Returns:
            Optional[AnomalyAlert]: Cảnh báo vừa phát, hoặc None.
        """
        if quarantined_count < threshold:
            return None
        if batch_run_id:
            with self._lock:
                if batch_run_id in self._quarantine_alerted:
                    return None
                self._quarantine_alerted[batch_run_id] = None
                if len(self._quarantine_alerted) > MAX_SPIKE_ALERT_KEYS:
                    self._quarantine_alerted.pop(next(iter(self._quarantine_alerted)))
        rejection_pct = round(quarantined_count / max(total_count, 1) * 100.0, 2)
        return self.trigger_alert(
            alert_type="QUARANTINE_SPIKE",
            severity="CRITICAL",
            symbol="GLOBAL",
            message=f"Số lượng bản ghi bị cách ly ({quarantined_count}) tăng đột biến ({rejection_pct}% tổng số).",
            metrics={
                "quarantined_count": quarantined_count,
                "total_count": total_count,
                "rejection_pct": rejection_pct,
                "batch_run_id": batch_run_id or "",
            },
        )

    def check_sequence_gap(
        self,
        symbol: str,
        sequence_gap_count: int,
        sequence_gap_rate: float = 0.0,
        run_id: Optional[str] = None,
        critical_pct: float = DEFAULT_GAP_CRITICAL_PCT,
    ) -> Optional[AnomalyAlert]:
        """Cảnh báo mất chuỗi trade_id (Sequence Gap) từ bảng kiểm toán đối soát.

        Args:
            symbol: Cặp giao dịch.
            sequence_gap_count: Tổng số trade_id bị thiếu (sequence_gap_count) của lô.
            sequence_gap_rate: Tỷ lệ hụt chuỗi R_gap (%) lớn nhất của lô.
            run_id: Mã lô kiểm toán; mỗi (run_id, symbol) chỉ cảnh báo một lần.
            critical_pct: R_gap (%) từ đó cảnh báo là CRITICAL.

        Returns:
            Optional[AnomalyAlert]: Cảnh báo vừa phát, hoặc None nếu không có khoảng hở.
        """
        if sequence_gap_count <= 0:
            return None
        if run_id:
            key = (run_id, symbol.upper())
            with self._lock:
                if key in self._gap_alerted:
                    return None
                self._gap_alerted[key] = None
                if len(self._gap_alerted) > MAX_SPIKE_ALERT_KEYS:
                    self._gap_alerted.pop(next(iter(self._gap_alerted)))
        severity = "CRITICAL" if sequence_gap_rate >= critical_pct else "WARNING"
        return self.trigger_alert(
            alert_type="SEQUENCE_GAP",
            severity=severity,
            symbol=symbol,
            message=(
                f"Phát hiện {sequence_gap_count} trade_id bị thiếu trong chuỗi "
                f"(R_gap = {sequence_gap_rate:.4f}%)."
            ),
            metrics={
                "sequence_gap_count": sequence_gap_count,
                "sequence_gap_rate": sequence_gap_rate,
                "critical_pct": critical_pct,
                "run_id": run_id or "",
            },
        )

    def check_streaming_lag(
        self,
        symbol: str,
        lag_seconds: float,
        threshold_seconds: float = DEFAULT_LAG_WARNING_SECONDS,
        critical_seconds: float = DEFAULT_LAG_CRITICAL_SECONDS,
    ) -> Optional[AnomalyAlert]:
        """Kiểm tra độ trễ Watermark (freshness lag = T_end - W).

        WARNING khi lag > threshold_seconds (mặc định 900 s = một chu kỳ Batch),
        CRITICAL khi lag >= critical_seconds (mặc định 1800 s = lỡ hai chu kỳ).
        """
        if lag_seconds <= threshold_seconds:
            return None
        severity = "CRITICAL" if lag_seconds >= critical_seconds else "WARNING"
        return self.trigger_alert(
            alert_type="STREAMING_LAG",
            severity=severity,
            symbol=symbol,
            message=(
                f"Độ trễ Watermark (freshness lag = {lag_seconds:.1f}s) vượt ngưỡng "
                f"{threshold_seconds:.0f}s (CRITICAL từ {critical_seconds:.0f}s)."
            ),
            metrics={
                "lag_seconds": lag_seconds,
                "threshold_seconds": threshold_seconds,
                "critical_seconds": critical_seconds,
            },
        )

    def check_price_spike(
        self,
        symbol: str,
        candle: Dict[str, Any],
    ) -> Optional[AnomalyAlert]:
        """Kiểm tra và phát cảnh báo bất thường biến động giá (Price Spike).

        Kích hoạt khi candle có trường is_spike == 1 hoặc is_spike is True.
        Mỗi cây nến (symbol, window_start) chỉ phát cảnh báo một lần, để các
        truy vấn lặp lại của Dashboard không sinh cảnh báo trùng.

        Args:
            symbol (str): Cặp giao dịch.
            candle (dict): Nến OHLCV có trường is_spike.

        Returns:
            Optional[AnomalyAlert]: Cảnh báo vừa phát, hoặc None nếu không phát.
        """
        is_spike_val = candle.get("is_spike")
        if is_spike_val is True or is_spike_val == 1 or str(is_spike_val) == "1":
            open_p = float(candle.get("open_price", candle.get("open", 0.0)))
            close_p = float(candle.get("close_price", candle.get("close", 0.0)))
            high_p = float(candle.get("high_price", candle.get("high", 0.0)))
            low_p = float(candle.get("low_price", candle.get("low", 0.0)))
            volume = float(candle.get("volume", 0.0))
            window_start = str(candle.get("window_start", ""))

            dedupe_key = (symbol.upper(), window_start)
            with self._lock:
                if dedupe_key in self._spike_alerted:
                    return None
                self._spike_alerted[dedupe_key] = None
                if len(self._spike_alerted) > MAX_SPIKE_ALERT_KEYS:
                    self._spike_alerted.pop(next(iter(self._spike_alerted)))

            amplitude_pct = (
                round(abs(close_p - open_p) / open_p * 100.0, 3)
                if open_p > 0
                else 0.0
            )
            range_pct = (
                round((high_p - low_p) / low_p * 100.0, 3)
                if low_p > 0
                else 0.0
            )

            severity = "CRITICAL" if amplitude_pct >= 5.0 or range_pct >= 6.0 else "WARNING"
            return self.trigger_alert(
                alert_type="PRICE_SPIKE",
                severity=severity,
                symbol=symbol,
                message=(
                    f"Phát hiện biến động giá bất thường (Price Spike) cho {symbol}: "
                    f"Biên độ Đóng–Mở = {amplitude_pct}%, Biên độ Đỉnh–Đáy = {range_pct}%."
                ),
                metrics={
                    "window_start": window_start,
                    "open_price": open_p,
                    "close_price": close_p,
                    "high_price": high_p,
                    "low_price": low_p,
                    "volume": volume,
                    "amplitude_pct": amplitude_pct,
                    "range_pct": range_pct,
                },
            )
        return None

    def get_recent_alerts(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Trả về danh sách các cảnh báo gần nhất."""
        with self._lock:
            return [asdict(a) for a in list(self._history)[:limit]]


# Singleton helper functions
def get_alert_manager() -> AlertManager:
    return AlertManager.get_instance()
