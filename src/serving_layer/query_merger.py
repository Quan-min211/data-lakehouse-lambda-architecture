"""
query_merger.py
================
TRÁI TIM CỦA KIẾN TRÚC LAMBDA NÂNG CẤP:
Auto-Correcting Query Merger (Bộ Ghép Nối Dữ Liệu Tự Động Sửa Sai).

Thuật toán tự động phân luồng truy vấn theo mốc System Watermark (W):
- CASE 1 (History):   T_end <= W  --> 100% Batch View (Reconciled)
- CASE 2 (Realtime):  T_start > W --> 100% Speed View (Provisional)
- CASE 3 (Hybrid):    T_start <= W < T_end --> Cắt đôi cửa sổ:
                      * [T_start, W]  lấy từ Batch View (Reconciled)
                      * (W, T_end]    lấy từ Speed View (Provisional)
                      * Ghép nối bảo đảm ZERO DOUBLE-COUNTING!
                      * Tính sai số hiệu chỉnh: delta = |VWAP_speed - VWAP_batch|
"""

from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from src.serving_layer.schemas import (
    CandleData,
    CandleStatus,
    QueryCase,
    MarketDataResponse,
    TimeRange,
    ReconciliationDetail,
)
from src.serving_layer.watermark_reader import WatermarkReader
from src.serving_layer.clickhouse_client import ClickHouseQueryClient
from src.utils.alerting import get_alert_manager
from src.utils.logger import setup_logger

logger = setup_logger("query_merger")


class AutoCorrectingQueryMerger:
    """Hiện thực hóa thuật toán Auto-Correcting Query Merger."""

    def __init__(
        self,
        watermark_reader: Optional[WatermarkReader] = None,
        ch_client: Optional[ClickHouseQueryClient] = None,
    ):
        self.watermark_reader = watermark_reader or WatermarkReader()
        self.ch_client = ch_client or ClickHouseQueryClient()

    @staticmethod
    def _as_candles(
        records: List[Dict[str, Any]],
        status: CandleStatus,
    ) -> List[CandleData]:
        """Convert raw ClickHouse rows to typed CandleData records."""
        return [CandleData(**record, status=status) for record in records]

    @staticmethod
    def _dedupe_prefer_batch(candles: List[CandleData]) -> tuple[List[CandleData], int]:
        """
        Deduplicate candles by (symbol, window_start), preferring reconciled Batch data.

        Returns:
            tuple of (deduplicated_candles, duplicate_count).
        """
        by_key: Dict[tuple[str, datetime], CandleData] = {}
        duplicate_count = 0

        for candle in candles:
            key = (candle.symbol.upper(), candle.window_start)
            existing = by_key.get(key)
            if existing is None:
                by_key[key] = candle
                continue

            duplicate_count += 1
            if existing.status != CandleStatus.RECONCILED and candle.status == CandleStatus.RECONCILED:
                by_key[key] = candle

        return sorted(by_key.values(), key=lambda c: c.window_start), duplicate_count

    def merge_query(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
    ) -> MarketDataResponse:
        """
        Truy vấn và tự động ghép nối dữ liệu nến cho cặp coin và khoảng thời gian yêu cầu.
        """
        if start_time >= end_time:
            raise ValueError(f"start_time ({start_time}) phải nhỏ hơn end_time ({end_time})")

        # Đảm bảo timezone UTC
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)

        symbol = symbol.upper()
        # 1. Đọc mốc Watermark hiện tại của Batch Layer
        watermark = self.watermark_reader.get_watermark(layer="batch_layer")

        logger.info(
            f"Query Merger nhận request: {symbol} | Range: [{start_time} -> {end_time}] | Watermark: {watermark}"
        )

        candles: List[CandleData] = []
        reconciliation_delta: Optional[float] = None
        source_breakdown = {"batch": 0, "speed": 0, "deduplicated": 0}

        # ---------------------------------------------------------------------
        # CASE 1: TOÀN BỘ LÀ DỮ LIỆU LỊCH SỬ (T_end <= Watermark)
        # ---------------------------------------------------------------------
        if end_time <= watermark:
            query_case = QueryCase.CASE_1_HISTORY
            case_desc = (
                "Case 1 (History): 100% dữ liệu từ Batch View (Ground Truth, chuẩn xác tuyệt đối)."
            )
            overall_status = CandleStatus.RECONCILED
            reconciliation_delta = 0.0

            raw_batch = self.ch_client.query_candles(
                table="batch_agg",
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
            )
            candles.extend(self._as_candles(raw_batch, CandleStatus.RECONCILED))
            source_breakdown["batch"] = len(raw_batch)

        # ---------------------------------------------------------------------
        # CASE 2: TOÀN BỘ LÀ DỮ LIỆU THỜI GIAN THỰC (T_start > Watermark)
        # ---------------------------------------------------------------------
        elif start_time >= watermark:
            query_case = QueryCase.CASE_2_REALTIME
            case_desc = (
                "Case 2 (Realtime): 100% dữ liệu từ Speed View (Provisional, tức thời từ luồng streaming)."
            )
            overall_status = CandleStatus.PROVISIONAL
            reconciliation_delta = None

            raw_speed = self.ch_client.query_candles(
                table="speed_agg",
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
            )
            candles.extend(self._as_candles(raw_speed, CandleStatus.PROVISIONAL))
            source_breakdown["speed"] = len(raw_speed)

        # ---------------------------------------------------------------------
        # CASE 3: GIAO THOA LAI (T_start <= Watermark < T_end)
        # ---------------------------------------------------------------------
        else:
            query_case = QueryCase.CASE_3_HYBRID
            case_desc = (
                "Case 3 (Hybrid): Tự động phân tách tại Watermark. "
                "Cửa sổ [T_start, Watermark) lấy từ Batch (Reconciled) + [Watermark, T_end) lấy từ Speed (Provisional). "
                "Cam kết ZERO DOUBLE-COUNTING!"
            )
            overall_status = CandleStatus.PARTIALLY_RECONCILED

            # 3.1: Truy vấn phần lịch sử từ Batch Layer: [start_time, watermark)
            raw_batch = self.ch_client.query_candles(
                table="batch_agg",
                symbol=symbol,
                start_time=start_time,
                end_time=watermark,
            )
            candles.extend(self._as_candles(raw_batch, CandleStatus.RECONCILED))
            source_breakdown["batch"] = len(raw_batch)

            # 3.2: Truy vấn phần tức thời từ Speed Layer: [watermark, end_time)
            raw_speed = self.ch_client.query_candles(
                table="speed_agg",
                symbol=symbol,
                start_time=watermark,
                end_time=end_time,
            )
            candles.extend(self._as_candles(raw_speed, CandleStatus.PROVISIONAL))
            source_breakdown["speed"] = len(raw_speed)

            # 3.3: Tính sai số đối soát (Reconciliation Delta) tại vùng biên Watermark
            # Lấy nến tại hoặc sát mốc Watermark để so sánh sai lệch giữa Speed và Batch
            boundary_batch = self.ch_client.query_candles(
                table="batch_agg",
                symbol=symbol,
                start_time=start_time,
                end_time=watermark,
            )
            boundary_speed = self.ch_client.query_candles(
                table="speed_agg",
                symbol=symbol,
                start_time=start_time,
                end_time=watermark,
            )
            if boundary_batch and boundary_speed:
                last_b = boundary_batch[-1]
                last_s = boundary_speed[-1]
                vwap_b = float(last_b.get("vwap", 0.0))
                vwap_s = float(last_s.get("vwap", 0.0))
                reconciliation_delta = round(abs(vwap_s - vwap_b), 4)

        candles, duplicate_count = self._dedupe_prefer_batch(candles)
        source_breakdown["deduplicated"] = duplicate_count
        freshness_lag_seconds = max((end_time - watermark).total_seconds(), 0.0)
        get_alert_manager().check_streaming_lag(symbol, freshness_lag_seconds)

        return MarketDataResponse(
            symbol=symbol,
            time_range=TimeRange(start=start_time, end=end_time),
            query_case=query_case,
            query_case_description=case_desc,
            watermark=watermark,
            overall_status=overall_status,
            reconciliation_delta=reconciliation_delta,
            candles_count=len(candles),
            candles=candles,
            source_breakdown=source_breakdown,
            freshness_lag_seconds=freshness_lag_seconds,
        )

    def get_reconciliation_report(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
    ) -> List[ReconciliationDetail]:
        """
        Báo cáo đối soát chi tiết so sánh song song giữa Batch View và Speed View
        phục vụ Benchmark 2 (Reconciliation Accuracy) và Dashboard Audit.
        """
        batch_candles = {
            c["window_start"]: c
            for c in self.ch_client.query_candles("batch_agg", symbol, start_time, end_time)
        }
        speed_candles = {
            c["window_start"]: c
            for c in self.ch_client.query_candles("speed_agg", symbol, start_time, end_time)
        }

        all_windows = sorted(set(list(batch_candles.keys()) + list(speed_candles.keys())))
        report = []

        for w in all_windows:
            b = batch_candles.get(w)
            s = speed_candles.get(w)

            b_vwap = float(b["vwap"]) if b else None
            s_vwap = float(s["vwap"]) if s else None
            vwap_delta = round(abs(s_vwap - b_vwap), 4) if (s_vwap is not None and b_vwap is not None) else None

            b_vol = float(b["volume"]) if b else None
            s_vol = float(s["volume"]) if s else None
            vol_delta = round(abs(s_vol - b_vol), 6) if (s_vol is not None and b_vol is not None) else None

            w_end = (b or s)["window_end"]

            vol_drift_pct: Optional[float] = None
            if b_vol is not None and s_vol is not None:
                if b_vol > 0:
                    vol_drift_pct = round(abs(s_vol - b_vol) / b_vol * 100.0, 4)
                elif s_vol == 0:
                    vol_drift_pct = 0.0

            if vol_drift_pct is not None:
                get_alert_manager().check_volume_drift(symbol, vol_drift_pct, str(w))

            if b is not None and s is not None:
                is_matched = (
                    (vwap_delta is not None and vwap_delta < 1e-4) and
                    (vol_delta is not None and vol_delta < 1e-6)
                )
                recon_status = "MATCHED" if is_matched else "RECONCILED_AND_HEALED"
            elif b is not None:
                recon_status = "BATCH_ONLY"
            else:
                recon_status = "PROVISIONAL_ONLY"

            report.append(
                ReconciliationDetail(
                    window_start=w,
                    window_end=w_end,
                    batch_vwap=b_vwap,
                    speed_vwap=s_vwap,
                    vwap_delta=vwap_delta,
                    batch_volume=b_vol,
                    speed_volume=s_vol,
                    volume_delta=vol_delta,
                    volume_drift_pct=vol_drift_pct,
                    reconciliation_status=recon_status,
                    is_reconciled=(b is not None),
                )
            )

        return report
