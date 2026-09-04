"""
schemas.py
==========
Pydantic Schemas cho Tầng Phục Vụ (Serving Layer).
Định nghĩa cấu trúc dữ liệu phản hồi cho Dashboard và các bên tiêu thụ API:
- CandleData: Dữ liệu nến OHLCV kèm trạng thái Reconciled / Provisional / Partially Reconciled
- MarketDataResponse: Kết quả truy vấn hợp nhất qua Query Merger
- WatermarkResponse: Mốc Watermark hiện tại của Batch Layer
- ReconciliationDetail: Báo cáo đối soát chi tiết giữa Speed View và Batch View
"""

from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field


class CandleStatus(str, Enum):
    """Trạng thái dữ liệu nến trong kiến trúc Lambda."""
    RECONCILED = "Reconciled"              # Dữ liệu chính xác tuyệt đối từ Batch Layer (đã khử trùng & DQ Gate)
    PROVISIONAL = "Provisional"            # Dữ liệu tức thời từ Speed Layer (có thể chứa late data/nhiễu)
    PARTIALLY_RECONCILED = "Partially Reconciled"  # Dữ liệu trong vùng giao thoa giữa Batch và Speed


class QueryCase(int, Enum):
    """3 kịch bản phân luồng của Auto-Correcting Query Merger."""
    CASE_1_HISTORY = 1     # T_end <= Watermark (100% Batch View)
    CASE_2_REALTIME = 2    # T_start > Watermark (100% Speed View)
    CASE_3_HYBRID = 3      # T_start <= Watermark < T_end (Cắt ghép Batch + Speed không trùng lặp)


class CandleData(BaseModel):
    """Thông tin chi tiết một cây nến OHLCV."""
    symbol: str
    window_start: datetime
    window_end: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    trade_count: int
    vwap: float
    is_spike: int = 0
    status: CandleStatus


class TimeRange(BaseModel):
    start: datetime
    end: datetime


class MarketDataResponse(BaseModel):
    """Phản hồi chuẩn của endpoint GET /api/market."""
    symbol: str
    time_range: TimeRange
    query_case: QueryCase
    query_case_description: str
    watermark: Optional[datetime] = None
    overall_status: CandleStatus
    reconciliation_delta: Optional[float] = Field(
        default=None,
        description="Sai số tuyệt đối |VWAP_speed - VWAP_batch| tại vùng giao thoa (Case 3)"
    )
    candles_count: int
    candles: List[CandleData]


class WatermarkResponse(BaseModel):
    """Phản hồi chuẩn của endpoint GET /api/watermark."""
    layer: str
    watermark_time: datetime
    updated_at: datetime
    status: str = "ACTIVE"


class ReconciliationDetail(BaseModel):
    """Chi tiết đối soát giữa Batch Layer và Speed Layer cho một cửa sổ thời gian."""
    window_start: datetime
    window_end: datetime
    batch_vwap: Optional[float]
    speed_vwap: Optional[float]
    vwap_delta: Optional[float]
    batch_volume: Optional[float]
    speed_volume: Optional[float]
    volume_delta: Optional[float]
    is_reconciled: bool
