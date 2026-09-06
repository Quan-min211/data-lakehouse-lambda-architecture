"""
api_routes.py
=============
FastAPI REST API cho Tầng Phục Vụ (Serving Layer).
Cung cấp các endpoint:
- GET /health
- GET /api/watermark
- GET /api/market: Hợp nhất nến thời gian thực & lịch sử (Query Merger)
- GET /api/reconciliation: Báo cáo đối soát chi tiết phục vụ Benchmark 2
"""

from datetime import datetime, timezone, timedelta
from typing import Optional, List

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from src.serving_layer.schemas import (
    MarketDataResponse,
    WatermarkResponse,
    ReconciliationDetail,
)
from src.serving_layer.query_merger import AutoCorrectingQueryMerger
from src.serving_layer.watermark_reader import WatermarkReader
from src.serving_layer.clickhouse_client import ClickHouseQueryClient

app = FastAPI(
    title="Lambda Lakehouse Serving API",
    description=(
        "Hệ thống API Phục Vụ dữ liệu thị trường Crypto theo kiến trúc Lambda "
        "tích hợp bộ ghép nối tự động sửa sai (Auto-Correcting Query Merger)."
    ),
    version="1.0.0",
)

# Kích hoạt CORS cho phép Streamlit Dashboard (Port 8501) kết nối
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Khởi tạo singletons
_watermark_reader = WatermarkReader()
_ch_client = ClickHouseQueryClient()
query_merger = AutoCorrectingQueryMerger(
    watermark_reader=_watermark_reader,
    ch_client=_ch_client,
)


def parse_datetime_param(dt_str: Optional[str], default_offset_minutes: int = 0) -> datetime:
    """Helper phân tích chuỗi thời gian (ISO hoặc epoch ms) sang datetime UTC."""
    if not dt_str:
        return datetime.now(timezone.utc) + timedelta(minutes=default_offset_minutes)

    try:
        # Thử parse epoch milliseconds
        if dt_str.isdigit():
            epoch_ms = int(dt_str)
            return datetime.fromtimestamp(epoch_ms / 1000.0, timezone.utc)
        # Parse ISO format
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Định dạng thời gian không hợp lệ: '{dt_str}'. Vui lòng dùng ISO format (ví dụ: '2026-09-04T12:00:00Z') hoặc epoch ms.",
        )


@app.get("/health", tags=["System"])
async def health_check():
    """Kiểm tra trạng thái sẵn sàng của Serving Layer."""
    return {
        "status": "healthy",
        "service": "serving_layer",
        "version": "1.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/", tags=["System"])
async def root():
    return {
        "message": "Lambda Lakehouse Serving Layer API — Auto-Correcting Query Merger",
        "docs_url": "/docs",
        "version": "1.0.0",
    }


@app.get("/api/watermark", response_model=WatermarkResponse, tags=["Metadata"])
async def get_watermark(layer: str = Query("batch_layer", description="Tên layer")):
    """Lấy mốc System Watermark hiện tại từ ClickHouse."""
    wm = _watermark_reader.get_watermark(layer=layer)
    return WatermarkResponse(
        layer=layer,
        watermark_time=wm,
        updated_at=datetime.now(timezone.utc),
        status="ACTIVE",
    )


@app.get("/api/market", response_model=MarketDataResponse, tags=["Market Data"])
async def get_market_data(
    symbol: str = Query("BTCUSDT", description="Cặp coin cần truy vấn (ví dụ: BTCUSDT)"),
    start_time: Optional[str] = Query(None, description="Thời gian bắt đầu (ISO hoặc epoch ms)"),
    end_time: Optional[str] = Query(None, description="Thời gian kết thúc (ISO hoặc epoch ms)"),
):
    """
    Truy vấn dữ liệu nến OHLCV qua bộ Auto-Correcting Query Merger:
    - Case 1: 100% Lịch sử (Reconciled)
    - Case 2: 100% Tức thời (Provisional)
    - Case 3: Giao thoa Lai (Partially Reconciled) — Zero Double-Counting
    """
    # Mặc định lấy 60 phút gần nhất nếu không truyền start/end
    end_dt = parse_datetime_param(end_time, default_offset_minutes=0)
    start_dt = parse_datetime_param(start_time, default_offset_minutes=-60)

    if start_dt >= end_dt:
        raise HTTPException(
            status_code=400,
            detail=f"start_time ({start_dt.isoformat()}) phải nhỏ hơn end_time ({end_dt.isoformat()})",
        )

    response = query_merger.merge_query(
        symbol=symbol,
        start_time=start_dt,
        end_time=end_dt,
    )
    return response


@app.get("/api/reconciliation", response_model=List[ReconciliationDetail], tags=["Audit & Benchmark"])
async def get_reconciliation(
    symbol: str = Query("BTCUSDT", description="Cặp coin cần đối soát"),
    start_time: Optional[str] = Query(None, description="Thời gian bắt đầu"),
    end_time: Optional[str] = Query(None, description="Thời gian kết thúc"),
):
    """
    Báo cáo đối soát chi tiết so sánh song song giữa Batch View và Speed View
    phục vụ Benchmark 2 (Reconciliation Accuracy) và kiểm toán chất lượng dữ liệu.
    """
    end_dt = parse_datetime_param(end_time, default_offset_minutes=0)
    start_dt = parse_datetime_param(start_time, default_offset_minutes=-60)

    report = query_merger.get_reconciliation_report(
        symbol=symbol,
        start_time=start_dt,
        end_time=end_dt,
    )
    return report
