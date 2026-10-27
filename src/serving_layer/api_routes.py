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

import json
import socket
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any

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
from src.utils.config import get_kafka_config, get_redis_config, get_clickhouse_config

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


def _tcp_status(name: str, host: str, port: int, timeout: float = 0.3) -> Dict[str, Any]:
    """Check TCP connectivity for a dependency."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return {"name": name, "status": "ok", "host": host, "port": port}
    except Exception as exc:
        return {
            "name": name,
            "status": "unavailable",
            "host": host,
            "port": port,
            "error": str(exc),
        }


def _clickhouse_status() -> Dict[str, Any]:
    """Check ClickHouse TCP availability without blocking on a query client."""
    cfg = get_clickhouse_config()
    status = _tcp_status("clickhouse", cfg.host, cfg.port)
    status["database"] = cfg.database
    return status


def _redis_status() -> Dict[str, Any]:
    """Check Redis TCP availability without making it required for API health."""
    cfg = get_redis_config()
    return _tcp_status("redis", cfg.host, cfg.port)


def _kafka_status() -> Dict[str, Any]:
    """Check Kafka bootstrap TCP availability."""
    cfg = get_kafka_config()
    bootstrap = cfg.bootstrap_servers.split(",")[0].strip()
    host, _, port_s = bootstrap.partition(":")
    try:
        port = int(port_s or "9092")
    except ValueError:
        port = 9092
    return _tcp_status("kafka", host, port)


@app.get("/health", tags=["System"])
async def health_check():
    """Kiểm tra trạng thái sẵn sàng của Serving Layer."""
    return {
        "status": "healthy",
        "service": "serving_layer",
        "version": "1.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/system/status", tags=["System"])
async def get_system_status():
    """Return dependency and freshness status for dashboard/demo readiness."""
    now = datetime.now(timezone.utc)
    watermark = _watermark_reader.get_watermark(layer="batch_layer")
    if watermark.tzinfo is None:
        watermark = watermark.replace(tzinfo=timezone.utc)

    dependencies = [
        _clickhouse_status(),
        _redis_status(),
        _kafka_status(),
    ]
    required_ok = all(
        item["status"] == "ok"
        for item in dependencies
        if item["name"] in {"clickhouse", "kafka"}
    )
    overall = "ok" if required_ok else "degraded"

    return {
        "status": overall,
        "service": "serving_layer",
        "timestamp": now.isoformat(),
        "watermark": {
            "layer": "batch_layer",
            "watermark_time": watermark.isoformat(),
            "freshness_lag_seconds": max((now - watermark).total_seconds(), 0.0),
        },
        "dependencies": dependencies,
    }


@app.get("/api/speed/latest", tags=["Market Data"])
async def get_latest_speed_candle(
    symbol: str = Query("BTCUSDT", description="Cặp coin cần đọc từ Redis hot cache"),
):
    """Read the latest provisional Speed candle from Redis hot cache."""
    cfg = get_redis_config()
    key = f"speed:candle:{symbol.upper()}:latest"
    try:
        import redis
        client = redis.Redis(
            host=cfg.host,
            port=cfg.port,
            password=cfg.password or None,
            decode_responses=True,
            socket_connect_timeout=0.3,
            socket_timeout=0.3,
        )
        raw = client.get(key)
        if raw is None:
            return {
                "symbol": symbol.upper(),
                "cache_status": "miss",
                "redis_key": key,
                "candle": None,
            }
        return {
            "symbol": symbol.upper(),
            "cache_status": "hit",
            "redis_key": key,
            "candle": json.loads(raw),
        }
    except Exception as exc:
        return {
            "symbol": symbol.upper(),
            "cache_status": "unavailable",
            "redis_key": key,
            "error": str(exc),
            "candle": None,
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


@app.get("/api/system/alerts", tags=["System"])
async def get_system_alerts(limit: int = Query(50, ge=1, le=200, description="Số lượng cảnh báo gần nhất")):
    """
    Truy vấn danh sách cảnh báo bất thường chủ động (Proactive Anomaly Alerts):
    - Volume Drift Anomaly (> 5%)
    - Quarantine Spike Anomaly
    - Streaming Lag SLA Anomaly
    """
    from src.utils.alerting import get_alert_manager
    manager = get_alert_manager()
    alerts = manager.get_recent_alerts(limit=limit)
    return {
        "status": "ok",
        "total_alerts": len(alerts),
        "alerts": alerts,
        "queried_at": datetime.now(timezone.utc).isoformat(),
    }

