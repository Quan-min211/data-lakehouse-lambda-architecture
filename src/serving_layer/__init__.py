"""
Serving Layer Package
=====================
Auto-Correcting Query Merger & REST API:
- AutoCorrectingQueryMerger: Thuật toán phân luồng và ghép nối dữ liệu 3 Case
- WatermarkReader: Đọc mốc Watermark từ ClickHouse
- ClickHouseQueryClient: Truy vấn Batch Views và Speed Views
- app: FastAPI application instance
"""

from src.serving_layer.schemas import (
    CandleData,
    CandleStatus,
    QueryCase,
    MarketDataResponse,
    WatermarkResponse,
    ReconciliationDetail,
)
from src.serving_layer.watermark_reader import WatermarkReader
from src.serving_layer.clickhouse_client import ClickHouseQueryClient
from src.serving_layer.query_merger import AutoCorrectingQueryMerger
from src.serving_layer.api_routes import app

__all__ = [
    "CandleData",
    "CandleStatus",
    "QueryCase",
    "MarketDataResponse",
    "WatermarkResponse",
    "ReconciliationDetail",
    "WatermarkReader",
    "ClickHouseQueryClient",
    "AutoCorrectingQueryMerger",
    "app",
]
