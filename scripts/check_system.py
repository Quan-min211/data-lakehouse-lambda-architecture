"""
check_system.py
===============
Kiem tra toan bo he thong local truoc khi chay Streaming.
Chay bang: python scripts/check_system.py

Kiem tra:
  1. Python imports (fastapi, clickhouse_connect, kafka, redis, pydantic)
  2. Ket noi ClickHouse (localhost:8123)
  3. Ket noi Kafka (localhost:9094)
  4. Ket noi Redis (localhost:6379)
  5. Ket noi MinIO (localhost:9000)
  6. Ket noi Iceberg REST (localhost:8181)
  7. Ket noi FastAPI Serving (localhost:8000)
  8. Import chain chinh (src.batch_layer, src.speed_layer, src.serving_layer)
  9. Validate docker-compose.yml syntax

Hien thi bang tong hop PASS/FAIL mau sac.
"""

from __future__ import annotations

import importlib
import os
import socket
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
RESET  = "\033[0m"
BOLD   = "\033[1m"


def _pass(msg: str) -> str:
    return f"{GREEN}[PASS]{RESET} {msg}"


def _fail(msg: str, detail: str = "") -> str:
    d = f" — {detail}" if detail else ""
    return f"{RED}[FAIL]{RESET} {msg}{d}"


def _warn(msg: str, detail: str = "") -> str:
    d = f" — {detail}" if detail else ""
    return f"{YELLOW}[WARN]{RESET} {msg}{d}"


def _skip(msg: str) -> str:
    return f"{YELLOW}[SKIP]{RESET} {msg}"


# ── 1. Python Imports ─────────────────────────────────────────────────────────
def check_imports() -> list[str]:
    results = []
    pkgs = {
        "fastapi":              "FastAPI (Serving Layer)",
        "uvicorn":              "Uvicorn (ASGI server)",
        "pydantic":             "Pydantic (schema validation)",
        "clickhouse_connect":   "ClickHouse Connect",
        "kafka":                "kafka-python",
        "redis":                "Redis client",
        "yaml":                 "PyYAML (config)",
        "dotenv":               "python-dotenv",
        "httpx":                "httpx (TestClient dep)",
    }
    for pkg, label in pkgs.items():
        try:
            importlib.import_module(pkg)
            results.append(_pass(label))
        except ImportError as e:
            results.append(_fail(label, str(e)))
    return results


# ── 2. Internal Imports (src.*) ───────────────────────────────────────────────
def check_internal_imports() -> list[str]:
    results = []
    modules = {
        "src.ingestion.models":          "TradeEvent model",
        "src.ingestion.fault_injector":  "FaultInjector",
        "src.speed_layer.metrics_calculator": "MetricsCalculator",
        "src.speed_layer.spike_detector":     "SpikeDetector",
        "src.speed_layer.trade_schema":       "TradeSchema",
        "src.serving_layer.schemas":          "Serving schemas",
        "src.serving_layer.watermark_reader": "WatermarkReader",
        "src.serving_layer.clickhouse_client":"ClickHouseQueryClient",
        "src.serving_layer.query_merger":     "AutoCorrectingQueryMerger",
        "src.batch_layer.clickhouse_sync":    "ClickHouseBatchSync",
        "src.batch_layer.compaction":         "CompactionJob",
        "src.utils.logger":                   "Logger utility",
        "src.utils.config":                   "Config utility",
        "src.data_quality.dq_checks":         "DQ checks",
    }
    for mod, label in modules.items():
        try:
            importlib.import_module(mod)
            results.append(_pass(label))
        except Exception as e:
            results.append(_fail(label, str(e)))
    return results


# ── 3. HTTP Endpoint Checks ───────────────────────────────────────────────────
def _http_check(label: str, url: str, timeout: int = 3) -> str:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            if r.status < 400:
                return _pass(f"{label} ({url}) — HTTP {r.status}")
            return _fail(f"{label} ({url})", f"HTTP {r.status}")
    except Exception as e:
        return _warn(f"{label} ({url}) — khong ket noi duoc (Docker dang chay?)", str(e)[:80])


def check_docker_services() -> list[str]:
    return [
        _http_check("ClickHouse",    "http://localhost:8123/ping"),
        _http_check("MinIO",         "http://localhost:9000/minio/health/live"),
        _http_check("Iceberg REST",  "http://localhost:8181/v1/config"),
        _http_check("FastAPI",       "http://localhost:8000/health"),
        _http_check("Streamlit",     "http://localhost:8501", timeout=3),
        _http_check("Spark Master",  "http://localhost:8080", timeout=3),
    ]


# ── 4. TCP Port Checks ────────────────────────────────────────────────────────
def _tcp_check(label: str, host: str, port: int, timeout: int = 2) -> str:
    try:
        s = socket.create_connection((host, port), timeout=timeout)
        s.close()
        return _pass(f"{label} ({host}:{port})")
    except Exception:
        return _warn(f"{label} ({host}:{port})", "port khong mo (Docker dang chay?)")


def check_tcp_ports() -> list[str]:
    return [
        _tcp_check("Kafka External",  "localhost", 9094),
        _tcp_check("Redis",           "localhost", 6379),
        _tcp_check("ClickHouse HTTP", "localhost", 8123),
        _tcp_check("Iceberg REST",    "localhost", 8181),
        _tcp_check("MinIO S3 API",    "localhost", 9000),
    ]


# ── 5. Docker Compose syntax check ───────────────────────────────────────────
def check_docker_compose() -> list[str]:
    compose_file = ROOT / "docker-compose.yml"
    if not compose_file.exists():
        return [_fail("docker-compose.yml", "file khong ton tai")]

    try:
        import yaml
        with open(compose_file, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        services = list(data.get("services", {}).keys())
        return [_pass(f"docker-compose.yml valid — {len(services)} services: {', '.join(services)}")]
    except Exception as e:
        return [_fail("docker-compose.yml", str(e))]


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    start = time.time()
    print(f"\n{BOLD}{'='*65}{RESET}")
    print(f"{BOLD}  LAMBDA LAKEHOUSE — SYSTEM HEALTH CHECK{RESET}")
    print(f"{BOLD}  {ROOT}{RESET}")
    print(f"{BOLD}{'='*65}{RESET}")

    sections = [
        ("Python Package Imports",  check_imports),
        ("Internal Module Imports (src.*)", check_internal_imports),
        ("Docker Compose Config",   check_docker_compose),
        ("TCP Port Connectivity",   check_tcp_ports),
        ("HTTP Service Endpoints",  check_docker_services),
    ]

    total_pass = 0
    total_fail = 0
    total_warn = 0

    for title, fn in sections:
        print(f"\n{CYAN}{BOLD}[{title}]{RESET}")
        items = fn()
        for item in items:
            print(f"  {item}")
            if "[PASS]" in item: total_pass += 1
            elif "[FAIL]" in item: total_fail += 1
            elif "[WARN]" in item: total_warn += 1

    elapsed = time.time() - start
    overall_color = GREEN if total_fail == 0 else RED

    print(f"\n{BOLD}{'='*65}{RESET}")
    print(
        f"  {overall_color}{BOLD}PASS: {total_pass}  FAIL: {total_fail}  WARN: {total_warn}"
        f"  ({elapsed:.1f}s){RESET}"
    )
    if total_warn > 0:
        print(f"  {YELLOW}WARN = Docker services chua chay — dung: docker compose up -d{RESET}")
    if total_fail > 0:
        print(f"  {RED}FAIL = Can sua truoc khi chay streaming!{RESET}")
    print(f"{BOLD}{'='*65}{RESET}\n")

    if total_fail > 0:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
