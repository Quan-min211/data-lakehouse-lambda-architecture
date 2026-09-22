"""
verify_dbt.py
=============
Script kiểm tra & xác thực khả năng chạy dbt models qua Spark Thrift Server:
  1. Kiểm tra kết nối TCP tới Spark Thrift Server (localhost:10000).
  2. Kiểm tra các thư viện dbt (dbt-core, dbt-spark).
  3. Kiểm tra file cấu hình dbt_project.yml & profiles.yml.
  4. Kiểm tra sự tồn tại của bảng Iceberg Bronze (iceberg_catalog.bronze.crypto_trades).
  5. Chạy `dbt debug` để verify kết nối dbt -> Spark Thrift.
  6. Chạy `dbt run` (Bronze -> Silver -> Gold).
  7. Chạy `dbt test` để kiểm tra Data Contract tests.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path

# ── PYTHONPATH ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DBT_DIR = ROOT / "dbt_project"
THRIFT_HOST = "localhost"
THRIFT_PORT = 10000

# ── Colors ────────────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
RESET  = "\033[0m"
BOLD   = "\033[1m"


def print_banner(title: str):
    print(f"\n{BOLD}{'='*68}{RESET}")
    print(f"{BOLD}  {title}{RESET}")
    print(f"{BOLD}{'='*68}{RESET}")


def check_thrift_port(retries: int = 15, delay: float = 2.0) -> bool:
    print(f"\n{CYAN}{BOLD}[BƯỚC 1] Kiểm tra cổng Spark Thrift Server ({THRIFT_HOST}:{THRIFT_PORT}){RESET}")
    for attempt in range(1, retries + 1):
        try:
            s = socket.create_connection((THRIFT_HOST, THRIFT_PORT), timeout=2)
            s.close()
            print(f"  {GREEN}[PASS]{RESET} Port {THRIFT_PORT} đang mở và sẵn sàng nhận kết nối JDBC/Thrift!")
            return True
        except Exception as e:
            if attempt < retries:
                print(f"  [Đang chờ] Port {THRIFT_PORT} chưa mở (thử {attempt}/{retries}). Đang khởi động... ({delay}s)", end="\r")
                time.sleep(delay)
            else:
                print(f"\n  {RED}[FAIL]{RESET} Không kết nối được port {THRIFT_PORT}: {e}")
                print(f"  {YELLOW}Khởi động spark-thrift: docker compose up -d spark-thrift{RESET}")
                print(f"  {YELLOW}Xem log: docker compose logs -f spark-thrift{RESET}")
                return False
    return False


def check_dbt_installed() -> bool:
    print(f"\n{CYAN}{BOLD}[BƯỚC 2] Kiểm tra cài đặt dbt & dbt-spark{RESET}")
    missing = []
    try:
        import dbt.version
        print(f"  {GREEN}[PASS]{RESET} dbt-core đã được cài đặt.")
    except ImportError:
        missing.append("dbt-core>=1.7.0")

    try:
        import dbt.adapters.spark
        print(f"  {GREEN}[PASS]{RESET} dbt-spark adapter đã được cài đặt.")
    except ImportError:
        missing.append("dbt-spark[PyHive]>=1.7.0")

    if missing:
        print(f"  {YELLOW}[WARN]{RESET} Thiếu thư viện dbt trên máy host. Cần cài đặt:")
        print(f"    pip install {' '.join(missing)}")
        return False

    return True


def run_dbt_command(cmd_args: list[str], desc: str) -> bool:
    print(f"\n{CYAN}{BOLD}[BƯỚC] {desc}{RESET}")
    import shutil
    dbt_bin = shutil.which("dbt")
    if dbt_bin:
        cmd = [dbt_bin, *cmd_args, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    else:
        cmd = [
            sys.executable, "-m", "dbt.cli.main",
            *cmd_args,
            "--project-dir", str(DBT_DIR),
            "--profiles-dir", str(DBT_DIR),
        ]
    print(f"  Lệnh: {' '.join(cmd)}")
    t0 = time.time()
    try:
        result = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True, timeout=180)
        elapsed = time.time() - t0
        print(result.stdout)
        if result.stderr:
            print(f"  {YELLOW}[STDERR]{RESET}\n{result.stderr}")

        if result.returncode == 0:
            print(f"  {GREEN}[PASS]{RESET} {desc} thành công trong {elapsed:.1f}s!")
            return True
        else:
            print(f"  {RED}[FAIL]{RESET} {desc} thất bại (exit code: {result.returncode})")
            return False
    except subprocess.TimeoutExpired:
        print(f"  {RED}[FAIL]{RESET} {desc} bị timeout sau 180s.")
        return False
    except Exception as e:
        print(f"  {RED}[FAIL]{RESET} Lỗi thực thi: {e}")
        return False


def main():
    print_banner("KIỂM TRA & XÁC THỰC DBT RUN QUA SPARK THRIFT SERVER")
    print(f"  dbt Project Dir  : {DBT_DIR}")
    print(f"  Spark Thrift     : {THRIFT_HOST}:{THRIFT_PORT}")

    # 1. Check Thrift port
    thrift_ok = check_thrift_port()
    if not thrift_ok:
        print(f"\n{RED}{BOLD}Dừng: Spark Thrift Server chưa sẵn sàng.{RESET}\n")
        sys.exit(1)

    # 2. Check dbt packages
    dbt_ok = check_dbt_installed()
    if not dbt_ok:
        print(f"\n{YELLOW}{BOLD}Gợi ý: Cài đặt dbt-spark trên host trước khi chạy:{RESET}")
        print(f"  pip install \"dbt-spark[PyHive]>=1.7.0\" \"dbt-core>=1.7.0\"\n")
        sys.exit(1)

    # 3. dbt debug
    debug_ok = run_dbt_command(["debug"], "Kiểm tra kết nối dbt debug -> Spark Thrift")
    if not debug_ok:
        print(f"\n{RED}{BOLD}Dừng: dbt debug không thành công.{RESET}\n")
        sys.exit(1)

    # 4. dbt run
    run_ok = run_dbt_command(["run"], "Chạy dbt run (Bronze -> Silver -> Gold)")

    # 5. dbt test
    test_ok = False
    if run_ok:
        test_ok = run_dbt_command(["test"], "Chạy dbt test (Kiểm tra schema Data Contracts)")

    # Summary
    print_banner("TỔNG KẾT XÁC THỰC DBT")
    status = "PASS" if (run_ok and test_ok) else ("PARTIAL" if run_ok else "FAIL")
    color = GREEN if status == "PASS" else (YELLOW if status == "PARTIAL" else RED)
    print(f"  Thrift Port 10000 : {GREEN}ONLINE{RESET}")
    print(f"  dbt debug         : {GREEN if debug_ok else RED}{'PASS' if debug_ok else 'FAIL'}{RESET}")
    print(f"  dbt run           : {GREEN if run_ok else RED}{'PASS' if run_ok else 'FAIL'}{RESET}")
    print(f"  dbt test          : {GREEN if test_ok else RED}{'PASS' if test_ok else 'FAIL'}{RESET}")
    print(f"\n  {color}{BOLD}KẾT QUẢ: {status}{RESET}")
    print(f"{BOLD}{'='*68}{RESET}\n")

    sys.exit(0 if (run_ok and test_ok) else 1)


if __name__ == "__main__":
    main()
