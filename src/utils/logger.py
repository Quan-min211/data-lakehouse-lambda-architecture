"""
logger.py
=========
Module cấu hình logging tập trung cho toàn bộ hệ thống Data Lakehouse.
Đảm bảo an toàn mã hóa UTF-8 trên cả Windows và Linux.
"""

import logging
import os
import sys


def setup_logger(name: str = "lakehouse", level: str = None) -> logging.Logger:
    """Khởi tạo và cấu hình logger theo tiêu chuẩn hệ thống.

    Args:
        name (str): Tên định danh cho logger module.
        level (str, optional): Cấp độ log (DEBUG, INFO, WARNING, ERROR). Mặc định lấy từ LOG_LEVEL env.

    Returns:
        logging.Logger: Đối tượng logger đã được cấu hình.
    """
    if level is None:
        level = os.getenv("LOG_LEVEL", "INFO").upper()

    log_level = getattr(logging, level, logging.INFO)
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(log_level)
        
        # Bảo đảm stdout hỗ trợ UTF-8 trên Windows
        if hasattr(sys.stdout, "reconfigure"):
            try:
                sys.stdout.reconfigure(encoding="utf-8")
            except Exception:
                pass

        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(log_level)
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
