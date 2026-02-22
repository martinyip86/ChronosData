import logging
import sys
import os
from logging.handlers import RotatingFileHandler

def setup_logger(name="swiss-quant",log_file=None):
    # 1. 创建 Logger 对象
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)# 默认设置为 INFO 级别

    # 2. 定义日志格式 (时间 | 级别 | 文件名:行号 | 消息)
    formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(name)s | %(filename)s:%(lineno)d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 如果已经有 Handler 了（比如 Airflow 已经初始化过），直接返回，避免重复打印
    if logger.handlers:
        has_file_handler = any(isinstance(h,logging.FileHandler) for h in logger.handlers)
        if log_file and not has_file_handler:
            _add_file_handler(logger, log_file, formatter)
        return logger

    # 3. 配置控制台输出 (Stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    if log_file:
        _add_file_handler(logger, log_file, formatter)

    return logger

def _add_file_handler(logger, log_file, formatter):
    try:
        os.makedirs(os.path.dirname(log_file),exist_ok=True)

        file_handler = RotatingFileHandler(log_file,mode='a',maxBytes=10*1024*1024,backupCount=5,encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        print(f"Failed to set up file logging: {e}")
    