import logging
import sys
import os

def setup_logger(name="swiss-quant"):
    # 1. 创建 Logger 对象
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)# 默认设置为 INFO 级别

    # 如果已经有 Handler 了（比如 Airflow 已经初始化过），直接返回，避免重复打印
    if logger.handlers:
        return logger
    
    # 2. 定义日志格式 (时间 | 级别 | 文件名:行号 | 消息)
    formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(name)s | %(filename)s:%(lineno)d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 3. 配置控制台输出 (Stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger

logger = setup_logger()