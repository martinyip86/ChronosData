# 使用轻量级底座
FROM python:3.10-slim

# 设置工作目录
WORKDIR /app

# 安装必要的系统依赖（Polars 运行有时需要一些 C 库）
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt .
RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

# 复制源代码
COPY . .

# 设置环境变量，确保 Python 日志实时输出，不被缓存
ENV PYTHONUNBUFFERED=1

# 启动程序
CMD ["python","main.py"]